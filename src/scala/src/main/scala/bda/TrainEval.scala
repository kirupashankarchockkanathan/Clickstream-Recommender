package bda

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/** M4: temporal-split next-item evaluation of four recommenders on the cleaned clickstream.
  *   pop        : most popular items in training data
  *   als_batch  : MLlib implicit ALS, user vector learned at training time (stale: ignores the current session)
  *   als_foldin : same ALS item factors; session vector re-solved from the clicks seen so far (this project)
  *   cooc       : item-to-item session co-occurrence (nearest-neighbour style baseline, cf. Ludewig et al.)
  * Protocol: train on events up to trainEnd; test sessions start on/after testStart (strict temporal split, no leakage).
  * For a test session with unique items i1,i2,..., a case is (prefix=i1..ik, target=i(k+1)), k=1..maxSteps.
  * Targets/prefix items outside the model catalogue (too rare) are removed and this is stated in the report.
  * Args: trainEnd testStart rank reg alpha iters userPct itemMinUsers topN testSessionPct maxSteps
  */
object TrainEval {
  final case class Case(case_id: Long, user_id: Long, k: Int, prefix: Seq[Long], prefix_w: Seq[Double], target: Long)

  def main(argv: Array[String]): Unit = {
    val a = Common.parseArgs(argv)
    val trainEnd = a.getOrElse("trainEnd", "2019-11-20"); val testStart = a.getOrElse("testStart", "2019-11-21")
    val rank = a.getOrElse("rank", "32").toInt; val reg = a.getOrElse("reg", "0.05").toDouble
    val alpha = a.getOrElse("alpha", "20").toDouble; val iters = a.getOrElse("iters", "8").toInt
    val userPct = a.getOrElse("userPct", "25").toInt; val minItemUsers = a.getOrElse("itemMinUsers", "30").toLong
    val topN = a.getOrElse("topN", "20").toInt; val testPct = a.getOrElse("testSessionPct", "10").toInt
    val maxSteps = a.getOrElse("maxSteps", "3").toInt
    val testEnd = a.getOrElse("testEnd", "2019-11-30"); val tag = a.getOrElse("tag", "final")
    val spark = Common.spark("M4-train-eval", "spark.sql.shuffle.partitions" -> a.getOrElse("partitions", "48"))
    import spark.implicits._
    spark.sparkContext.setCheckpointDir(Common.Hdfs + "/bda/tmp/ckpt")

    val w = when($"event_type" === "view", 1.0).when($"event_type" === "cart", 3.0).otherwise(5.0)
    val ev = spark.read.parquet(Common.CleanEvents)
      .filter($"event_type".isin("view", "cart", "purchase") && $"product_id" < Int.MaxValue && $"user_id" < Int.MaxValue)
      .filter(pmod(hash($"user_id"), lit(100)) < userPct)               // deterministic user sample (disclosed in report)
      .select($"user_id", $"product_id", $"user_session", $"event_type", $"ts", $"event_date", w.as("w"))

    // ---------- training data ----------
    val train = ev.filter($"event_date" <= trainEnd)
    val ratings0 = train.groupBy($"user_id", $"product_id").agg(sum($"w").as("rating"))
    val catalog = ratings0.groupBy("product_id").agg(countDistinct("user_id").as("n")).filter($"n" >= minItemUsers).select("product_id", "n")
    val ratings = ratings0.join(broadcast(catalog.select("product_id")), "product_id")
      .select($"user_id".cast("int").as("u"), $"product_id".cast("int").as("i"), $"rating".cast("float").as("r")).cache()
    val nRatings = ratings.count(); val nItems = catalog.count()
    println(s"[data] train ratings=$nRatings catalogue items=$nItems")

    val t0 = System.nanoTime()
    val model = new ALS().setImplicitPrefs(true).setRank(rank).setRegParam(reg).setAlpha(alpha).setMaxIter(iters)
      .setUserCol("u").setItemCol("i").setRatingCol("r").setNumBlocks(8).setCheckpointInterval(4).setSeed(42).fit(ratings)
    val alsSecs = (System.nanoTime() - t0) / 1e9
    println(f"[als] trained in $alsSecs%.1f s")

    val itemRows = model.itemFactors.collect()
    val factors = FoldIn.ItemFactors(itemRows.map(_.getInt(0).toLong),
      itemRows.flatMap(_.getSeq[Float](1)), rank)
    val bFactors = spark.sparkContext.broadcast(factors)

    // ---------- test cases ----------
    val catIds = factors.index
    val bCat = spark.sparkContext.broadcast(catIds.keySet)
    val testEv = ev.filter($"event_date" >= testStart && $"event_date" <= testEnd)
      .filter(pmod(hash($"user_session"), lit(100)) < testPct)
    val sessions = testEv.groupBy("user_session", "user_id")
      .agg(sort_array(collect_list(struct($"ts", $"product_id", $"w"))).as("evs"))
      .select($"user_session", $"user_id", $"evs.product_id".as("pids"), $"evs.w".as("ws"))
    val cases = sessions.as[(String, Long, Seq[Long], Seq[Double])].flatMap { case (sess, uid, pids, ws) =>
      val seen = scala.collection.mutable.LinkedHashMap.empty[Long, Double]       // first occurrence order, first-event weight
      pids.zip(ws).foreach { case (p, wt) => if (bCat.value.contains(p) && !seen.contains(p)) seen(p) = wt }
      val items = seen.toIndexedSeq
      (1 to math.min(items.length - 1, maxSteps)).map { k =>
        val pre = items.take(k).takeRight(10)
        Case(((scala.util.hashing.MurmurHash3.stringHash(sess, 1).toLong << 32) | (scala.util.hashing.MurmurHash3.stringHash(sess, 2) & 0xffffffffL)) ^ k, uid, k, pre.map(_._1), pre.map(_._2), items(k)._1)
      }
    }.cache()
    val nCases = cases.count()
    println(s"[eval] test cases=$nCases")

    // ---------- recommenders: each returns DataFrame(case_id, recs) ----------
    val pop = ratings0.join(broadcast(catalog.select("product_id")), "product_id").groupBy("product_id")
      .agg(countDistinct("user_id").as("n")).orderBy($"n".desc).limit(topN + 20).select("product_id").as[Long].collect().toSeq
    val popRecs = cases.map(c => (c.case_id, pop.filterNot(c.prefix.contains).take(topN))).toDF("case_id", "recs")

    val caseUsers = cases.select($"user_id".cast("int").as("u")).distinct()
    val uvecs = model.userFactors.join(broadcast(caseUsers), $"id" === $"u").select($"id", $"features").collect()
      .map(r => r.getInt(0).toLong -> r.getSeq[Float](1).map(_.toDouble).toArray).toMap
    val bU = spark.sparkContext.broadcast(uvecs)
    val batchRecs = cases.map { c =>
      val f = bFactors.value
      val recs = bU.value.get(c.user_id) match {
        case Some(x) => FoldIn.topN(f, x, topN, c.prefix.toSet).toSeq
        case None => pop.filterNot(c.prefix.contains).take(topN)              // cold user -> popularity fallback
      }
      (c.case_id, recs)
    }.toDF("case_id", "recs")

    val foldRecs = cases.map { c =>
      val f = bFactors.value
      val recs = FoldIn.sessionVector(f, c.prefix.zip(c.prefix_w), reg, alpha) match {
        case Some(x) => FoldIn.topN(f, x, topN, c.prefix.toSet).toSeq
        case None => pop.filterNot(c.prefix.contains).take(topN)
      }
      (c.case_id, recs)
    }.toDF("case_id", "recs")

    val coocRecs = coOccurrence(spark, train, catalog.select("product_id"), cases, topN, pop)

    // hybrid: reciprocal-rank fusion (RRF, k=60) of the fold-in ALS list and the co-occurrence list
    val rrf = udf((x: Seq[Long], y: Seq[Long]) => {
      val sc = scala.collection.mutable.HashMap.empty[Long, Double].withDefaultValue(0.0)
      x.zipWithIndex.foreach { case (i, r) => sc(i) += 1.0 / (60 + r + 1) }
      y.zipWithIndex.foreach { case (i, r) => sc(i) += 1.0 / (60 + r + 1) }
      sc.toSeq.sortBy(-_._2).take(topN).map(_._1)
    })
    val hybRecs = foldRecs.withColumnRenamed("recs", "r1").join(coocRecs.withColumnRenamed("recs", "r2"), "case_id")
      .select($"case_id", rrf($"r1", $"r2").as("recs"))

    // ---------- metrics ----------
    val methods = Seq("pop" -> popRecs, "als_batch" -> batchRecs, "als_foldin" -> foldRecs, "cooc" -> coocRecs, "hybrid_rrf" -> hybRecs)
    val results = methods.map { case (name, recs) =>
      val t = System.nanoTime()
      val m = metrics(cases.select("case_id", "k", "target"), recs, topN).withColumn("method", lit(name))
      m.cache().count()
      println(f"[eval] $name%-11s scored in ${(System.nanoTime() - t) / 1e9}%.1f s")
      m
    }.reduce(_ unionByName _)
    results.orderBy("k", "method").show(50, truncate = false)
    results.coalesce(1).write.mode(SaveMode.Overwrite).json(Common.Metrics + s"/eval_by_k_$tag")

    // fold-in latency micro-benchmark (driver-side, single thread)
    val sample = cases.limit(5000).collect()
    val tt = System.nanoTime()
    var sink = 0L
    sample.foreach { c => FoldIn.sessionVector(factors, c.prefix.zip(c.prefix_w), reg, alpha).foreach(x => sink += FoldIn.topN(factors, x, topN, c.prefix.toSet).length) }
    val perCallMs = (System.nanoTime() - tt) / 1e6 / sample.length
    println(f"[latency] fold-in + top-$topN over ${factors.ids.length} items: $perCallMs%.3f ms per request (sink=$sink)")
    Seq((nRatings, nItems, nCases, alsSecs, perCallMs, rank, reg, alpha, iters, userPct, testPct))
      .toDF("train_ratings", "catalog_items", "test_cases", "als_train_seconds", "foldin_topn_ms", "rank", "reg", "alpha", "iters", "user_pct", "test_session_pct")
      .coalesce(1).write.mode(SaveMode.Overwrite).json(Common.Metrics + s"/eval_run_info_$tag")
    model.write.overwrite().save(Common.Models + (if (tag == "final") "/als_implicit" else s"/als_$tag"))
    spark.stop()
  }

  /** Item-to-item co-occurrence recommender built from training sessions (window 3, 1/distance weights, cosine-like norm). */
  def coOccurrence(spark: org.apache.spark.sql.SparkSession, train: DataFrame, catalog: DataFrame, cases: org.apache.spark.sql.Dataset[Case],
                   topN: Int, pop: Seq[Long]): DataFrame = {
    import spark.implicits._
    val seqs = train.join(broadcast(catalog), "product_id").groupBy("user_session")
      .agg(sort_array(collect_list(struct($"ts", $"product_id"))).as("evs")).select($"evs.product_id".as("items"))
    val pairs = seqs.as[Seq[Long]].flatMap { raw =>
      val items = raw.foldLeft(Vector.empty[Long])((acc, p) => if (acc.contains(p)) acc else acc :+ p).take(40)
      for (i <- items.indices; d <- 1 to 3 if i + d < items.length) yield (items(i), items(i + d), 1.0 / d)
    }.toDF("a", "b", "w")
    val co = pairs.groupBy("a", "b").agg(sum("w").as("c"))
    val freq = train.join(broadcast(catalog), "product_id").groupBy("product_id").agg(countDistinct("user_session").as("f"))
    val scored = co.join(freq.withColumnRenamed("product_id", "a").withColumnRenamed("f", "fa"), "a")
      .join(freq.withColumnRenamed("product_id", "b").withColumnRenamed("f", "fb"), "b")
      .withColumn("s", $"c" / sqrt($"fa".cast("double") * $"fb"))
    val nb = scored.withColumn("rk", row_number().over(Window.partitionBy("a").orderBy($"s".desc))).filter($"rk" <= 50).select("a", "b", "s").cache()
    val probes = cases.toDF().select($"case_id", posexplode($"prefix").as(Seq("pos", "a")), $"prefix")
      .withColumn("dist", size($"prefix") - $"pos" - 1).filter($"dist" < 3)
      .withColumn("rw", pow(lit(0.5), $"dist".cast("double")))
    val cand = probes.join(nb, "a").groupBy("case_id", "b").agg(sum($"rw" * $"s").as("score"), first("prefix").as("prefix"))
      .filter(!array_contains($"prefix", $"b"))
    val top = cand.withColumn("rk", row_number().over(Window.partitionBy("case_id").orderBy($"score".desc))).filter($"rk" <= topN)
      .groupBy("case_id").agg(sort_array(collect_list(struct($"rk", $"b"))).as("l"))
      .select($"case_id", $"l.b".as("recs"))
    val popArr = array(pop.take(topN).map(lit): _*)
    cases.toDF().select("case_id").join(top, Seq("case_id"), "left")
      .withColumn("recs", coalesce($"recs", popArr))                       // no neighbours -> popularity fallback
  }

  /** HR@N, MRR@N, NDCG@N grouped by prefix length k. */
  def metrics(cases: DataFrame, recs: DataFrame, topN: Int): DataFrame = {
    val spark = cases.sparkSession; import spark.implicits._
    val j = cases.join(recs, "case_id").withColumn("pos", array_position($"recs".cast("array<long>"), $"target"))
    j.withColumn("hit", when($"pos" > 0 && $"pos" <= topN, 1.0).otherwise(0.0))
      .withColumn("mrr", when($"pos" > 0 && $"pos" <= topN, lit(1.0) / $"pos").otherwise(0.0))
      .withColumn("ndcg", when($"pos" > 0 && $"pos" <= topN, lit(1.0) / (log($"pos" + 1.0) / log(lit(2.0)))).otherwise(0.0))
      .groupBy("k").agg(count("*").as("cases"), avg("hit").as(s"HR_$topN"), avg("mrr").as(s"MRR_$topN"), avg("ndcg").as(s"NDCG_$topN"))
  }
}
