package bda

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

/** M4: measured Spark/HDFS tuning experiments (CO2/CO3). Every number printed here is a real wall-clock measurement from the cluster
  * (3 DataNodes, 1 NodeManager, 2 executors x 1.25 GB). Each experiment is run once (no repeats) - treat differences < ~15% as noise.
  *  E1 JSON vs Parquet scan        E2 shuffle.partitions sweep       E3 cache vs no cache (3 queries on one DataFrame)
  *  E4 small files (500 vs 8 files) E5 broadcast vs sort-merge join  E6 partitioned (by day) pruning vs full scan
  */
object TuningBench {
  case class Row3(experiment: String, variant: String, seconds: Double, note: String)

  def time[T](f: => T): (T, Double) = { val t = System.nanoTime(); val r = f; (r, (System.nanoTime() - t) / 1e9) }

  def main(argv: Array[String]): Unit = {
    val spark = Common.spark("M4-tuning-bench", "spark.sql.adaptive.enabled" -> "false")   // AQE off so each knob is measured on its own
    import spark.implicits._
    val out = scala.collection.mutable.ArrayBuffer.empty[Row3]
    def rec(e: String, v: String, s: Double, n: String = ""): Unit = { out += Row3(e, v, s, n); println(f"[bench] $e%-4s $v%-34s $s%8.2f s $n") }

    // ---- E1: JSON vs Parquet on identical rows (5 raw shards) ----
    val shards = (0 to 4).map(i => f"${Common.RawJson}/2019-Nov-part-$i%04d.jsonl")
    val json = spark.read.schema(Common.RawSchema).json(shards: _*)
    val (nJson, tJson) = time(json.groupBy("event_type").count().collect().map(_.getLong(1)).sum)
    rec("E1", "JSON scan+groupBy", tJson, s"rows=$nJson")
    val tmp = Common.Hdfs + "/bda/tmp/bench_parquet"
    val (_, tW) = time(json.write.mode(SaveMode.Overwrite).parquet(tmp))
    rec("E1", "write same rows as Parquet", tW)
    val (nPq, tPq) = time(spark.read.parquet(tmp).groupBy("event_type").count().collect().map(_.getLong(1)).sum)
    rec("E1", "Parquet scan+groupBy", tPq, s"rows=$nPq")

    // ---- common working set: one week of clean events ----
    val week = spark.read.parquet(Common.CleanEvents).filter($"event_date" between ("2019-11-01", "2019-11-07"))
    def query(df: DataFrame): Long =
      df.groupBy("event_date", "category_l1", "event_type").agg(countDistinct("user_id").as("u"), count("*").as("n")).collect().length.toLong   // collect() so the aggregates are really computed (count() lets the optimizer drop them)

    // ---- E2: shuffle partitions ----
    for (p <- Seq(4, 24, 96, 400)) {
      spark.conf.set("spark.sql.shuffle.partitions", p.toString)
      val (_, t) = time(query(week))
      rec("E2", s"shuffle.partitions=$p", t)
    }
    spark.conf.set("spark.sql.shuffle.partitions", "24")

    // ---- E3: caching ----
    def three(df: DataFrame): Unit = {
      df.groupBy("event_type").count().collect()
      df.groupBy("category_l1").agg(avg("price")).collect()
      df.agg(countDistinct("user_session")).collect()
    }
    val (_, tNo) = time(three(week.select("event_type", "category_l1", "price", "user_session")))
    rec("E3", "3 queries, no cache", tNo)
    val cached = week.select("event_type", "category_l1", "price", "user_session").cache()
    val (_, tYes) = time(three(cached))
    rec("E3", "3 queries, cache (incl. fill)", tYes)
    val (_, tYes2) = time(three(cached))
    rec("E3", "3 queries again (cache warm)", tYes2)
    cached.unpersist()

    // ---- E4: small files ----
    val sample = week.filter($"event_date" === "2019-11-03")
    val many = Common.Hdfs + "/bda/tmp/bench_many"; val few = Common.Hdfs + "/bda/tmp/bench_few"
    val (_, tm) = time(sample.repartition(500).write.mode(SaveMode.Overwrite).parquet(many))
    val (_, tf) = time(sample.coalesce(8).write.mode(SaveMode.Overwrite).parquet(few))
    rec("E4", "write as 500 files", tm); rec("E4", "write as 8 files", tf)
    val (_, rm) = time(spark.read.parquet(many).groupBy("event_type").count().collect())
    val (_, rf) = time(spark.read.parquet(few).groupBy("event_type").count().collect())
    rec("E4", "read 500 small files", rm); rec("E4", "read 8 files", rf)

    // ---- E5: join strategy with the 207k-row products table (from PostgreSQL via JDBC copy) ----
    val products = spark.read.parquet(Common.RelTables + "/products").select("product_id", "last_price")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val (_, tSmj) = time(week.join(products, "product_id").agg(avg($"price" / $"last_price")).collect())
    rec("E5", "sort-merge join (broadcast off)", tSmj)
    val (_, tBj) = time(week.join(broadcast(products), "product_id").agg(avg($"price" / $"last_price")).collect())
    rec("E5", "broadcast join", tBj)

    // ---- E6: partition pruning ----
    val all = spark.read.parquet(Common.CleanEvents)
    val (_, tFull) = time(all.filter($"event_type" === "purchase").count())
    val (_, tPrune) = time(all.filter($"event_date" === "2019-11-03" && $"event_type" === "purchase").count())
    rec("E6", "purchases, all 61 days", tFull); rec("E6", "purchases, 1 day (pruned)", tPrune)

    out.toSeq.toDF().coalesce(1).write.mode(SaveMode.Overwrite).json(Common.Metrics + "/tuning_bench")
    spark.stop()
  }
}
