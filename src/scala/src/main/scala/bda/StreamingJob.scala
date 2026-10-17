package bda

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types._

/** M4: Kafka -> Spark Structured Streaming.
  *  Query 1 (engagement): 60 s windows sliding every 10 s: events / unique users per top-level category -> HDFS Parquet + Kafka topic `engagement`
  *  Query 2 (recommend):  per-session state (last clicks) -> ALS fold-in -> top-N -> relational filter (drop out-of-stock items from PostgreSQL inventory)
  *                        -> Kafka topic `recs`, with end-to-end latency (producer emit time -> result time).
  * Args: bootstrap=kafka:9092 topic=clicks trigger=5 topN=10 duration=0(run forever, seconds otherwise) shufflePartitions=6
  */
object StreamingJob {
  final case class Click(session: String, user_id: Long, product_id: Long, event_type: String, emit_ms: Long)
  final case class SessState(items: Vector[Long], weights: Vector[Double])
  final case class Rec(session: String, user_id: Long, n_clicks: Int, last_item: Long, recs: Seq[Long], emit_ms: Long, done_ms: Long)

  val schema: StructType = StructType(Seq(
    StructField("event_type", StringType), StructField("product_id", LongType), StructField("category_code", StringType),
    StructField("price", DoubleType), StructField("user_id", LongType), StructField("user_session", StringType), StructField("emit_ms", LongType)))

  def main(argv: Array[String]): Unit = {
    val a = Common.parseArgs(argv)
    val bootstrap = a.getOrElse("bootstrap", "kafka:9092"); val topic = a.getOrElse("topic", "clicks")
    val trig = a.getOrElse("trigger", "5").toInt; val topN = a.getOrElse("topN", "10").toInt
    val duration = a.getOrElse("duration", "0").toLong
    val spark = Common.spark("M4-streaming", "spark.sql.shuffle.partitions" -> a.getOrElse("shufflePartitions", "6"),
      "spark.sql.streaming.stateStore.providerClass" -> "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
    import spark.implicits._

    // static side: item factors from the batch-trained ALS model + out-of-stock set from the relational inventory table
    val model = org.apache.spark.ml.recommendation.ALSModel.load(Common.Models + "/als_implicit")
    val rows = model.itemFactors.collect()
    val factors = FoldIn.ItemFactors(rows.map(_.getInt(0).toLong), rows.flatMap(_.getSeq[Float](1)), model.rank)
    val bF = spark.sparkContext.broadcast(factors)
    val oos = spark.read.parquet(Common.RelTables + "/inventory").filter($"stock_qty" === 0).select("product_id").as[Long].collect().toSet
    val bOos = spark.sparkContext.broadcast(oos)
    val lambda = a.getOrElse("reg", "0.05").toDouble; val alpha = a.getOrElse("alpha", "20").toDouble
    println(s"[stream] item factors=${factors.ids.length} out-of-stock items=${oos.size}")

    val clicks = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap).option("subscribe", topic)
      .option("startingOffsets", "latest").option("maxOffsetsPerTrigger", a.getOrElse("maxOffsets", "200000")).load()
      .select(from_json($"value".cast("string"), schema).as("j")).select("j.*").withColumnRenamed("user_session", "session")
      .withColumn("event_time", ($"emit_ms" / 1000).cast("timestamp"))
      .withWatermark("event_time", "30 seconds")

    val q1 = clicks.withColumn("category_l1", split(coalesce($"category_code", lit("unknown")), "\\.").getItem(0))
      .groupBy(window($"event_time", "60 seconds", "10 seconds"), $"category_l1")
      .agg(count("*").as("events"), approx_count_distinct("user_id").as("users"),
        sum(when($"event_type" === "purchase", 1).otherwise(0)).as("purchases"))
      .select($"window.start".as("w_start"), $"window.end".as("w_end"), $"category_l1", $"events", $"users", $"purchases")
      .writeStream.outputMode(OutputMode.Append()).format("parquet")
      .option("path", Common.Hdfs + "/bda/stream/engagement").option("checkpointLocation", Common.Hdfs + "/bda/tmp/ckpt_engagement")
      .trigger(Trigger.ProcessingTime(s"$trig seconds")).start()

    val update: (String, Iterator[Click], GroupState[SessState]) => Iterator[Rec] = (sess, it, st) => {
      if (st.hasTimedOut) { st.remove(); Iterator.empty }
      else {
        val batch = it.toVector.sortBy(_.emit_ms)
        var s = st.getOption.getOrElse(SessState(Vector.empty, Vector.empty))
        batch.foreach { c =>
          FoldIn.kindOf(c.event_type).foreach { k =>
            if (!s.items.contains(c.product_id)) s = SessState((s.items :+ c.product_id).takeRight(10), (s.weights :+ k.weight).takeRight(10))
          }
        }
        st.update(s); st.setTimeoutDuration("30 minutes")
        batch.lastOption match {
          case None => Iterator.empty
          case Some(last) =>
            val f = bF.value
            val recs = FoldIn.sessionVector(f, s.items.zip(s.weights), lambda, alpha)
              .map(x => FoldIn.topN(f, x, topN + 20, s.items.toSet).filterNot(bOos.value.contains).take(topN).toSeq)
              .getOrElse(Seq.empty)
            Iterator(Rec(sess, last.user_id, s.items.length, last.product_id, recs, last.emit_ms, System.currentTimeMillis()))
        }
      }
    }
    val q2 = clicks.as[Click].groupByKey(_.session).flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout())(update)
      .select(col("session").as("key"), to_json(struct(col("*"))).as("value"))
      .writeStream.format("kafka").option("kafka.bootstrap.servers", bootstrap).option("topic", "recs")
      .option("checkpointLocation", Common.Hdfs + "/bda/tmp/ckpt_recs").trigger(Trigger.ProcessingTime(s"$trig seconds")).start()

    if (duration > 0) { Thread.sleep(duration * 1000); q1.stop(); q2.stop(); spark.stop() }
    else spark.streams.awaitAnyTermination()
  }
}
