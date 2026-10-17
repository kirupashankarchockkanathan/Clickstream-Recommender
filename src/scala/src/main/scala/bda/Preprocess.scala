package bda

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

/** M3: raw JSON (HDFS) -> cleaned, partitioned Parquet (HDFS) + veracity report.
  *
  * Cleaning rules (each counted, nothing silently dropped):
  *  1. rows missing user_id / product_id / user_session / parsable event_time are dropped
  *  2. event_type must be one of view|cart|remove_from_cart|purchase
  *  3. price <= 0 or null on a product event is invalid -> row dropped
  *  4. exact duplicates (time,type,product,user,session) are removed
  *  5. brand / category_code nulls are kept and labelled "unknown" (they are informative, not errors)
  *  6. bot-like sessions (> maxSessionLen events) are flagged and removed
  * Usage args: in=<path> out=<path> partitions=<n> maxSessionLen=<n>
  */
object Preprocess {
  def main(argv: Array[String]): Unit = {
    val a = Common.parseArgs(argv)
    val in = a.getOrElse("in", Common.RawJson)
    val out = a.getOrElse("out", Common.CleanEvents)
    val maxLen = a.getOrElse("maxSessionLen", "1000").toLong
    val spark = Common.spark("M3-preprocess", "spark.sql.shuffle.partitions" -> a.getOrElse("partitions", "48"))
    import spark.implicits._

    // Pass 1: parse the 26 GB of JSON exactly once into typed Parquet (columnar, compressed). Every later count reads the
    // Parquet copy instead of re-parsing JSON (measured in the tuning report).
    val typedPath = out + "_typed"
    spark.read.schema(Common.RawSchema).json(in)
      .withColumn("ts", to_timestamp(regexp_replace($"event_time", " UTC$", ""), "yyyy-MM-dd HH:mm:ss"))
      .drop("event_time")
      .write.mode(SaveMode.Overwrite).parquet(typedPath)
    val typed = spark.read.parquet(typedPath)
    val nRaw = typed.count()

    val c1 = typed.filter($"ts".isNotNull && $"user_id".isNotNull && $"product_id".isNotNull && $"user_session".isNotNull)
    val c2 = c1.filter($"event_type".isin(Common.EventTypes.toSeq: _*))
    val c3 = c2.filter($"price".isNotNull && $"price" > 0)
    val c4 = c3.dropDuplicates("ts", "event_type", "product_id", "user_id", "user_session")

    val sessLen = c4.groupBy("user_session").count().filter($"count" > maxLen).select("user_session")
    val c5 = c4.join(broadcast(sessLen), Seq("user_session"), "left_anti")

    val clean = c5
      .withColumn("brand", lower(coalesce($"brand", lit("unknown"))))
      .withColumn("category_code", coalesce($"category_code", lit("unknown")))
      .withColumn("category_l1", split($"category_code", "\\.").getItem(0))
      .withColumn("event_date", to_date($"ts"))
      .withColumn("event_hour", hour($"ts"))

    // One output file per day-partition avoids the HDFS small-files problem (see tuning report).
    clean.repartition($"event_date")
      .write.mode(SaveMode.Overwrite).partitionBy("event_date").parquet(out)

    val nClean = spark.read.parquet(out).count()
    val stages = Seq(
      ("raw_rows", nRaw), ("after_required_fields", c1.count()), ("after_valid_event_type", c2.count()),
      ("after_valid_price", c3.count()), ("after_dedup", c4.count()), ("final_clean_rows", nClean))
    val report: DataFrame = stages.toDF("stage", "rows")
      .withColumn("dropped_vs_raw", lit(nRaw) - $"rows")
    report.coalesce(1).write.mode(SaveMode.Overwrite).json(Common.Metrics + "/preprocess_veracity")
    report.show(false)
    spark.stop()
  }
}
