package bda

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/** Shared helpers: Spark session factory, HDFS paths, event schema, typed CLI args. */
object Common {
  val Hdfs = "hdfs://namenode:9000"
  val RawJson    = s"$Hdfs/bda/raw/events"          // JSON Lines as produced by convert_to_jsonl.py
  val CleanEvents = s"$Hdfs/bda/clean/events"       // Parquet partitioned by event_date
  val RelTables  = s"$Hdfs/bda/clean/rel"           // Parquet copies of PostgreSQL tables
  val Models     = s"$Hdfs/bda/models"
  val Metrics    = s"$Hdfs/bda/metrics"

  val EventTypes: Set[String] = Set("view", "cart", "remove_from_cart", "purchase")

  /** Explicit schema: avoids a full extra pass over ~27 GB of JSON just to infer types. */
  val RawSchema: StructType = StructType(Seq(
    StructField("event_time", StringType), StructField("event_type", StringType),
    StructField("product_id", LongType), StructField("category_id", LongType),
    StructField("category_code", StringType), StructField("brand", StringType),
    StructField("price", DoubleType), StructField("user_id", LongType),
    StructField("user_session", StringType)))

  def spark(app: String, conf: (String, String)*): SparkSession = {
    val b = SparkSession.builder().appName(app)
    conf.foreach { case (k, v) => b.config(k, v) }
    b.getOrCreate()
  }

  /** key=value command-line args, e.g. `partitions=48 cache=true`. */
  def parseArgs(args: Array[String]): Map[String, String] =
    args.collect { case a if a.contains("=") => val Array(k, v) = a.split("=", 2); k -> v }.toMap

  val JdbcUrl  = "jdbc:postgresql://postgres:5432/shop"
  val JdbcProps: java.util.Properties = {
    val p = new java.util.Properties(); p.setProperty("user", "bda"); p.setProperty("password", "bda_pass")
    p.setProperty("driver", "org.postgresql.Driver"); p
  }
}
