package bda

import org.apache.spark.sql.SaveMode

/** M3: pull the PostgreSQL tables into HDFS as Parquet over JDBC (what Sqoop does in classic Hadoop stacks).
  * Large tables are read in parallel: numeric partition column + bounds -> several concurrent JDBC connections. */
object IngestRelational {
  def main(argv: Array[String]): Unit = {
    val spark = Common.spark("M3-ingest-relational")
    val bounds = Map("users" -> "user_id", "orders" -> "order_id", "order_items" -> "order_id", "products" -> "product_id", "inventory" -> "product_id")
    bounds.foreach { case (table, col) =>
      val (lo, hi) = {
        val r = spark.read.jdbc(Common.JdbcUrl, s"(select min($col) lo, max($col) hi from $table) q", Common.JdbcProps).head()
        (r.getLong(0), r.getLong(1))
      }
      val df = spark.read.option("fetchsize", "50000")
        .jdbc(Common.JdbcUrl, table, col, lo, hi, 4, Common.JdbcProps)
      df.write.mode(SaveMode.Overwrite).parquet(s"${Common.RelTables}/$table")
      println(s"[ingest] $table -> ${Common.RelTables}/$table rows=${spark.read.parquet(Common.RelTables + "/" + table).count()}")
    }
    spark.stop()
  }
}
