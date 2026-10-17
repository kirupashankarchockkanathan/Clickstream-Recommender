"""Demo job for the Spark-UI screenshots: runs a real aggregation over one week of cleaned events, then keeps the UI alive for 4 minutes."""
import time
from pyspark.sql import SparkSession, functions as F
s = SparkSession.builder.appName("Demo-DAG-clean-events").config("spark.sql.shuffle.partitions", "24").getOrCreate()
e = s.read.parquet("hdfs://namenode:9000/bda/clean/events").filter(F.col("event_date").between("2019-11-01", "2019-11-07"))
p = s.read.parquet("hdfs://namenode:9000/bda/clean/rel/products").select("product_id", "last_price")
r = (e.join(F.broadcast(p), "product_id").groupBy("event_date", "category_l1", "event_type")
     .agg(F.countDistinct("user_id").alias("users"), F.count("*").alias("events"), F.avg("price").alias("avg_price")).orderBy(F.desc("events")))
r.show(5, False)
time.sleep(240)
