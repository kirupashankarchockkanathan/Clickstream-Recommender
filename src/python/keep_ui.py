"""Keeps a Spark application (and so its web UI on port 4040) alive for hours for demos. Runs a few real queries first so the Jobs, Stages, SQL and Storage tabs have content."""
import time
from pyspark.sql import SparkSession, functions as F
s = SparkSession.builder.appName("Demo-SparkUI-keepalive").config("spark.sql.shuffle.partitions", "24").getOrCreate()
e = s.read.parquet("hdfs://namenode:9000/bda/clean/events").filter(F.col("event_date").between("2019-11-01", "2019-11-07"))
p = s.read.parquet("hdfs://namenode:9000/bda/clean/rel/products").select("product_id", "last_price")
e.groupBy("event_date", "event_type").count().orderBy("event_date").show(30, False)
e.join(F.broadcast(p), "product_id").groupBy("category_l1").agg(F.count("*").alias("events")).orderBy(F.desc("events")).show(10, False)
c = e.cache(); c.count()
print("[ui] ready, http://localhost:4040", flush=True)
time.sleep(12 * 3600)
