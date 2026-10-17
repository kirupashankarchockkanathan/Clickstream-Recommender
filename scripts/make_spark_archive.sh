#!/usr/bin/env bash
# One-time: put an archive of the Spark jars into HDFS so every spark-submit does not re-upload ~300 MB (spark.yarn.archive).
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
mkdir -p data/tmp
docker exec bigdataproject-spark-client-1 python3 -c "
import zipfile, glob, os
z = zipfile.ZipFile('/workspace/data/tmp/spark-libs.zip', 'w', zipfile.ZIP_STORED)
for f in glob.glob('/opt/spark/jars/*.jar'): z.write(f, os.path.basename(f))
z.close()"
docker exec bigdataproject-namenode-1 bash -c "hdfs dfs -mkdir -p /bda/lib && hdfs dfs -put -f /data/tmp/spark-libs.zip /bda/lib/ && hdfs dfs -ls /bda/lib"
