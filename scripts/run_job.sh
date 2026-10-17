#!/usr/bin/env bash
# Usage: scripts/run_job.sh <ScalaClass> [key=value ...]   (cluster must be up, jar built with build_scala.ps1)
# Resource knobs via env: EXEC_MEM (default 1280m) EXEC_NUM (2) EXEC_CORES (2) DRIVER_MEM (768m)
export MSYS_NO_PATHCONV=1
CLASS="$1"; shift
JARS=$(docker exec bigdataproject-spark-client-1 bash -c 'ls /opt/extra-jars/*.jar | tr "\n" ","' | sed 's/,$//')
docker exec bigdataproject-spark-client-1 spark-submit \
  --master yarn --deploy-mode client \
  --conf spark.yarn.archive=hdfs://namenode:9000/bda/lib/spark-libs.zip \
  --driver-memory ${DRIVER_MEM:-768m} --num-executors ${EXEC_NUM:-2} \
  --executor-memory ${EXEC_MEM:-1280m} --executor-cores ${EXEC_CORES:-2} \
  --conf spark.sql.adaptive.enabled=true --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --jars "$JARS" --class "bda.$CLASS" /workspace/src/scala/target/scala-2.12/clickstream-recs_2.12-0.1.0.jar "$@"
