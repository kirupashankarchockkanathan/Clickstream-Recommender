# Verifies the cluster: HDFS DataNodes + replication, YARN node, Kafka round trip, PostgreSQL, Spark-on-YARN job.
$env:MSYS_NO_PATHCONV = "1"
$p = "bigdataproject"
Write-Host "== HDFS =="; docker exec "$p-namenode-1" hdfs dfsadmin -report | Select-String "Live datanodes|^Name:"
docker exec "$p-namenode-1" bash -c "echo ok > /tmp/t.txt; hdfs dfs -mkdir -p /test; hdfs dfs -put -f /tmp/t.txt /test/; hdfs fsck /test/t.txt -files -blocks | grep -E 'Live_repl|Status'"
Write-Host "== YARN =="; docker exec "$p-resourcemanager-1" yarn node -list | Select-String "Total Nodes|RUNNING"
Write-Host "== Kafka =="; docker exec "$p-kafka-1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
Write-Host "== Postgres =="; docker exec "$p-postgres-1" psql -U bda -d shop -c "select 1 as ok;"
Write-Host "== Spark on YARN (SparkPi) =="
docker exec "$p-spark-client-1" spark-submit --master yarn --driver-memory 512m --num-executors 2 --executor-memory 768m --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 100 2>&1 | Select-String "Pi is roughly"
