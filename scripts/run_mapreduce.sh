#!/usr/bin/env bash
# Event-type count with classic Hadoop MapReduce (Hadoop Streaming) on the first N November raw shards.
# Usage: scripts/run_mapreduce.sh [N=10]     (cluster must be up)
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
N=${1:-10}
RM=bigdataproject-resourcemanager-1
H=/opt/hadoop-3.2.1
docker exec $RM mkdir -p /workspace_mr
docker cp src/mapreduce/. $RM:/workspace_mr/
docker exec $RM bash -c "hdfs dfs -rm -r -f /bda/tmp/mr_out /bda/tmp/mr_in; hdfs dfs -mkdir -p /bda/tmp/mr_in"
for i in $(seq 0 $((N-1))); do
  docker exec $RM hdfs dfs -cp /bda/raw/events/2019-Nov-part-$(printf %04d $i).jsonl /bda/tmp/mr_in/
done
CP="$H/share/hadoop/mapreduce/*:$H/share/hadoop/mapreduce/lib/*:$H/share/hadoop/common/*:$H/share/hadoop/common/lib/*:$H/share/hadoop/hdfs/*:$H/share/hadoop/hdfs/lib/*:$H/share/hadoop/yarn/*:$H/share/hadoop/yarn/lib/*"
docker exec -w /workspace_mr $RM hadoop jar $H/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -D mapreduce.job.name=eventtype-count -D mapreduce.map.memory.mb=512 -D mapreduce.reduce.memory.mb=512 \
  -D yarn.app.mapreduce.am.resource.mb=512 -D mapreduce.job.reduces=2 \
  -D "mapreduce.application.classpath=$CP" \
  -D "yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=$H" -D "mapreduce.map.env=HADOOP_MAPRED_HOME=$H" -D "mapreduce.reduce.env=HADOOP_MAPRED_HOME=$H" \
  -files mapper.sh,reducer.sh -mapper ./mapper.sh -reducer ./reducer.sh -input /bda/tmp/mr_in -output /bda/tmp/mr_out \
  && docker exec $RM bash -c "hdfs dfs -cat /bda/tmp/mr_out/part-* | sort"
