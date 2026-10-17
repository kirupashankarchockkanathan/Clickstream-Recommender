# Session-Aware Real-Time Recommendation over E-Commerce Clickstreams

GitHub: https://github.com/kirupashankarchockkanathan/Clickstream-Recommender

For a detailed IEEE-formatted report, see [docs/REPORT_IEEE.pdf](docs/REPORT_IEEE.pdf) (LaTeX source: `docs/REPORT_IEEE.tex`).
ALS fold-in with Kafka and Spark Structured Streaming on a Hadoop cluster (1 NameNode, 3 DataNodes, YARN) that runs on one 16 GB laptop.
Developed at the Amrita School of Artificial Intelligence, Amrita Vishwa Vidyapeetham.

## What it does
- Stores **109.95 M real click events** (REES46, Oct+Nov 2019; 26.3 GiB as JSON Lines) plus 5 PostgreSQL tables on HDFS (replication 3).
- Cleans the data with Scala/Spark on YARN (0.37 % of rows removed, every step counted) and writes day-partitioned Parquet.
- Trains MLlib implicit ALS and compares five recommenders on 159,629 held-out next-item cases with a strict temporal split.
- Replays events through Kafka into Spark Structured Streaming which returns recommendations per session (ALS fold-in, out-of-stock filter).
- Measures Spark/HDFS tuning (Parquet vs JSON, small files, broadcast join, partition pruning, shuffle partitions, caching).

Headline results (all files in `outputs/metrics/`, charts in `outputs/charts/`): HR@20 after one click: popularity 0.094, stale batch ALS 0.132, **ALS + fold-in 0.370**, item co-occurrence 0.440, hybrid 0.454. A simple co-occurrence method is still the best single model, in line with the base paper (Ludewig et al., UMUAI 2021). Streaming latency click-to-recommendation: p50 8.5 s, p99 20.1 s. See `docs/REPORT_IEEE.pdf` for the discussion and limitations (25 % user sample, synthetic users/inventory, simulated velocity, single runs).

## Repository layout
```
docker-compose.yml          cluster definition (HDFS, YARN, Kafka, PostgreSQL, Spark client)
docker/                     Spark client image, Hadoop client config
scripts/                    start/stop/smoke test, build, run_job, grid search, MapReduce
src/scala/                  Scala 2.12 / Spark 3.5.1 jobs (Preprocess, TrainEval, StreamingJob, ...)
src/python/                 converter, table builder, Kafka producer, charts
src/dashboard/app.py        Streamlit dashboard
src/mapreduce/              Hadoop Streaming mapper/reducer
src/sql/schema.sql          PostgreSQL schema
outputs/                    real metrics (JSON) and charts (PNG)
docs/                       REPORT_IEEE.pdf/.tex (paper), FINAL_REVIEW_GUIDE.md, SCREENSHOT_CHECKLIST.md, presentation/ (Beamer)
```
`data/` (raw and intermediate data) is not committed because of its size.

## How to run everything again
Requirements: Windows 11 + Docker Desktop (WSL2), `~/.wslconfig` with `memory=12GB`, about 150 GB free disk, internet for the first build.
1. `.\scripts\start.ps1` (first run builds the Spark client image), wait about 2 minutes, then `.\scripts\smoke_test.ps1`.
2. Data (needs about 5 GB download, 27 GiB JSON):
   ```
   mkdir data\raw
   curl -L -o data\raw\2019-Oct.csv.gz https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz
   curl -L -o data\raw\2019-Nov.csv.gz https://data.rees46.com/datasets/marketplace/2019-Nov.csv.gz
   python src/python/convert_to_jsonl.py data/raw/2019-Oct.csv.gz data/json/events data/tables/stage
   python src/python/convert_to_jsonl.py data/raw/2019-Nov.csv.gz data/json/events data/tables/stage
   python src/python/build_tables.py data/tables/stage data/tables
   ```
3. Load PostgreSQL and HDFS (Git Bash; set `MSYS_NO_PATHCONV=1`):
   ```
   P="docker exec -i bigdataproject-postgres-1 psql -q -U bda -d shop"
   $P < src/sql/schema.sql
   for t in users products inventory orders order_items; do $P -c "\copy $t from stdin with (format csv)" < data/tables/$t.csv; done
   docker exec bigdataproject-namenode-1 bash -c "hdfs dfs -mkdir -p /bda/raw/events /bda/lib && hdfs dfs -put -f /data/json/events/*.jsonl /bda/raw/events/"
   ```
4. Spark jar archive (once): `scripts/make_spark_archive.sh` (also needs `data/` mounted into the NameNode, which compose does).
5. `.\scripts\build_scala.ps1`, then `scripts/run_job.sh IngestRelational`, `scripts/run_job.sh Preprocess partitions=96` (about 30 min).
6. `scripts/run_job.sh TrainEval userPct=25 iters=8 testSessionPct=20 rank=64 alpha=40 reg=0.05 tag=final` (about 30 min), `scripts/run_job.sh TuningBench`, `scripts/run_mapreduce.sh 10`.
7. Streaming demo: see `docs/FINAL_REVIEW_GUIDE.md`. Charts: `python src/python/make_charts.py`. Dashboard: `docker exec -d -w /workspace bigdataproject-spark-client-1 streamlit run src/dashboard/app.py --server.port 8501 --server.address 0.0.0.0`, open http://localhost:8501.
8. Stop: `.\scripts\stop.ps1` (keeps HDFS/Kafka/PostgreSQL volumes; `-Wipe` deletes them).
UIs: HDFS http://localhost:9870, YARN http://localhost:8088, History http://localhost:19888, Spark UI http://localhost:4040 (while a job runs).

## Honest scope
Real: all click events, products, orders, metrics. Synthetic: user profiles, inventory. Simulated: event velocity (replay). Sampled: 25 % of users and 20 % of test sessions. Not done: neural/SKNN baselines, significance tests, multi-node hardware.
