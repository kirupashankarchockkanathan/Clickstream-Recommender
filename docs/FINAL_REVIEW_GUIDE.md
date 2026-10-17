# Final Review guide (15 min presentation + demo, 10 min viva)

## Before the review (the day before)
1. Docker Desktop started; `.\scripts\start.ps1`; wait 2-3 minutes; `.\scripts\smoke_test.ps1` must show 3 live DataNodes and "Pi is roughly".
2. Close other heavy programs (Chrome tabs, MATLAB). Do not run training live: it takes 30 min. Show stored results.
3. Rehearse the streaming demo once (below). Keep `outputs/charts/*.png` and the deck open as a fallback.
4. Fill the red screenshot boxes (see `SCREENSHOT_CHECKLIST.md`), compile the deck, put the GitHub link on slide 10, upload PPT and report.

## Suggested 15-minute flow
| Min | What | Slide / screen |
|---|---|---|
| 0-1 | Title, team, faculty | 1 |
| 1-3 | Problem, objectives, 5 Vs, base paper and gap | 2 |
| 3-4 | Dataset: 109.95 M events, 26.3 GiB JSON, 5 tables, real vs synthetic | 3 (+ HDFS UI `/bda/raw/events`) |
| 4-5 | Architecture walk-through | 4 |
| 5-7 | Spark workflow, DAG, partitioning, caching; honest caching result | 5 (+ YARN UI) |
| 7-8 | Scala code: FoldIn, case classes, pattern matching | 6 |
| 8-9 | ML pipeline: features, ALS, fold-in, tuning on validation window | 7 |
| 9-11 | Results table/chart and business insight | 8 |
| 11-13 | **Live demo** (below) | terminal + browser |
| 13-14 | Limitations, conclusion, future work | 9 |
| 14-15 | References, GitHub | 10 |

## Live demo script (about 3 minutes, cluster already running)
1. HDFS UI http://localhost:9870 > Datanodes: three live. Say: "1 NameNode, 3 DataNodes, replication 3".
2. `docker exec bigdataproject-namenode-1 hdfs fsck /bda/raw -blocks` : "Average block replication 3.0, HEALTHY".
3. Start the streaming job (takes about 60-90 s to come up, so start it first thing in the talk, before slide 2):
   `scripts/run_job.sh StreamingJob duration=600 trigger=5 topN=10`
4. When the log prints `[stream] item factors=45863`, start the producer:
   `docker exec bigdataproject-spark-client-1 python3 /workspace/src/python/kafka_producer.py --files /workspace/data/json/events/2019-Nov-part-0064.jsonl --rate 1500 --max 60000`
5. Show the recommendations arriving:
   `docker exec bigdataproject-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic recs --from-beginning --max-messages 5`
   Explain one message: session id, number of clicks, last item, 10 recommended item ids, timestamps (latency = done_ms - emit_ms).
6. YARN UI http://localhost:8088 shows the "M4-streaming" application RUNNING; dashboard http://localhost:8501 (start it as in the checklist).
Fallbacks: if the streaming job does not start, show `outputs/metrics/recs_stream_sample_all.jsonl` and `outputs/charts/stream_latency.png` (a real earlier run) and say so. Never claim a fallback is live.

## Likely questions and honest answers
1. **Why is this big data?** 110 M events, 26 GB raw (79 GB replicated), streaming, mixed formats; a single Python process cannot clean it in minutes and Pandas would not fit 26 GB in 16 GB RAM.
2. **Why 3 DataNodes and replication 3?** Faculty asked for at least 2 DataNodes and default replication. Default is 3; with only 2 DataNodes HDFS would be permanently under-replicated, so we used 3. They share one SSD, so it shows the mechanism, not real fault tolerance.
3. **What is novel?** Distributed, streaming ALS fold-in evaluated against kNN-style baselines with a leakage-free temporal split plus tuning measurements. We do not claim nobody has combined these; our survey was limited.
4. **Which paper is the base paper and what does yours add?** Ludewig et al., UMUAI 2021: simple neighbour methods beat complex models, single machine, offline. We add a Spark/YARN/Kafka implementation, scale, streaming latency, and a fold-in variant of ALS. Our results agree with theirs in direction.
5. **Did your method win?** No single method beat co-occurrence; fold-in is 2.8x better than stale ALS; hybrid is best only at the first click (HR@20 0.454 vs 0.440). We report that plainly.
6. **What is ALS fold-in?** A new session's vector is computed by solving a small ridge regression against the fixed item vectors, instead of retraining ALS (which takes minutes). Cost about 6 ms.
7. **Why is fold-in worse with more clicks?** We do not know; we did not investigate. Hypotheses: weights, rank, popularity bias.
8. **How did you avoid leakage?** Temporal split: train up to Nov 20, test Nov 21-30; prefix weights use only first events; hyper-parameters tuned on a separate validation window (Nov 14-20).
9. **Is the data real?** Events, products and orders are real (REES46). Users and inventory are synthetic. Velocity is a replay.
10. **JSON vs CSV rule?** The download is CSV; we convert to JSON Lines and drop the CSV. The system uses JSON + relational tables.
11. **Why Parquet?** Columnar and compressed: 5.1 GiB vs 26.3 GiB, scans 8x faster in our test.
12. **Did caching help?** Not for three queries (slower because of the fill); only on re-use.
13. **Is latency real-time?** Median 8.5 s, p99 20 s; it is near-real-time on one NodeManager with a 5 s trigger.
14. **Did you use MapReduce?** Yes, a Hadoop Streaming job counting event types (20 map tasks); it matches Spark exactly. It is shell/awk because the Hadoop containers have no Python.
15. **What would you do with more time?** Significance tests, more baselines (GRU4Rec on Kaggle GPU), larger user sample, multi-broker Kafka.
