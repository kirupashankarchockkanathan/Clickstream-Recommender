# Screenshot checklist (take these yourself; they go into `docs/presentation/main.tex`)

> **Done (2026-10-17):** all 10 screenshots were captured automatically and are in `docs/presentation/shots/`; the deck is compiled to `docs/presentation/main.pdf`. The table below remains the reference for what each image shows and how to retake it. shot7 and shot9 are real command output rendered as images (see docs/presentation/README.md).

Start the cluster first: `.\scripts\start.ps1`, wait about 2 minutes. Use Windows key + Shift + S to capture. Save each image in `docs/presentation/shots/` as `shotN.png` (N = number below), then in `main.tex` replace the matching `\shot{...}{...}{N: ...}` box with `\includegraphics[width=\linewidth]{shots/shotN.png}`.

| # | Capture | Screen / URL | Slide |
|---|---|---|---|
| 1 | Raw data in HDFS: list of the 106 `2019-*-part-*.jsonl` files | http://localhost:9870 > Utilities > Browse the file system > `/bda/raw/events` | 3 |
| 2 | DAG of the cleaning job | run `scripts/run_job.sh Preprocess in=hdfs://namenode:9000/bda/sample/events out=hdfs://namenode:9000/bda/sample/clean` (fast, 200k rows) and open http://localhost:4040 > Jobs > click a job > "DAG Visualization"; or the YARN UI http://localhost:8088 if the job is gone | 5 |
| 3 | VS Code with the project tree on the left and `FoldIn.scala` open | VS Code, open the project folder, `src/scala/src/main/scala/bda/FoldIn.scala` | 6 |
| 4 | YARN applications list showing finished Spark jobs (TrainEval, Preprocess) | http://localhost:8088 > Applications (the list is kept until the ResourceManager restarts; re-run a short job if empty) | 7 |
| 5 | Dashboard, Accuracy tab with the bar chart | `docker exec -d -w /workspace bigdataproject-spark-client-1 streamlit run src/dashboard/app.py --server.port 8501 --server.address 0.0.0.0` then http://localhost:8501 | 8 |
| 6 | Three live DataNodes | http://localhost:9870 > Datanodes tab | backup |
| 7 | Replication 3 | PowerShell: `docker exec bigdataproject-namenode-1 hdfs fsck /bda/raw -blocks` (show "Average block replication 3.0", HEALTHY) | backup |
| 8 | Clean data by day in HDFS | http://localhost:9870 browse `/bda/clean/events` | backup |
| 9 | Messages on the recs topic | PowerShell: `docker exec bigdataproject-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic recs --from-beginning --max-messages 5` | backup |
| 10 | Streaming job running | start `scripts/run_job.sh StreamingJob duration=300`, run the producer (see FINAL_REVIEW_GUIDE.md), capture http://localhost:8088 with the "M4-streaming" application RUNNING | backup |

Also useful for the viva: Docker Desktop > Containers (all `bigdataproject` containers green), and http://localhost:8088/cluster showing 1 active node with 3.5 GB.
Charts in `outputs/charts/` are already real and need no screenshot.
