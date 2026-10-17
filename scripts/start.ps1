# One-command start of the whole cluster (run from the project root: .\scripts\start.ps1)
# Needs Docker Desktop running. First run builds the spark-client image (needs internet).
docker compose up -d --build
Write-Host "UIs: HDFS http://localhost:9870 | YARN http://localhost:8088 | History http://localhost:19888 | Spark UI http://localhost:4040 (while a job runs)"
Write-Host "Check: .\scripts\smoke_test.ps1"
