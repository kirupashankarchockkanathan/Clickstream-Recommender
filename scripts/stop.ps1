# One-command stop. Keeps all data (HDFS/Kafka/Postgres volumes). Add -Wipe to delete the volumes too.
param([switch]$Wipe)
if ($Wipe) { docker compose down -v } else { docker compose down }
