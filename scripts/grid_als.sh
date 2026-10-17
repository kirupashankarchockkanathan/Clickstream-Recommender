#!/usr/bin/env bash
# Hyper-parameter search on a VALIDATION window (train <= Nov 13, validate Nov 14-20); the test window (Nov 21-30) is never used here.
cd "$(dirname "$0")/.."
i=0
for cfg in "64 40 0.05" "128 20 0.05"; do
  set -- $cfg; i=$((i+1))
  scripts/run_job.sh TrainEval userPct=10 iters=6 testSessionPct=20 trainEnd=2019-11-13 testStart=2019-11-14 testEnd=2019-11-20 rank=$1 alpha=$2 reg=$3 tag=val_r$1_a$2_l$3 > data/gridB_$i.log 2>&1
  echo "done $cfg"
done
