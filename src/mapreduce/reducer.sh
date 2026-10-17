#!/bin/bash
# Hadoop Streaming reducer (shell/awk version of the lab reduce.py): sums the 1s per key (keys arrive sorted).
awk -F'\t' '{c[$1]+=$2} END {for (k in c) print k "\t" c[k]}'
