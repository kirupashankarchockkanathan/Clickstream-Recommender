#!/bin/bash
# Hadoop Streaming mapper (shell version of the lab map.py; the Hadoop containers have no Python).
# Input: one JSON event per line. Output: "<event_type>\t1"
sed -nE 's/.*"event_type":"([a-z_]+)".*/\1\t1/p'
