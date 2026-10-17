#!/usr/bin/env python3
"""Replay recorded clickstream events into Kafka at a controlled rate (velocity is SIMULATED; the events are real).

Each event gets `emit_ms` = wall-clock time at send, which the streaming job uses as event time and for latency.
Keyed by user_session so one session's events stay ordered inside one partition.

Usage (inside the spark-client container):
  python3 src/python/kafka_producer.py --files data/json/events/2019-Nov-part-0030.jsonl --rate 2000 --max 200000
"""
import argparse, json, time
from kafka import KafkaProducer

ap = argparse.ArgumentParser()
ap.add_argument("--bootstrap", default="kafka:9092")
ap.add_argument("--topic", default="clicks")
ap.add_argument("--files", nargs="+", required=True)
ap.add_argument("--rate", type=int, default=1000, help="events per second")
ap.add_argument("--max", type=int, default=0, help="stop after N events (0 = whole files)")
a = ap.parse_args()

prod = KafkaProducer(bootstrap_servers=a.bootstrap, linger_ms=20, batch_size=64 * 1024, acks=1,
                     key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode())
sent, t0 = 0, time.time()
for fn in a.files:
    with open(fn) as f:
        for line in f:
            e = json.loads(line)
            if e["user_session"] is None:
                continue
            e["emit_ms"] = int(time.time() * 1000)
            prod.send(a.topic, key=e["user_session"], value=e)
            sent += 1
            if sent % a.rate == 0:                       # pace: one second worth of events per loop
                lag = sent / a.rate - (time.time() - t0)
                if lag > 0:
                    time.sleep(lag)
                print(f"sent={sent} elapsed={time.time()-t0:.1f}s", flush=True)
            if a.max and sent >= a.max:
                break
    if a.max and sent >= a.max:
        break
prod.flush()
print(f"done: {sent} events in {time.time()-t0:.1f}s ({sent/(time.time()-t0):.0f} ev/s)")
