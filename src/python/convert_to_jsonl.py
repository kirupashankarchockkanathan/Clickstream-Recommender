#!/usr/bin/env python3
"""Stream one REES46 monthly dump (CSV.gz) -> JSON Lines shards, plus side extracts for the relational tables.

Usage: python convert_to_jsonl.py data/raw/2019-Oct.csv.gz data/json/events data/tables/stage

Raw data is NOT cleaned here (nulls, duplicates and bad prices are kept as-is); cleaning is done later in Spark (Scala)
so that the veracity step is measurable. Each output shard is ~SHARD_MB of JSON so HDFS blocks/partitions are even.
"""
import csv, gzip, json, os, sys

SHARD_MB = 256
COLS = ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"]


def main(src, out_dir, stage_dir):
    month = os.path.basename(src).split(".")[0]          # e.g. 2019-Oct
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(stage_dir, exist_ok=True)
    products = {}                                        # product_id -> (category_id, category_code, brand, last_price)
    users = set()
    purchases = open(os.path.join(stage_dir, f"{month}_purchases.csv"), "w", newline="")
    pw = csv.writer(purchases)
    shard, written, n = 0, 0, 0
    out = open(os.path.join(out_dir, f"{month}-part-{shard:04d}.jsonl"), "w")
    with gzip.open(src, "rt", newline="") as f:
        rd = csv.reader(f)
        header = next(rd)
        assert header == COLS, header
        for r in rd:
            n += 1
            rec = {
                "event_time": r[0], "event_type": r[1],
                "product_id": int(r[2]) if r[2] else None,
                "category_id": int(r[3]) if r[3] else None,
                "category_code": r[4] or None, "brand": r[5] or None,
                "price": float(r[6]) if r[6] else None,
                "user_id": int(r[7]) if r[7] else None,
                "user_session": r[8] or None,
            }
            line = json.dumps(rec, separators=(",", ":")) + "\n"
            out.write(line)
            written += len(line)
            if rec["product_id"] is not None:
                products[rec["product_id"]] = (rec["category_id"], rec["category_code"], rec["brand"], rec["price"])
            if rec["user_id"] is not None:
                users.add(rec["user_id"])
            if r[1] == "purchase":
                pw.writerow([r[8], r[7], r[0], r[2], r[6]])
            if written >= SHARD_MB * 1024 * 1024:
                out.close(); shard += 1; written = 0
                out = open(os.path.join(out_dir, f"{month}-part-{shard:04d}.jsonl"), "w")
            if n % 5_000_000 == 0:
                print(month, n, "rows", flush=True)
    out.close(); purchases.close()
    with open(os.path.join(stage_dir, f"{month}_products.csv"), "w", newline="") as pf:
        w = csv.writer(pf)
        for pid, v in products.items():
            w.writerow([pid, *v])
    with open(os.path.join(stage_dir, f"{month}_users.txt"), "w") as uf:
        uf.write("\n".join(map(str, users)))
    print(f"{month}: {n} rows, {shard + 1} shards, {len(products)} products, {len(users)} users", flush=True)


if __name__ == "__main__":
    main(*sys.argv[1:4])
