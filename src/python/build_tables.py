#!/usr/bin/env python3
"""Merge the per-month side extracts into relational CSV files ready for PostgreSQL COPY.

REAL (derived from the dataset): products(product_id, category_id, category_code, brand, last_price),
                                 orders / order_items (from 'purchase' events).
SYNTHETIC (generated, deterministic via hashing, NOT from the source):
    users(region, segment, signup_date)  and  inventory(stock_qty, warehouse).
Usage: python build_tables.py data/tables/stage data/tables
"""
import csv, glob, hashlib, os, sys, datetime

REGIONS = ["North", "South", "East", "West", "Central", "Overseas"]
SEGMENTS = ["new", "regular", "loyal", "bargain_hunter"]
WAREHOUSES = ["WH-A", "WH-B", "WH-C"]


def h(x, salt):
    return int(hashlib.md5(f"{salt}:{x}".encode()).hexdigest()[:8], 16)


def main(stage, out):
    os.makedirs(out, exist_ok=True)
    files = sorted(glob.glob(os.path.join(stage, "*_products.csv")))
    prods = {}
    for fn in files:                                     # later months overwrite -> latest attributes/price win
        for r in csv.reader(open(fn, newline="")):
            prods[r[0]] = r[1:]
    with open(os.path.join(out, "products.csv"), "w", newline="") as f, open(os.path.join(out, "inventory.csv"), "w", newline="") as g:
        w, iw = csv.writer(f), csv.writer(g)
        for pid, v in prods.items():
            w.writerow([pid, *v])
            iw.writerow([pid, WAREHOUSES[h(pid, "wh") % 3], h(pid, "stock") % 500])   # synthetic stock 0..499 (0 = out of stock)
    users = set()
    for fn in sorted(glob.glob(os.path.join(stage, "*_users.txt"))):
        users.update(open(fn).read().split())
    base = datetime.date(2015, 1, 1)
    with open(os.path.join(out, "users.csv"), "w", newline="") as f:
        w = csv.writer(f)
        for u in users:
            w.writerow([u, REGIONS[h(u, "r") % 6], SEGMENTS[h(u, "s") % 4], (base + datetime.timedelta(days=h(u, "d") % 1700)).isoformat()])
    n_orders = 0
    with open(os.path.join(out, "orders.csv"), "w", newline="") as fo, open(os.path.join(out, "order_items.csv"), "w", newline="") as fi:
        wo, wi = csv.writer(fo), csv.writer(fi)
        seen = {}
        for fn in sorted(glob.glob(os.path.join(stage, "*_purchases.csv"))):
            for sess, user, t, pid, price in csv.reader(open(fn, newline="")):
                if sess not in seen:                     # one order per (session) that has purchases
                    n_orders += 1
                    seen[sess] = n_orders
                    wo.writerow([n_orders, user, sess, t])
                wi.writerow([seen[sess], pid, price])
    print("products", len(prods), "users", len(users), "orders", n_orders)


if __name__ == "__main__":
    main(*sys.argv[1:3])
