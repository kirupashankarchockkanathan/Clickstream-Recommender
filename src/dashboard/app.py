"""Streamlit dashboard: results of the clickstream recommendation platform.
Run inside the spark-client container:  streamlit run src/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
Then open http://localhost:8501. All numbers come from outputs/metrics (real runs); the "live" tab reads Kafka topic `recs`.
"""
import json, os, time
import pandas as pd
import streamlit as st

M = "outputs/metrics"
st.set_page_config(page_title="Clickstream Recs", layout="wide")
st.title("Session-aware real-time recommendations: results")
st.caption("REES46 e-commerce clickstream, Oct+Nov 2019 | HDFS (3 DataNodes) + YARN + Spark + Kafka")


def rows(name):
    p = os.path.join(M, name)
    return pd.DataFrame([json.loads(l) for l in open(p, encoding="utf8") if l.startswith("{")]) if os.path.exists(p) else pd.DataFrame()


tab1, tab2, tab3, tab4, tab5 = st.tabs(["Accuracy", "Data quality", "Tuning", "ALS grid", "Live stream"])

with tab1:
    df = rows("eval_by_k_final.json")
    info = rows("eval_run_info_final.json")
    if not df.empty:
        metric = st.radio("Metric", ["HR_20", "MRR_20", "NDCG_20"], horizontal=True)
        pivot = df.pivot(index="k", columns="method", values=metric)
        st.bar_chart(pivot, stack=False)   # grouped, NOT stacked: the methods are alternatives, their scores must not add up
        st.dataframe(df.sort_values(["k", "method"]), width="stretch")
    if not info.empty:
        st.json(info.iloc[0].to_dict())
    st.info("Co-occurrence (kNN-style) is the strongest single method; ALS fold-in is far better than stale batch ALS. See the report for the discussion.")

with tab2:
    v = rows("preprocess_veracity.json")
    if not v.empty:
        st.dataframe(v, width="stretch")
        st.bar_chart(v.set_index("stage")["rows"])

with tab3:
    t = rows("tuning_bench.json")
    if not t.empty:
        for e, g in t.groupby("experiment"):
            st.subheader(e)
            st.bar_chart(g.set_index("variant")["seconds"])
        st.caption("Each experiment was run once; differences below ~15% are noise.")

with tab4:
    import glob
    out = []
    for f in sorted(glob.glob(os.path.join(M, "eval_by_k_val_*.json"))):
        d = pd.DataFrame([json.loads(l) for l in open(f) if l.startswith("{")])
        d = d[(d.method == "als_foldin") & (d.k == 1)]
        if not d.empty:
            out.append({"config": os.path.basename(f)[len("eval_by_k_val_"):-5], "NDCG_20": float(d.iloc[0]["NDCG_20"]), "HR_20": float(d.iloc[0]["HR_20"])})
    if out:
        st.dataframe(pd.DataFrame(out).sort_values("NDCG_20", ascending=False), width="stretch")

with tab5:
    st.write("Reads the last messages of Kafka topic `recs` (needs the cluster and the streaming job running).")
    if st.button("Fetch latest recommendations"):
        try:
            from kafka import KafkaConsumer, TopicPartition
            c = KafkaConsumer(bootstrap_servers="kafka:9092", consumer_timeout_ms=4000)
            tps = [TopicPartition("recs", p) for p in (c.partitions_for_topic("recs") or [])]
            c.assign(tps)
            ends = c.end_offsets(tps)
            for tp in tps:
                c.seek(tp, max(0, ends[tp] - 20))
            msgs = [json.loads(m.value) for m in c]
            if msgs:
                d = pd.DataFrame(msgs)
                d["latency_s"] = (d["done_ms"] - d["emit_ms"]) / 1000
                st.dataframe(d[["session", "user_id", "n_clicks", "last_item", "recs", "latency_s"]].tail(30), width="stretch")
            else:
                st.warning("No messages yet.")
        except Exception as e:  # noqa
            st.error(f"Kafka not reachable: {e}")
