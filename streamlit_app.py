import os, json, time, traceback
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
import streamlit as st
import pandas as pd
import plotly.express as px

load_dotenv()
BOOT  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("METRICS_TOPIC", "repo_metrics")

st.set_page_config(page_title="Repo Metrics – quick view", layout="wide")
st.title("Repo Metrics – quick view")
st.caption(f"Kafka: `{BOOT}` • topic: `{TOPIC}`")

def read_all_from_beginning(topic: str, timeout_ms=8000, max_records=500):
    """Hard-seek to earliest for all partitions and read a batch."""
    c = KafkaConsumer(
        bootstrap_servers=[BOOT],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=False,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=15000,
        client_id=f"quick-view-{int(time.time())}",
        consumer_timeout_ms=timeout_ms,
    )

    topics = c.topics()
    if topic not in topics:
        return [], {"topics_seen": sorted(list(topics)), "note": f"Topic '{topic}' not in cluster topics."}

    parts = list(c.partitions_for_topic(topic) or [])
    tps = [TopicPartition(topic, p) for p in parts]
    if not tps:
        return [], {"topics_seen": sorted(list(topics)), "note": f"No partitions for topic '{topic}'."}

    c.assign(tps)
    c.seek_to_beginning(*tps)


    rows = []
    end = time.time() + (timeout_ms / 1000.0)
    while time.time() < end and len(rows) < max_records:
        batch = c.poll(timeout_ms=1000, max_records=max_records - len(rows))
        for _tp, records in batch.items():
            for r in records:
                try:
                    rows.append(r.value)
                except Exception:
                    pass
        if not batch:
            
            time.sleep(0.2)
    c.close()
    return rows, {"topics_seen": sorted(list(topics)), "partitions": parts, "read": len(rows)}

try:
    rows, dbg = read_all_from_beginning(TOPIC)
except Exception as e:
    st.error("Kafka read error:")
    st.code(traceback.format_exc())
    st.stop()

with st.expander("Debug info", expanded=False):
    st.write(dbg)

if not rows:
    st.warning("No snapshots found on the topic (from the beginning) within the timeout. "
               "If your metrics processor is running, emit one more snapshot and refresh.")
    st.stop()

st.success(f"Loaded {len(rows)} snapshot(s).")
latest = rows[-1]

with st.expander("Latest snapshot (raw JSON)", expanded=True):
    st.json(latest)

df = pd.DataFrame([{
    "ts": r.get("ts"),
    "issue_comments": r.get("issue_comments", 0),
    "forks": r.get("forks", 0),
    "commits_today": r.get("commits_today", 0),
    "commits_last_10min": r.get("commits_last_10min", 0),
    "prs_opened_last_10min": r.get("prs_opened_last_10min", 0),
    "prs_closed_last_10min": r.get("prs_closed_last_10min", 0),
    "issue_comments_last_10min": r.get("issue_comments_last_10min", 0),
    "forks_last_10min": r.get("forks_last_10min", 0),
} for r in rows])

st.subheader("Snapshots (table)")
st.dataframe(df, use_container_width=True)

df_time = df[["ts", "issue_comments", "forks"]].copy()
df_time["ts"] = pd.to_datetime(df_time["ts"], errors="coerce")
df_time = df_time.dropna(subset=["ts"])
st.subheader("Totals over time")
st.line_chart(df_time.set_index("ts"))


last10 = latest
pie_df = pd.DataFrame({
    "Event": ["Commits", "PRs opened", "PRs closed", "Issue comments", "Forks"],
    "Count": [
        last10.get("commits_last_10min", 0),
        last10.get("prs_opened_last_10min", 0),
        last10.get("prs_closed_last_10min", 0),
        last10.get("issue_comments_last_10min", 0),
        last10.get("forks_last_10min", 0),
    ],
})
st.subheader("Last 10 minutes breakdown")
st.plotly_chart(px.pie(pie_df, names="Event", values="Count"), use_container_width=True)
