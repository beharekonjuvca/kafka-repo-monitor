import os, json, threading, queue, time
from collections import defaultdict, deque
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
import streamlit as st
import plotly.express as px
import pandas as pd

load_dotenv()
BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("METRICS_TOPIC", "repo_metrics")

st.set_page_config(page_title="Kafka Repo Monitor", layout="wide")

snap_q: "queue.Queue[dict]" = queue.Queue(maxsize=1000)
stop_flag = threading.Event()

def consume_metrics():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOT,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="dashboard-view",
        consumer_timeout_ms=10000
    )
    while not stop_flag.is_set():
        try:
            for msg in consumer:
                if stop_flag.is_set():
                    break
                snap_q.put(msg.value)
        except Exception as e:
            time.sleep(2)  

if "consumer_started" not in st.session_state:
    t = threading.Thread(target=consume_metrics, daemon=True)
    t.start()
    st.session_state.consumer_started = True

st.title("Kafka → GitHub Repo Metrics (live)")
st.caption(f"Kafka bootstrap: `{BOOT}` | topic: `{TOPIC}`")

if "snaps" not in st.session_state:
    st.session_state.snaps = deque(maxlen=500)

while True:
    try:
        st.session_state.snaps.append(snap_q.get_nowait())
    except queue.Empty:
        break

latest = st.session_state.snaps[-1] if st.session_state.snaps else None

col1, col2, col3, col4 = st.columns(4)
with col1:
    col1.metric("Open PRs tracked", latest.get("open_prs_tracked") if latest else 0)
with col2:
    avg_hrs = latest.get("avg_pr_merge_hours") if latest else None
    col2.metric("Avg PR merge time (hrs)", f"{avg_hrs:.1f}" if avg_hrs else "—")
with col3:
    col3.metric("Issue comments (run total)", latest.get("issue_comments") if latest else 0)
with col4:
    col4.metric("Forks (run total)", latest.get("forks") if latest else 0)

commits_by_day = latest.get("commits_by_day") if latest else {}
if commits_by_day:
    df = pd.DataFrame(
        [{"day": k, "commits": v} for k, v in sorted(commits_by_day.items())]
    )
    fig = px.line(df, x="day", y="commits", title="Commits per day")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Waiting for snapshots on `repo_metrics`…")

st.caption("Auto-refreshing…")
st.experimental_rerun() if False else None
