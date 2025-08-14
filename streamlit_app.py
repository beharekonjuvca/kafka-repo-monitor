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

def read_all_from_beginning(topic: str, timeout_ms=8000, max_records=800):
    """Read messages from earliest offsets (one-shot)."""
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
        return [], {"topics_seen": sorted(list(topics)), "note": f"Topic '{topic}' not found."}

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
except Exception:
    st.error("Kafka read error:")
    st.code(traceback.format_exc())
    st.stop()

with st.expander("Debug info", expanded=False):
    st.write(dbg)

if not rows:
    st.warning("No snapshots found on the topic. Emit one snapshot and refresh.")
    st.stop()

st.success(f"Loaded {len(rows)} snapshot(s).")
latest = rows[-1]

with st.expander("Latest snapshot (raw JSON)", expanded=True):
    st.json(latest)

df = pd.DataFrame([{
    "ts": r.get("ts"),
    "issue_comments": r.get("issue_comments", 0),
    "forks": r.get("forks", 0),
    "stars": r.get("stars", 0),
    "commits_today": r.get("commits_today", 0),
    "commits_last_10min": r.get("commits_last_10min", 0),
    "prs_opened_last_10min": r.get("prs_opened_last_10min", 0),
    "prs_closed_last_10min": r.get("prs_closed_last_10min", 0),
    "issue_comments_last_10min": r.get("issue_comments_last_10min", 0),
    "forks_last_10min": r.get("forks_last_10min", 0),
    "stars_last_10min": r.get("stars_last_10min", 0),
    "events_by_type_last_10min": r.get("events_by_type_last_10min", None),
} for r in rows])

zero_cols = [
    "commits_last_10min","prs_opened_last_10min","prs_closed_last_10min",
    "issue_comments_last_10min","forks_last_10min","stars_last_10min",
    "commits_today"
]
df_display = df.copy()
for col in zero_cols:
    if col in df_display.columns:
        df_display[col] = df_display[col].replace(0, pd.NA)

st.subheader("Snapshots (table)")
st.dataframe(df_display.drop(columns=["events_by_type_last_10min"], errors="ignore"), use_container_width=True)

now_epoch = time.time()
def agg_last_n_min(dfin, n):
    d = dfin.copy()
    tsdt = pd.to_datetime(d["ts"], errors="coerce")
    try:
        d["ts_epoch"] = tsdt.view("int64") / 1e9
    except Exception:
        d["ts_epoch"] = tsdt.astype("int64") / 1e9
    cutoff = now_epoch - n * 60
    return d[d["ts_epoch"] >= cutoff]

for mins in [1, 5, 10]:
    agg = agg_last_n_min(df, mins)
    st.subheader(f"Activity in last {mins} minute(s)")
    st.write(
        agg[[
            "issue_comments",
            "forks",
            "stars",
            "commits_last_10min",
            "prs_opened_last_10min",
            "prs_closed_last_10min",
            "stars_last_10min"
        ]].sum(min_count=1)
    )

def calc_activity(row):
    ev = row.get("events_by_type_last_10min")
    if isinstance(ev, dict):
        try:
            return int(sum(int(v) for v in ev.values()))
        except Exception:
            return None
    fields = [
        "commits_last_10min","prs_opened_last_10min","prs_closed_last_10min",
        "issue_comments_last_10min","forks_last_10min","stars_last_10min"
    ]
    return sum(int(row.get(f, 0) or 0) for f in fields)

df["activity_last_10min"] = df.apply(calc_activity, axis=1)

st.subheader("Total activity (last 10 min) sparkline")
df_act = df[["ts", "activity_last_10min"]].copy()
df_act["ts"] = pd.to_datetime(df_act["ts"], errors="coerce")
df_act = df_act.dropna(subset=["ts"])
st.line_chart(df_act.set_index("ts"))

st.subheader("Totals over time (stars, forks, comments)")
df_time = df[["ts", "stars", "forks", "issue_comments"]].copy()
df_time["ts"] = pd.to_datetime(df_time["ts"], errors="coerce")
df_time = df_time.dropna(subset=["ts"])
st.line_chart(df_time.set_index("ts"))

ev = latest.get("events_by_type_last_10min") or {}
if isinstance(ev, dict) and ev:
    rename = {
        "PushEvent": "Commits",
        "PullRequestEvent": "PRs",
        "PullRequestReviewEvent": "PR reviews",
        "PullRequestReviewCommentEvent": "PR review comments",
        "IssueCommentEvent": "Issue comments",
        "ForkEvent": "Forks",
        "WatchEvent": "Stars",
        "StarEvent": "Stars",
    }
    pie_df = pd.DataFrame({
        "Event": [rename.get(k, k) for k in ev.keys()],
        "Count": [int(v) for v in ev.values()],
    })
else:
    pie_df = pd.DataFrame({
        "Event": ["Commits", "PRs opened", "PRs closed", "Issue comments", "Forks", "Stars"],
        "Count": [
            latest.get("commits_last_10min", 0) or 0,
            latest.get("prs_opened_last_10min", 0) or 0,
            latest.get("prs_closed_last_10min", 0) or 0,
            latest.get("issue_comments_last_10min", 0) or 0,
            latest.get("forks_last_10min", 0) or 0,
            latest.get("stars_last_10min", 0) or 0,
        ],
    })

st.subheader("Last 10 minutes breakdown (including stars)")
st.plotly_chart(px.pie(pie_df, names="Event", values="Count"), use_container_width=True)

st.metric("Stars (total)", latest.get("stars", 0))
