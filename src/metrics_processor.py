import json, time
from collections import defaultdict, deque
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = "localhost:9092"
IN_TOPIC = "github_events"
OUT_TOPIC = "repo_metrics"

def ts():
    return datetime.now(timezone.utc).isoformat()

commits_by_day = defaultdict(int)
open_pr_created = {}           
pr_merge_hours = deque(maxlen=1000) 
issue_comments = 0
forks = 0

consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="metrics",
    auto_offset_reset="earliest",
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

last_emit = 0
EMIT_EVERY_SEC = 5

print("metrics processor listening …")
for msg in consumer:
    e = msg.value
    etype = e.get("type")

    if etype == "PushEvent":
        created = e.get("created_at") or ""
        day = created[:10] if created else datetime.utcnow().date().isoformat()
        commits_by_day[day] += int(e.get("push_commits") or 0)

    if etype == "PullRequestEvent":
        action = e.get("action")
        pr = e.get("pr_number")
        created = e.get("created_at")
        if action == "opened" and pr:
            open_pr_created[pr] = datetime.fromisoformat(created.replace("Z", "+00:00"))
        if action == "closed" and pr:
            pr_raw = e.get("_raw", {}).get("payload", {}).get("pull_request", {})
            merged_at = pr_raw.get("merged_at")
            if merged_at:
                opened_at = open_pr_created.get(pr)
                if not opened_at:
                    opened_at = datetime.fromisoformat(pr_raw.get("created_at").replace("Z","+00:00"))
                merged_dt = datetime.fromisoformat(merged_at.replace("Z","+00:00"))
                hours = (merged_dt - opened_at).total_seconds()/3600.0
                pr_merge_hours.append(hours)
            open_pr_created.pop(pr, None)

    if etype == "IssueCommentEvent":
        issue_comments += 1
    if etype == "ForkEvent":
        forks += 1

    now = time.time()
    if now - last_emit >= EMIT_EVERY_SEC:
        snapshot = {
            "ts": ts(),
            "commits_by_day": dict(commits_by_day),
            "avg_pr_merge_hours": (sum(pr_merge_hours)/len(pr_merge_hours) if pr_merge_hours else None),
            "open_prs_tracked": len(open_pr_created),
            "issue_comments": issue_comments,
            "forks": forks,
        }
        producer.send(OUT_TOPIC, snapshot)
        producer.flush()
        last_emit = now
        print("emitted:", snapshot)
