import json, time, re
from collections import defaultdict, deque, Counter
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = "localhost:9092"
IN_TOPIC = "github_events"
OUT_TOPIC = "repo_metrics"

def ts(): return datetime.now(timezone.utc).isoformat()
def iso2dt(s): 
    if not s: return None
    return datetime.fromisoformat(s.replace("Z","+00:00"))

BOT_RE = re.compile(r"\b(bot)\b", re.I)

commits_by_day = defaultdict(int)
open_pr_created = {}                  
pr_merge_hours = deque(maxlen=2000)
issue_comments = 0
forks = 0

WIN = 600
events_roll = deque()                 
events_by_type_roll = deque()         
actors_roll = deque()                 
files_roll = deque()                  
first_resp_watch = {}                 
first_resp_minutes = deque(maxlen=500)

PER_MIN = 60
BURST_WINDOW = 30
per_min_counts = deque(maxlen=BURST_WINDOW)

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
last_min_bucket = None
print("metrics processor listening …")

def prune(dq, now_ts, win=WIN):
    cut = now_ts - win
    while dq and dq[0][0] < cut:
        dq.popleft()

while True:
    for msg in consumer:
        e = msg.value
        etype = e.get("type")
        created = e.get("created_at")
        now_dt = datetime.now(timezone.utc)
        now_ts = time.time()
        today_str = now_dt.date().isoformat()

        events_roll.append((now_ts, 1))
        actors = []
        if isinstance(e.get("actor"), dict):
            actors.append(e["actor"].get("login"))
        elif isinstance(e.get("actor"), str):
            actors.append(e["actor"])
        for a in actors:
            if a:
                actors_roll.append((now_ts, a))
        events_by_type_roll.append((now_ts, etype or "Unknown"))

        cur_bucket = int(now_ts // PER_MIN)
        if last_min_bucket is None:
            last_min_bucket = cur_bucket
            per_min_counts.append(0)
        if cur_bucket != last_min_bucket:
            for _ in range(cur_bucket - last_min_bucket):
                per_min_counts.append(0)
            last_min_bucket = cur_bucket
        per_min_counts[-1] += 1

        if etype == "PushEvent":
            day = created[:10] if created else today_str
            n_commits = int(e.get("push_commits") or 0) or 1
            commits_by_day[day] += n_commits

        if etype == "PullRequestEvent":
            action = e.get("action")
            pr = e.get("pr_number")
            if action == "opened" and pr:
                dt_open = iso2dt(created) or now_dt
                open_pr_created[pr] = dt_open
                first_resp_watch[pr] = dt_open
            if action == "closed" and pr:
                pr_raw = (e.get("_raw") or {}).get("payload", {}).get("pull_request", {})
                merged_at = pr_raw.get("merged_at")
                if merged_at:
                    opened_at = open_pr_created.get(pr) or iso2dt(pr_raw.get("created_at"))
                    merged_dt = iso2dt(merged_at)
                    if opened_at and merged_dt:
                        pr_merge_hours.append((merged_dt - opened_at).total_seconds()/3600.0)
                open_pr_created.pop(pr, None)
                first_resp_watch.pop(pr, None)

        if etype in ("PullRequestReviewEvent", "PullRequestReviewCommentEvent"):
            pr_num = e.get("pr_number")
            if pr_num in first_resp_watch:
                opened_at = first_resp_watch.pop(pr_num)
                minutes = (iso2dt(created) - opened_at).total_seconds()/60.0
                if minutes >= 0:
                    first_resp_minutes.append(minutes)

            path = (e.get("_raw") or {}).get("payload", {}).get("comment", {}).get("path")
            if path:
                files_roll.append((now_ts, path))

        if etype == "IssueCommentEvent":
            issue_comments += 1

        if etype == "ForkEvent":
            forks += 1

        prune(events_roll, now_ts)
        prune(events_by_type_roll, now_ts)
        prune(actors_roll, now_ts)
        prune(files_roll, now_ts)

        if now_ts - last_emit >= EMIT_EVERY_SEC:
           
            events_last_10 = sum(v for _, v in events_roll)
            rate_per_min = round(events_last_10 / (WIN/60.0), 2)

            if len(per_min_counts) >= 2:
                mean = sum(per_min_counts) / len(per_min_counts)
                var = sum((x-mean)**2 for x in per_min_counts) / (len(per_min_counts)-1)
                burst = round(var / mean, 2) if mean > 0 else None
            else:
                burst = None

            actor_counts = Counter(a for _, a in actors_roll if a and not BOT_RE.search(a))
            top_actors = dict(actor_counts.most_common(5))
            active_actors = len(actor_counts)

            type_counts = Counter(t for _, t in events_by_type_roll)
            events_by_type_10 = dict(type_counts.most_common())

          
            hot_files = dict(Counter(p for _, p in files_roll).most_common(5))

            avg_merge = round(sum(pr_merge_hours)/len(pr_merge_hours), 2) if pr_merge_hours else None
            avg_first_resp_min = round(sum(first_resp_minutes)/len(first_resp_minutes), 1) if first_resp_minutes else None

            snapshot = {
                "ts": ts(),
                "commits_by_day": dict(commits_by_day),
                "avg_pr_merge_hours": avg_merge,
                "open_prs_tracked": len(open_pr_created),
                "issue_comments": issue_comments,
                "forks": forks,

                "events_per_min": rate_per_min,
                "events_by_type_last_10min": events_by_type_10,
                "active_actors_last_10min": active_actors,
                "top_actors_last_10min": top_actors,
                "hot_files_last_10min": hot_files,
                "avg_first_response_min": avg_first_resp_min,
                "burstiness_fano": burst,

                "commits_last_10min": sum(v for _, v in []), 
                "prs_opened_last_10min": sum(1 for _, a in events_by_type_roll if a=="PullRequestEvent"),
               
                "commits_today": commits_by_day.get(datetime.now(timezone.utc).date().isoformat(), 0),
            }
            producer.send(OUT_TOPIC, snapshot); producer.flush()
            print("emitted:", snapshot)
            last_emit = now_ts
