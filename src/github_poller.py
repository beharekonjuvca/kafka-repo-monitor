import os, time, json, requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

OWNER = os.getenv("OWNER", "apache")
REPO = os.getenv("REPO", "kafka")
URL = f"https://api.github.com/repos/{OWNER}/{REPO}/events"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("GITHUB_TOPIC", "github_events")
TOKEN = os.getenv("GITHUB_TOKEN")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
)

BASE_HEADERS = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "kafka-repo-monitor",
}
if TOKEN:
    BASE_HEADERS["Authorization"] = f"token {TOKEN}"

etag = None
seen_ids = set()   

def normalize(e: dict) -> dict:
    """Return a compact, consistent shape for all events."""
    evt_type = e.get("type")  
    actor = e.get("actor") or {}
    repo = e.get("repo") or {}
    created = e.get("created_at")
    payload = e.get("payload") or {}
    pr_num = (payload.get("pull_request") or {}).get("number")
    issue_num = (payload.get("issue") or {}).get("number")
    action = payload.get("action")
    commits = None
    stars = None
    if evt_type == "PushEvent":
        commits = len(payload.get("commits") or [])
    if evt_type in ("WatchEvent", "StarEvent"):
        stars = payload.get("action") 

    return {
        "id": e.get("id"),                      
        "type": evt_type,                        
        "created_at": created,                   
        "actor": actor.get("login"),
        "repo": repo.get("name"),                
        "action": action,                       
        "pr_number": pr_num,
        "issue_number": issue_num,
        "push_commits": commits,                 
        "star_action": stars,
        "_raw": e,                               
        "_event": evt_type,                      
    }

def poll_once() -> int:
    """Fetch newest events (if any), emit oldest→newest. Returns count."""
    global etag
    headers = BASE_HEADERS.copy()
    if etag:
        headers["If-None-Match"] = etag

    r = requests.get(URL, headers=headers, timeout=20)
    if r.status_code == 304:
        return 0
    r.raise_for_status()

    if "ETag" in r.headers:
        etag = r.headers["ETag"]

    events = r.json() or []
    produced = 0
    for e in reversed(events):
        eid = str(e.get("id"))
        if eid in seen_ids:     
            continue
        seen_ids.add(eid)
        out = normalize(e)
        producer.send(TOPIC, key=eid, value=out)
        produced += 1

    if produced:
        producer.flush()
    return produced

if __name__ == "__main__":
    print(f"Polling {URL} → Kafka topic '{TOPIC}' (CTRL+C to stop)")
    while True:
        try:
            n = poll_once()
            time.sleep(5 if n else 20)
        except requests.HTTPError as he:
            print("HTTP error:", he)
            time.sleep(30)
        except Exception as ex:
            print("Error:", ex)
            time.sleep(10)
