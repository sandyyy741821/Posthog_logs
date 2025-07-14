import requests
import json
import os
from datetime import datetime, timezone, timedelta
import time

# --- CONFIG ---
POSTHOG_API_KEY = os.environ["POSTHOG_API_KEY"]
POWER_BI_PUSH_URL = os.environ["POWER_BI_PUSH_URL"]
CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "last_processed_time.txt")
BATCH_SIZE = 10000

# --- Load last processed time ---
def load_last_processed_time():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            try:
                line = f.read().strip()
                return int(line.split()[0])
            except:
                pass
    default_start = datetime(2025, 4, 18, 0, 0, tzinfo=timezone.utc)
    return int(default_start.timestamp() * 1000)

# --- Save last processed time with IST ---
def save_last_processed_time(timestamp_ms):
    utc_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    ist_dt = utc_dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
    utc_str = utc_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    ist_str = ist_dt.strftime('%Y-%m-%d %H:%M:%S IST')
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(f"{timestamp_ms}  # {utc_str} [{ist_str}]\n")

# --- Fetch PostHog events ---
def fetch_posthog_events(from_ts_ms, to_ts_ms, retries=3):
    events = []
    url = "https://us.i.posthog.com/api/projects/148940/events/"
    headers = {"Authorization": f"Bearer {POSTHOG_API_KEY}"}

    after = datetime.fromtimestamp(from_ts_ms / 1000, tz=timezone.utc).isoformat()
    before = datetime.fromtimestamp(to_ts_ms / 1000, tz=timezone.utc).isoformat()

    params = {
        "after": after,
        "before": before,
        "limit": 1000
    }

    attempt = 1
    while url and attempt <= retries:
        try:
            response = requests.get(url, headers=headers, params=params if attempt == 1 else {}, timeout=60)
            if response.status_code == 429:
                print(f"Rate limit hit. Attempt {attempt}. Waiting 30s...")
                time.sleep(30)
                continue
            response.raise_for_status()
            data = response.json()
            page_events = data.get("results", [])
            events.extend(page_events)

            # Pagination: set next URL or exit
            url = data.get("next", None)
            params = {}  # clear params after first page
        except Exception as e:
            print(f"Error during fetch attempt {attempt}: {e}")
            time.sleep(10)
            attempt += 1
            continue

    return events

# --- Extract event timestamp in ms ---
def get_event_time(event):
    ts = event.get("timestamp")
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except:
            return 0
    return 0

# --- Transform for Power BI ---
def transform_events(events):
    transformed = []
    for e in events:
        props = e.get("properties", {})
        raw_time = e.get("timestamp", "")
        transformed.append({
            'event': e.get('event', ''),
            'caller': props.get('caller', ''),
            'endpoint': props.get('endpoint', ''),
            'level': props.get('level', ''),
            'method': props.get('method', ''),
            'timestamp': raw_time,
            'message': props.get('message', ''),
            'userId': props.get('userId', ''),
            'userRole': props.get('userRole', ''),
            'platform': props.get('$lib', '')
        })
    return transformed

# --- Push to Power BI ---
def push_to_powerbi_in_batches(data):
    headers = {"Content-Type": "application/json"}
    total = len(data)
    pushed_total = 0
    for i in range(0, total, BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        response = requests.post(POWER_BI_PUSH_URL, headers=headers, data=json.dumps(batch))
        if response.status_code == 200:
            pushed_total += len(batch)
            print(f"Pushed {len(batch)} rows (Batch {i} → {i + len(batch)})")
        else:
            print(f"Failed batch {i} → {i + len(batch)}:", response.status_code, response.text)
            return False
        time.sleep(2)  
    print(f"Total pushed to Power BI: {pushed_total} rows")
    return True

# --- MAIN ---
if __name__ == "__main__":
    print("-------------------------------------------------------------------------------")
    print(f"Script started at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("-------------------------------------------------------------------------------")

    last_ts = load_last_processed_time()
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

    from_utc = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
    to_utc = datetime.fromtimestamp(now_ts / 1000, tz=timezone.utc)
    from_ist = from_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    to_ist = to_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))

    print(f"⏱ Checkpoint range: {from_utc} → {to_utc} UTC [{from_ist.strftime('%Y-%m-%d %H:%M:%S')} → {to_ist.strftime('%Y-%m-%d %H:%M:%S')} IST]")

    events = fetch_posthog_events(last_ts, now_ts)
    print(f"🔹 Fetched {len(events)} raw events from PostHog")

    print("\nChecking total events day-by-day (UTC):")

    # # Get start & end dates (in date format, not datetime)
    # current_day = from_utc.date()
    # end_day = to_utc.date()

    # while current_day <= end_day:
    #     day_start = datetime(current_day.year, current_day.month, current_day.day, 0, 0, 0, tzinfo=timezone.utc)
    #     day_end = datetime(current_day.year, current_day.month, current_day.day, 23, 59, 59, tzinfo=timezone.utc)

    #     day_start_ts = int(day_start.timestamp() * 1000)
    #     day_end_ts = int(day_end.timestamp() * 1000)

    #     day_events = fetch_posthog_events(day_start_ts, day_end_ts)
    #     print(f"{current_day}: {len(day_events)} events")

    #     current_day += timedelta(days=1)


    if not events:
        print("No events fetched.")
        exit(0)

    # print("\nChecking timestamps in fetched events:")
    # for e in events[:5]:
    #     ts = get_event_time(e)
    #     if ts:
    #         print(f" - {ts} → {datetime.fromtimestamp(ts / 1000, tz=timezone.utc)}")
    #     else:
    #         print(" Missing/invalid timestamp in:", e)

    # Filter new events
    new_events = [e for e in events if (last_ts < get_event_time(e) <= now_ts)]
    print(f"\nNew events to push: {len(new_events)}")

    if new_events:
        new_events.sort(key=get_event_time)
        transformed = transform_events(new_events)
        success = push_to_powerbi_in_batches(transformed)
        if success:
            latest_ts = max(get_event_time(e) for e in new_events)
            save_last_processed_time(latest_ts)
            print(f"Checkpoint updated to: {latest_ts}")
    else:
        print("No new events to push.")
