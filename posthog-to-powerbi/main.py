import requests
import json
import os
from datetime import datetime, timezone, timedelta
import time

# --- CONFIG ---
POSTHOG_API_KEY = "phx_TtcdtTGuh9zx04dRMUYrWns0vjNFgk04LnVwetHeIUH56lU"
BACKEND_POWER_BI_PUSH_URL = "https://api.powerbi.com/beta/4a2ebf79-c54a-4ae4-a274-2f55027091ce/datasets/5824b765-02e5-4593-9d6b-08e2a82da91a/rows?experience=power-bi&key=d%2Bk%2FV8lA02KX0OAQ%2FeL9T9jr4pLpdD7ZLVNlsXddJJ2LeCgjH0%2BnZFZ7Wi%2BAtA5b%2BUddu3xWeNsbMyuqAI3J8g%3D%3D"
FRONTEND_POWER_BI_PUSH_URL = "https://api.powerbi.com/beta/4a2ebf79-c54a-4ae4-a274-2f55027091ce/datasets/e7cf953b-60f8-4db3-9c21-70ff7e67f909/rows?experience=power-bi&key=cA3Pq4NItQ3Cwdae%2FTMiPG6qvnVIaPyZ4g3i96Pnty4MO4Y1FMRjddngyYSat4XOXNQdIbgRtB5H3wUB8heDcg%3D%3D"
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
def fetch_posthog_events(from_ts_ms, to_ts_ms, max_retries=3):
    events = []
    base_url = "https://us.i.posthog.com/api/projects/148940/events/"
    headers = {"Authorization": f"Bearer {POSTHOG_API_KEY}"}

    after = datetime.fromtimestamp(from_ts_ms / 1000, tz=timezone.utc).isoformat()
    before = datetime.fromtimestamp(to_ts_ms / 1000, tz=timezone.utc).isoformat()

    params = {
        "after": after,
        "before": before,
        "limit": 1000
    }

    url = base_url
    while url:
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                if response.status_code == 429:
                    print("🔁 Rate limited by PostHog. Waiting 30 seconds...")
                    time.sleep(30)
                    retries += 1
                    continue
                response.raise_for_status()

                data = response.json()
                page_events = data.get("results", [])
                events.extend(page_events)

                # Prepare for next page
                url = data.get("next")
                params = {}  # only needed for the first page
                break  # Break retry loop if successful
            except Exception as e:
                print(f"Error fetching PostHog events (try {retries + 1}): {e}")
                retries += 1
                time.sleep(5)

        else:
            # After all retries fail
            print("Skipping remaining pages after multiple failures.")
            break

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

def safe_unix_to_ist(ms):
    try:
        if not ms or not isinstance(ms, (int, float)):
            return ''
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return ''



# --- Transform for Power BI ---
def transform_events(events):
    transformed = []
    truncated_count = 0
    max_len = 4000
    suffix = "..."

    for e in events:
        props = e.get("properties", {})
        timestamp_UTC = e.get("timestamp", "")
        message = props.get('message', '')
        try:
            utc_time = datetime.fromisoformat(timestamp_UTC.replace("Z", "+00:00"))
            timestamp_IST = utc_time.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            timestamp_IST = ''
        web_vitals_INP = props.get('$web_vitals_INP_event', {})
        attribute_INP = web_vitals_INP.get('attribution', {})
        web_vitals_CLS = props.get('$web_vitals_CLS_event', {})
        attribute_CLS = web_vitals_CLS.get('attribution', {})

        if props.get('$lib') == 'posthog-node':
            print("Fetching events from the backend")
            if not isinstance(message, str):
                message = str(message)

            if len(message) > max_len:
                truncated_count += 1
                # Trim to 3997 chars and add "..."
                message = message[:max_len - len(suffix)] + suffix

            transformed.append({
                'event': e.get('event', ''),
                'caller': props.get('caller', ''),
                'endpoint': props.get('endpoint', ''),
                'level': props.get('level', ''),
                'method': props.get('method', ''),
                'timestamp_IST': timestamp_IST,
                'timestamp_UTC': timestamp_UTC,
                'message': message,
                'userId': props.get('userId', ''),
                'userRole': props.get('userRole', ''),
                'platform': props.get('$lib', '')
            })
        else:
            print("Fetching events from the frontend")
            if not isinstance(message, str):
                message = str(message)

            if len(message) > max_len:
                truncated_count += 1
                # Trim to 3997 chars and add "..."
                message = message[:max_len - len(suffix)] + suffix

            transformed.append({
                "id" :e.get("id",''),
                "user id" :e.get("distinct_id", ''),
                "session id" :props.get('$session_id', ''),
                "message" : message,
                "insert id" : props.get('$insert_id', ''),
                "library" : props.get('$lib', ''),
                "title" : props.get('title', ''),
                "session timeout (ms)" : props.get('$configured_session_timeout_ms', 0),
                "page hostname" : props.get('$host', ''),
                "session start time": safe_unix_to_ist(props.get('$sdk_debug_session_start', 0)),
                "user logged in" : props.get('$is_identified', False),
                "first page pathname" : props.get('$session_entry_pathname', ''),
                "pathname" : props.get('$pathname', ''),
                "dead click tracking enabled" : props.get('$dead_clicks_enabled_server_side', False),
                "current url" : props.get('$current_url', ''),
                "time": safe_unix_to_ist(props.get('$time', '')),
                "first page url" :props.get('$session_entry_url', ''),
                "user ip address" : props.get('$ip', ''),
                "event sent time" : props.get('$sent_at', ''),
                "user city" :props.get('geoip_city_name', ''),
                "user country" : props.get('geoip_country_name', ''),
                "user country code" :props.get('geoip_country_code', ''),
                "user state" : props.get('geoip_subdivision_1_name', ''),
                "event name" : e.get('event', ''),
                "timestamp_IST" : timestamp_IST,
                "timestamp_UTC" : timestamp_UTC,
                "event type" : props.get('$event_type', ''),
                "clicked_name" : props.get('$el_text', ''),
                "prev pageview pathname" :props.get('$prev_pageview_pathname', ''),
                "INP input delay (ms)" :attribute_INP.get('inputDelay', 0),
                "INP interaction time" : attribute_INP.get('interactionTime', 0),
                "INP next paint time" : attribute_INP.get('nextPaintTime', 0),
                "INP presentation delay (ms)" : attribute_INP.get('presentationDelay', 0),
                "INP processing duration (ms)" : attribute_INP.get('processingDuration', 0),
                "INP total delay" : web_vitals_INP.get('delta', 0),
                "INP performance_rating" :web_vitals_INP.get('rating', ''),
                "INP event timestamp": safe_unix_to_ist(web_vitals_CLS.get('timestamp', 0)),
                "referrer url" : props.get('$referrer', ''),
                "CLS shift time" : attribute_CLS.get('largestShiftTime', 0),
                "CLS shift value" : attribute_CLS.get('largestShiftValue', 0),
                "CLS total shift delta" : web_vitals_CLS.get('delta', 0),
                "CLS performance rating" : web_vitals_CLS.get('rating', ''),
                "CLS timestamp":  safe_unix_to_ist(web_vitals_CLS.get('timestamp', 0)),
                "level": props.get('level', ''),
                })


    if truncated_count > 0:
        print(f"Truncated {truncated_count} messages with '...' suffix for Power BI")

    return transformed



# --- Push to Power BI ---
def push_to_powerbi_in_batches(data, library):

    if library == 'posthog-js':
        POWER_BI_PUSH_URL = BACKEND_POWER_BI_PUSH_URL
    else:
        POWER_BI_PUSH_URL = FRONTEND_POWER_BI_PUSH_URL

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
    print(f"Total pushed to Power BI: {pushed_total} rows")
    return True

# --- MAIN ---
# --- MAIN ---
if __name__ == "__main__":
    print("-------------------------------------------------------------------------------")
    print(f"Script started at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("-------------------------------------------------------------------------------")

    current_ts = load_last_processed_time()
    frozen_now = datetime.now(timezone.utc)
    now_ts = int(frozen_now.timestamp() * 1000)

    # Set 2-day interval in milliseconds
    interval_ms = 2 * 24 * 60 * 60 * 1000

    while current_ts < now_ts:
        next_ts = min(current_ts + interval_ms, now_ts)

        from_utc = datetime.fromtimestamp(current_ts / 1000, tz=timezone.utc)
        to_utc = datetime.fromtimestamp(next_ts / 1000, tz=timezone.utc)
        from_ist = from_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
        to_ist = to_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))

        print(f"\nFetching range: {from_utc} → {to_utc} UTC [{from_ist.strftime('%Y-%m-%d %H:%M:%S')} → {to_ist.strftime('%Y-%m-%d %H:%M:%S')} IST]")

        events = fetch_posthog_events(current_ts, next_ts)
        print(f"Total events fetched: {len(events)}")

        if not events:
            print("No events found in this range.")
            current_ts = next_ts
            continue

        new_events = [e for e in events if current_ts < get_event_time(e) <= next_ts]
        print(f"New events to push: {len(new_events)}")

        if new_events:
            new_events.sort(key=get_event_time)
            library = new_events[0].get("properties", {}).get("$lib", "unknown")
            transformed = transform_events(new_events)
            success = push_to_powerbi_in_batches(transformed,library)
            if success:
                save_last_processed_time(next_ts)
                print(f"Checkpoint updated to: {next_ts}")
                current_ts = next_ts  # Proceed to next 2-day block
            else:
                print("Push to Power BI failed. Halting process to prevent data loss.")
                break  # Exit the loop immediately
        else:
            print("No valid events to push.")
            current_ts = next_ts  # Still move forward if no events found


        current_ts = next_ts
