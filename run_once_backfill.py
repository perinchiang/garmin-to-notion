import os
import time
from datetime import date, timedelta, datetime
from garminconnect import Garmin
from notion_client import Client

# ================= ⚙️ CONFIGURATION =================
# 1. How many activities to backfill?
TOTAL_ACTIVITIES_TO_SYNC = 1000

# 2. Batch size
BATCH_SIZE = 100

# 3. How many days to backfill?
DAYS_TO_BACKFILL = 180
# ====================================================

# --- Helper Functions ---
def format_duration(seconds):
    if not seconds: return "0h 0m"
    m = seconds // 60
    return f"{m // 60}h {m % 60}m"

def format_pace(speed):
    if not speed or speed == 0: return "0:00 min/km"
    pace = 1000 / 60 / speed
    minutes = int(pace)
    seconds = int((pace - minutes) * 60)
    return f"{minutes}:{seconds:02d} min/km"

def format_time_readable(timestamp):
    # Simple formatting for time range string
    if not timestamp: return "Unknown"
    return datetime.fromtimestamp(timestamp / 1000).strftime("%H:%M")

def format_time_iso(timestamp):
    # ISO format for Notion date property
    if not timestamp: return None
    return datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y-%m-%dT%H:%M:%S.000Z")

# --- CORE: Sync Functions (English) ---

def sync_activity(notion, db_id, activity):
    name = activity.get('activityName', 'Unnamed Activity')
    start_time = activity.get('startTimeGMT')
    activity_type = activity.get('activityType', {}).get('typeKey', 'Unknown').replace('_', ' ').title()
    
    # Check existence using English property names
    query = notion.databases.query(
        database_id=db_id,
        filter={
            "and": [
                {"property": "Date", "date": {"equals": start_time.split('T')[0]}},
                {"property": "Activity Name", "title": {"equals": name}}
            ]
        }
    )
    if query['results']:
        print(f"      [.] Skipped: {start_time[:10]} - {name}")
        return

    # Map to ENGLISH Notion Properties
    props = {
        "Date": {"date": {"start": start_time}},
        "Activity Type": {"select": {"name": activity_type}},
        "Activity Name": {"title": [{"text": {"content": name}}]},
        "Distance (km)": {"number": round(activity.get('distance', 0) / 1000, 2)},
        "Duration (min)": {"number": round(activity.get('duration', 0) / 60, 2)},
        "Calories": {"number": round(activity.get('calories', 0))},
        "Avg Pace": {"rich_text": [{"text": {"content": format_pace(activity.get('averageSpeed', 0))}}]},
        "Avg Power": {"number": round(activity.get('avgPower', 0), 1)},
        "Max Power": {"number": round(activity.get('maxPower', 0), 1)},
        # Training Effect logic simplified for backfill
        "Training Effect": {"select": {"name": activity.get('trainingEffectLabel', 'Unknown').replace('_', ' ').title()}},
        "Aerobic": {"number": round(activity.get('aerobicTrainingEffect', 0), 1)},
        "Anaerobic": {"number": round(activity.get('anaerobicTrainingEffect', 0), 1)},
        "PR": {"checkbox": activity.get('pr', False)},
        "Fav": {"checkbox": activity.get('favorite', False)}
    }
    
    try:
        notion.pages.create(parent={"database_id": db_id}, properties=props)
        print(f"      [+] Added: {start_time[:10]} - {name}")
    except Exception as e:
        print(f"      [!] Error adding activity: {e}")

def sync_daily_steps(notion, db_id, data):
    date_str = data.get('calendarDate')
    
    # English Property Check
    query = notion.databases.query(
        database_id=db_id,
        filter={"property": "Date", "date": {"equals": date_str}}
    )
    if query['results']:
        print(f"   [.] Steps exist: {date_str}")
        return

    # English Properties
    props = {
        "Activity Type": {"title": [{"text": {"content": "Walking"}}]},
        "Date": {"date": {"start": date_str}},
        "Total Steps": {"number": data.get('totalSteps')},
        "Step Goal": {"number": data.get('stepGoal')},
        "Total Distance (km)": {"number": round((data.get('totalDistance') or 0) / 1000, 2)}
    }
    notion.pages.create(parent={"database_id": db_id}, properties=props)
    print(f"   [+] Steps added: {data.get('totalSteps')}")

def sync_sleep(notion, db_id, data):
    daily = data.get('dailySleepDTO', {})
    date_str = daily.get('calendarDate')
    
    # ⭐⭐ FORMATTING CHANGE: 2026-01-07 -> 2026/01/07 ⭐⭐
    title_date = date_str.replace('-', '/')
    
    total_sleep = daily.get('sleepTimeSeconds') or 0
    
    if total_sleep == 0:
        print(f"   [x] No sleep data: {date_str}")
        return

    # English Property Check ("Long Date" is used in your sleep-data.py)
    query = notion.databases.query(
        database_id=db_id,
        filter={"property": "Long Date", "date": {"equals": date_str}}
    )
    if query['results']:
        print(f"   [.] Sleep exists: {date_str}")
        return

    # English Properties (Matching your sleep-data.py exactly)
    props = {
        "Date": {"title": [{"text": {"content": title_date}}]}, # Changed format here
        "Long Date": {"date": {"start": date_str}},
        "Total Sleep (h)": {"number": round(total_sleep / 3600, 1)},
        "Deep Sleep (h)": {"number": round(daily.get('deepSleepSeconds', 0) / 3600, 1)},
        "Light Sleep (h)": {"number": round(daily.get('lightSleepSeconds', 0) / 3600, 1)},
        "REM Sleep (h)": {"number": round(daily.get('remSleepSeconds', 0) / 3600, 1)},
        "Awake Time (h)": {"number": round(daily.get('awakeSleepSeconds', 0) / 3600, 1)},
        "Total Sleep": {"rich_text": [{"text": {"content": format_duration(total_sleep)}}]},
        "Light Sleep": {"rich_text": [{"text": {"content": format_duration(daily.get('lightSleepSeconds', 0))}}]},
        "Deep Sleep": {"rich_text": [{"text": {"content": format_duration(daily.get('deepSleepSeconds', 0))}}]},
        "REM Sleep": {"rich_text": [{"text": {"content": format_duration(daily.get('remSleepSeconds', 0))}}]},
        "Resting HR": {"number": data.get('restingHeartRate', 0)}
    }
    
    # Add timestamps if available
    start_ts = daily.get('sleepStartTimestampGMT')
    end_ts = daily.get('sleepEndTimestampGMT')
    if start_ts and end_ts:
        props["Times"] = {"rich_text": [{"text": {"content": f"{format_time_readable(start_ts)} → {format_time_readable(end_ts)}"}}]}
        props["Full Date/Time"] = {"date": {"start": format_time_iso(start_ts), "end": format_time_iso(end_ts)}}

    notion.pages.create(parent={"database_id": db_id}, properties=props, icon={"emoji": "😴"})
    print(f"   [+] Sleep added: {title_date}")

def main():
    print(f"🚀 Starting ENGLISH Backfill (Target: {TOTAL_ACTIVITIES_TO_SYNC} Activities / {DAYS_TO_BACKFILL} Days)")
    
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    
    # Standard IDs (No CN prefix)
    db_act = os.getenv("NOTION_DB_ID")
    db_step = os.getenv("NOTION_STEPS_DB_ID")
    db_sleep = os.getenv("NOTION_SLEEP_DB_ID")

    if not all([email, password, notion_token]):
        print("❌ Missing Secrets")
        return

    print("🔄 Logging in to Garmin (CN)...")
    try:
        garmin = Garmin(email, password, is_cn=True) # Important for your account
        garmin.login()
        print("✅ Logged in")
    except Exception as e:
        print(f"❌ Login failed: {e}")
        return

    notion = Client(auth=notion_token)

    # 1. Activities
    print(f"\n🏃 Fetching Activities...")
    processed_count = 0
    start_index = 0
    
    while processed_count < TOTAL_ACTIVITIES_TO_SYNC:
        remaining = TOTAL_ACTIVITIES_TO_SYNC - processed_count
        current_limit = min(BATCH_SIZE, remaining)
        print(f"\n📄 Reading {start_index} to {start_index + current_limit}...")
        try:
            activities = garmin.get_activities(start_index, current_limit)
        except Exception as e:
            print(f"⚠️ Error fetching: {e}")
            break   
        if not activities:
            print("✅ No more history.")
            break
        for act in activities:
            sync_activity(notion, db_act, act)
        processed_count += len(activities)
        start_index += len(activities)
        time.sleep(1)

    # 2. Steps & Sleep
    print(f"\n📅 Backfilling Steps & Sleep ({DAYS_TO_BACKFILL} days)...")
    today = date.today()
    start = today - timedelta(days=DAYS_TO_BACKFILL)
    current = start
    
    while current < today:
        day_str = current.isoformat()
        print(f"\n🔎 Checking: {day_str}")
        try:
            steps = garmin.get_daily_steps(day_str, day_str)
            if steps: sync_daily_steps(notion, db_step, steps[0])
        except Exception as e:
            print(f"⚠️ Steps error: {e}")

        try:
            sleep = garmin.get_sleep_data(day_str)
            sync_sleep(notion, db_sleep, sleep)
        except Exception as e:
            print(f"⚠️ Sleep error: {e}")

        time.sleep(1) 
        current += timedelta(days=1)

    print("\n✅ Backfill Complete!")

if __name__ == "__main__":
    main()
