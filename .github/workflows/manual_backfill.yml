import os
import time
from datetime import date, timedelta
from garminconnect import Garmin
from notion_client import Client

# ================= ⚙️ 配置区域 =================

# 1. 运动记录：设为 1200 条 (多设一点没关系，以此确保覆盖全年)
TOTAL_ACTIVITIES_TO_SYNC = 1200 

# 2. 每次请求数量：保持 50 或 100 (不要贪多，防止 Garmin 报错)
BATCH_SIZE = 100

# 3. 日常数据：回填过去 366 天 (覆盖整整一年)
DAYS_TO_BACKFILL = 366 

# ==============================================

# --- 1. 静态翻译字典 ---
TYPE_TRANSLATION = {
    "Running": "跑步", "Cycling": "骑行", "Walking": "徒步", "Swimming": "游泳",
    "Strength": "力量训练", "Cardio": "有氧运动", "Yoga": "瑜伽", "Hiking": "登山",
    "Indoor Cycling": "室内骑行", "Treadmill Running": "跑步机", "Elliptical": "椭圆机",
    "Floor Climbing": "爬楼梯", "Unknown": "未知"
}

EFFECT_TRANSLATION = {
    "Sprint": "冲刺", "Anaerobic Capacity": "无氧容量", "VO2 Max": "最大摄氧量",
    "Threshold": "乳酸阈值", "Tempo": "节奏", "Base": "基础", "Recovery": "恢复",
    "Low Aerobic": "低强度有氧", "High Aerobic": "高强度有氧", "Anaerobic": "无氧", "Aerobic": "有氧"
}

def translate_type(english_type):
    return TYPE_TRANSLATION.get(english_type, english_type)

def translate_effect(label):
    if not label: return "Unknown"
    formatted = label.replace('_', ' ').title()
    if formatted.lower() == "vo2 max": formatted = "VO2 Max"
    return EFFECT_TRANSLATION.get(formatted, formatted)

# --- 2. 辅助工具函数 ---
def format_duration(seconds):
    if not seconds: return "0h 0m"
    m = seconds // 60
    return f"{m // 60}h {m % 60}m"

def format_pace(speed):
    if not speed or speed == 0: return "0:00"
    pace = 1000 / 60 / speed
    minutes = int(pace)
    seconds = int((pace - minutes) * 60)
    return f"{minutes}:{seconds:02d}"

# --- 3. 核心功能：写入 Notion ---

def sync_activity(notion, db_id, activity):
    name = activity.get('activityName', 'Unnamed')
    start_time = activity.get('startTimeGMT')
    a_type = activity.get('activityType', {}).get('typeKey', 'Unknown').replace('_', ' ').title()
    cn_type = translate_type(a_type)
    
    # 检查是否已存在
    query = notion.databases.query(
        database_id=db_id,
        filter={
            "and": [
                {"property": "日期", "date": {"equals": start_time.split('T')[0]}},
                {"property": "运动名称", "title": {"equals": name}}
            ]
        }
    )
    if query['results']:
        print(f"      [.] 已存在: {start_time[:10]} - {name}")
        return

    # 写入
    props = {
        "日期": {"date": {"start": start_time}},
        "运动类型": {"select": {"name": cn_type}},
        "运动名称": {"title": [{"text": {"content": name}}]},
        "距离 (km)": {"number": round(activity.get('distance', 0) / 1000, 2)},
        "时长 (min)": {"number": round(activity.get('duration', 0) / 60, 2)},
        "卡路里": {"number": round(activity.get('calories', 0))},
        "平均配速": {"rich_text": [{"text": {"content": format_pace(activity.get('averageSpeed', 0))}}]},
        "平均功率": {"number": round(activity.get('avgPower', 0), 1)},
        "训练效果": {"select": {"name": translate_effect(activity.get('trainingEffectLabel'))}},
        "PR": {"checkbox": activity.get('pr', False)},
    }
    notion.pages.create(parent={"database_id": db_id}, properties=props)
    print(f"      [+] 写入成功: {start_time[:10]} - {name}")

def sync_daily_steps(notion, db_id, data):
    date_str = data.get('calendarDate')
    query = notion.databases.query(
        database_id=db_id,
        filter={"property": "日期", "date": {"equals": date_str}}
    )
    if query['results']:
        print(f"   [.] 步数已存在: {date_str}")
        return

    props = {
        "运动类型": {"title": [{"text": {"content": "Walking"}}]},
        "日期": {"date": {"start": date_str}},
        "总步数": {"number": data.get('totalSteps')},
        "步数目标": {"number": data.get('stepGoal')},
        "总距离 (km)": {"number": round((data.get('totalDistance') or 0) / 1000, 2)}
    }
    notion.pages.create(parent={"database_id": db_id}, properties=props)
    print(f"   [+] 步数补全: {data.get('totalSteps')}")

def sync_sleep(notion, db_id, data):
    daily = data.get('dailySleepDTO', {})
    date_str = daily.get('calendarDate')
    total_sleep = daily.get('sleepTimeSeconds', 0)
    
    if total_sleep == 0:
        print(f"   [x] 无睡眠数据: {date_str}")
        return

    query = notion.databases.query(
        database_id=db_id,
        filter={"property": "长日期", "date": {"equals": date_str}}
    )
    if query['results']:
        print(f"   [.] 睡眠已存在: {date_str}")
        return

    goal_met = total_sleep >= (8 * 3600)
    props = {
        "日期": {"title": [{"text": {"content": date_str}}]},
        "长日期": {"date": {"start": date_str}},
        "总睡眠 (h)": {"number": round(total_sleep / 3600, 1)},
        "深睡 (h)": {"number": round(daily.get('deepSleepSeconds', 0) / 3600, 1)},
        "浅睡 (h)": {"number": round(daily.get('lightSleepSeconds', 0) / 3600, 1)},
        "快速眼动 (h)": {"number": round(daily.get('remSleepSeconds', 0) / 3600, 1)},
        "总睡眠时长": {"rich_text": [{"text": {"content": format_duration(total_sleep)}}]},
        "睡眠目标": {"checkbox": goal_met}
    }
    notion.pages.create(parent={"database_id": db_id}, properties=props, icon={"emoji": "😴"})
    print(f"   [+] 睡眠补全: {round(total_sleep/3600, 1)}h")


# --- 4. 主程序 ---
def main():
    print(f"🚀 启动超级回填脚本 (目标: {TOTAL_ACTIVITIES_TO_SYNC} 条运动 / {DAYS_TO_BACKFILL} 天生活数据)")
    
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    
    db_act = os.getenv("NOTION_CN_DB_ID") or os.getenv("NOTION_DB_ID")
    db_step = os.getenv("NOTION_CN_STEPS_DB_ID") or os.getenv("NOTION_STEPS_DB_ID")
    db_sleep = os.getenv("NOTION_CN_SLEEP_DB_ID") or os.getenv("NOTION_SLEEP_DB_ID")

    if not all([email, password, notion_token]):
        print("❌ 环境变量缺失")
        return

    # 登录 Garmin
    print("🔄 正在登录 Garmin CN...")
    try:
        garmin = Garmin(email, password, is_cn=True)
        garmin.login()
        print("✅ 登录成功")
    except Exception as e:
        print(f"❌ 登录失败: {e}")
        return

    notion = Client(auth=notion_token)

    # ================= 分页拉取运动记录 (核心修改) =================
    print(f"\n🏃 正在拉取运动记录 (每页 {BATCH_SIZE} 条)...")
    
    processed_count = 0
    start_index = 0
    
    while processed_count < TOTAL_ACTIVITIES_TO_SYNC:
        # 计算这波拉多少条
        remaining = TOTAL_ACTIVITIES_TO_SYNC - processed_count
        current_limit = min(BATCH_SIZE, remaining)
        
        print(f"\n📄 正在读取第 {start_index} 到 {start_index + current_limit} 条...")
        
        try:
            activities = garmin.get_activities(start_index, current_limit)
        except Exception as e:
            print(f"⚠️ 读取 Garmin 接口失败: {e}")
            break
            
        if not activities:
            print("✅ 已没有更多历史记录。")
            break
            
        # 同步这波数据
        for act in activities:
            sync_activity(notion, db_act, act)
        
        count = len(activities)
        processed_count += count
        start_index += count
        
        print(f"   -> 本页处理完成，休息 1 秒...")
        time.sleep(1) # 休息一下防封号

    # ================= 补全步数和睡眠 =================
    print(f"\n📅 正在回填过去 {DAYS_TO_BACKFILL} 天的步数和睡眠...")
    today = date.today()
    start = today - timedelta(days=DAYS_TO_BACKFILL)
    current = start
    
    while current < today:
        day_str = current.isoformat()
        print(f"\n🔎 检查: {day_str}")
        
        try:
            steps = garmin.get_daily_steps(day_str, day_str)
            if steps: sync_daily_steps(notion, db_step, steps[0])
        except Exception as e:
            print(f"⚠️ 步数错: {e}")

        try:
            sleep = garmin.get_sleep_data(day_str)
            sync_sleep(notion, db_sleep, sleep)
        except Exception as e:
            print(f"⚠️ 睡眠错: {e}")

        time.sleep(1) 
        current += timedelta(days=1)

    print("\n✅ 所有任务圆满完成！")

if __name__ == "__main__":
    main()
