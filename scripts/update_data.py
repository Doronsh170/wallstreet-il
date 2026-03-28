import json, os, requests
from datetime import datetime, timezone, timedelta

ISR_TZ = timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

def fetch_tweets():
    all_t = []
    for acc in ACCOUNTS:
        try:
            r = requests.get(
                f"https://api.twitterapi.io/twitter/user/last_tweets?userName={acc}",
                headers={"X-API-Key": TWITTER_API_KEY}
            )
            print(f"  @{acc}: status={r.status_code}")
            if r.ok:
                data = r.json()
                tweets = data.get("data", {}).get("tweets", [])
                print(f"    -> {len(tweets)} tweets")
                for t in tweets[:10]:
                    all_t.append(f"@{acc}: {t.get('text','')}")
            else:
                print(f"    -> Error: {r.text[:200]}")
        except Exception as e:
            print(f"  Error fetching {acc}: {e}")
    return "\n\n".join(all_t)

def call_gemini(tweets, review_type, date_str, day_name):
    prompt = f"""אתה אנליסט שוק ההון האמריקאי שכותב סקירות מקצועיות בעברית.
סגנון: מקצועי, תמציתי, ברור. כמו ניוזלטר של בית השקעות מוביל.

כללים:
- כתוב בעברית בלבד
- אל תתן המלצות השקעה ספציפיות (קנה/מכור)
- השתמש במונחים מקצועיים עם הסבר קצר בסוגריים כשצריך
- ציין מספרים ואחוזים מדויקים כשזמינים
- כל סעיף: כותרת + פסקה של 2-4 משפטים

סוג הסקירה: {review_type}
תאריך: {date_str}
יום בשבוע: {day_name}

פוסטים ממקורות (Twitter/X):
{tweets}

צור JSON בפורמט הבא (בלי backticks, בלי הסברים, רק JSON טהור):
{{"title":"כותרת הסקירה","date":"{date_str}","sections":[{{"heading":"כותרת סעיף","content":"תוכן הסעיף"}}]}}

כלול 3 סעיפים רלוונטיים לסוג הסקירה:
- daily_prep: פיוצ'רס ופרה-מרקט, אירועים מרכזיים היום, מה לעקוב אחריו
- daily_summary: ביצועי מדדים, מניות בולטות, סנטימנט ומבט קדימה
- weekly_prep: מבט כללי, אירועים מרכזיים בשבוע, רמות טכניות
- weekly_summary: ביצועים שבועיים, סקטורים בולטים, מבט לשבוע הבא

ענה אך ורק ב-JSON טהור."""

    r = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key={GEMINI_API_KEY}",
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 8192,
                "responseMimeType": "application/json"
            }
        }
    )

    resp_data = r.json()
    print(f"  Gemini status: {r.status_code}")
    
    # Gemini 2.5-pro may have multiple parts (thinking + response)
    candidate = resp_data.get("candidates", [{}])[0]
    content = candidate.get("content", {})
    parts = content.get("parts", [])
    
    # Find the text part (skip thinking parts)
    text = ""
    for part in parts:
        if "text" in part:
            text = part["text"]
    
    if not text:
        print(f"  Gemini raw response: {str(resp_data)[:500]}")
        raise Exception("Gemini returned no text")
    
    # Clean potential markdown
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw text (last 200 chars): ...{text[-200:]}")
        raise

def main():
    now = datetime.now(ISR_TZ)
    date_str = now.strftime("%Y-%m-%d")

    py_to_heb = {0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}
    day_name = py_to_heb[now.weekday()]

    print(f"Running {REVIEW_TYPE} for {date_str} ({day_name})")

    tweets = fetch_tweets()
    if not tweets:
        print("No tweets fetched, skipping.")
        return

    print(f"Fetched {len(tweets.split(chr(10)+chr(10)))} tweet blocks")

    result = call_gemini(tweets, REVIEW_TYPE, date_str, day_name)

    # Load existing data
    with open("data.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    data["lastUpdated"] = now.isoformat()

    key_map = {
        "daily_prep": "dailyPrep",
        "daily_summary": "dailySummary",
        "weekly_prep": "weeklyPrep",
        "weekly_summary": "weeklySummary"
    }

    if REVIEW_TYPE == "events":
        if "items" in result:
            data["events"]["items"] = result["items"]
            data["events"]["lastUpdated"] = now.isoformat()
    elif REVIEW_TYPE in key_map:
        data[key_map[REVIEW_TYPE]] = result

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Updated {REVIEW_TYPE} successfully.")

if __name__ == "__main__":
    main()
