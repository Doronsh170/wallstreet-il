import json, os, requests
from datetime import datetime, timezone, timedelta

ISR_TZ = timezone(timedelta(hours=3))
CLAUDE_API_KEY = os.environ["CLAUDE_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

def fetch_tweets():
    all_t = []
    for acc in ACCOUNTS:
        try:
            r = requests.get(
                f"https://api.twitterapi.io/twitter/user/last_tweets?userName={acc}&count=10",
                headers={"X-API-Key": TWITTER_API_KEY}
            )
            if r.ok:
                for t in r.json().get("tweets", []):
                    all_t.append(f"@{acc}: {t.get('text','')}")
        except Exception as e:
            print(f"Error fetching {acc}: {e}")
    return "\n\n".join(all_t)

def call_claude(tweets, review_type, date_str, day_name):
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

{"אם סוג הסקירה הוא events, צור JSON בפורמט: " + chr(123) + '"items":[' + chr(123) + '"time":"ISO8601","title":"שם בעברית","impact":"high/medium/low","description":"הסבר"' + chr(125) + ']' + chr(125) if review_type == "events" else ""}

צור JSON (בלי backticks, בלי הסברים) בפורמט:
{{"title":"כותרת הסקירה","date":"{date_str}","sections":[{{"heading":"כותרת סעיף","content":"תוכן הסעיף"}}]}}

כלול 3 סעיפים רלוונטיים."""

    r = requests.post("https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}]
        })
    
    resp_data = r.json()
    text = resp_data["content"][0]["text"]
    # Clean potential markdown
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    return json.loads(text)

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
    
    result = call_claude(tweets, REVIEW_TYPE, date_str, day_name)

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
