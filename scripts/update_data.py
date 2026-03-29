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

# ══════════════════════════════════════════════
# PROMPTS — each review type has a unique prompt
# ══════════════════════════════════════════════

def get_prompt(tweets, review_type, date_str, day_name):

    base_rules = """כללים חשובים:
- כתוב בעברית בלבד. מונחים מקצועיים באנגלית בסוגריים.
- ציין מספרים, אחוזים ושמות מניות (טיקרים) ספציפיים מהפוסטים.
- אל תתן המלצות קנה/מכור.
- כל סעיף: כותרת + פסקה של 3-5 משפטים עם תוכן ממשי.
- אל תחזור על אותם משפטים בסעיפים שונים.
- ענה אך ורק ב-JSON טהור, בלי backticks."""

    tweets_block = f"""פוסטים ממקורות (Twitter/X) מתאריך {date_str}:
{tweets}"""

    if review_type == "daily_prep":
        return f"""אתה אנליסט בכיר בבית השקעות שמכין תדריך בוקר (Morning Briefing) לצוות המסחר.
המטרה: לתת תמונת מצב חדה של מה קורה לפני פתיחת המסחר בוול סטריט היום.

{base_rules}

{tweets_block}

צור JSON בפורמט:
{{"title":"תדריך בוקר – {day_name} {date_str}","date":"{date_str}","sections":[{{"heading":"כותרת","content":"תוכן"}}]}}

כלול בדיוק 4 סעיפים:
1. "סנטימנט השוק בפתיחת השבוע" — מה מצב הפיוצ'רס? מה הכיוון הכללי? ציין מספרים (S&P, Nasdaq, Dow futures). האם יש פחד או אופטימיות?
2. "אירועים ונתונים שיזיזו את השוק היום" — נתונים כלכליים ספציפיים (עם שעות אם זמין), דוחות רבעוניים, נאומי פד, הצבעות. מה המשמעות של כל אחד?
3. "מניות וסקטורים תחת זרקור" — מניות ספציפיות שזזו בפרה-מרקט או שיש להן קטליסט היום. ציין טיקרים ואחוזים.
4. "מה האנליסט שלנו עוקב אחריו" — הנקודה הכי חשובה היום, הסיכון המרכזי, ומה ישנה כיוון. דעה אנליטית חדה."""

    elif review_type == "daily_summary":
        return f"""אתה עורך דסק שוק ההון שכותב סיכום יום מסחר (Market Wrap) למשקיעים.
המטרה: לספר מה קרה היום, למה, ומה זה אומר למחר.

{base_rules}

{tweets_block}

צור JSON בפורמט:
{{"title":"סיכום יום מסחר – {day_name} {date_str}","date":"{date_str}","sections":[{{"heading":"כותרת","content":"תוכן"}}]}}

כלול בדיוק 4 סעיפים:
1. "כך נסגר היום" — ביצועי המדדים המרכזיים: S&P 500, Nasdaq, Dow, Russell 2000. אחוזי שינוי, נקודות. האם זה יום עליות, ירידות, או מעורב? נפח מסחר.
2. "המניות שעשו את היום" — 4-6 מניות ספציפיות שהובילו או פיגרו. לכל מניה: טיקר, אחוז שינוי, והסיבה (דוח, שדרוג, חדשות, סנטימנט). 
3. "הסיפור מאחורי המספרים" — מה באמת הניע את השוק היום? גורם מאקרו, גיאופוליטי, טכני? האם הייתה רוטציה בין סקטורים? מה אומר ה-VIX?
4. "מבט קדימה למחר" — מה צפוי מחר? נתונים, דוחות, אירועים. האם המומנטום צפוי להמשיך? סיכונים."""

    elif review_type == "weekly_prep":
        return f"""אתה ראש מחלקת מחקר שמכין תחזית שבועית (Weekly Outlook) לוועדת ההשקעות.
המטרה: לתת מפה מלאה של השבוע הקרוב — אירועים, סיכונים, הזדמנויות.

{base_rules}

{tweets_block}

צור JSON בפורמט:
{{"title":"תחזית שבועית – שבוע {date_str}","date":"{date_str}","sections":[{{"heading":"כותרת","content":"תוכן"}}]}}

כלול בדיוק 4 סעיפים:
1. "הנושא המרכזי של השבוע" — מה הנרטיב הדומיננטי? (ריבית, גיאופוליטיקה, עונת דוחות, טכנולוגיה). למה דווקא זה חשוב השבוע? הסבר בשני משפטים.
2. "יומן אירועים — יום אחר יום" — לכל יום (שני עד שישי): אירוע מרכזי אחד לפחות. נתונים כלכליים, דוחות רבעוניים, נאומי פד, פקיעות. כתוב כרשימה עם תאריך ושעה אם זמין.
3. "סקטורים ומניות לרדאר" — אילו סקטורים רגישים במיוחד השבוע? מניות ספציפיות שצפויות לתנודתיות (דוחות, אנליסטים, מוצר חדש). ציין טיקרים.
4. "רמות טכניות ותמונת סיכון" — תמיכה/התנגדות ל-S&P 500 ו-Nasdaq. רמת VIX. האם השוק במגמה ברורה או בטווח? מה הסיכון הגדול ביותר השבוע?"""

    elif review_type == "weekly_summary":
        return f"""אתה אנליסט בכיר שכותב סיכום שבועי (Weekly Review) לניוזלטר משקיעים.
המטרה: להסתכל אחורה על השבוע, לזהות מגמות, ולהציג תמונה גדולה.

{base_rules}

{tweets_block}

צור JSON בפורמט:
{{"title":"סיכום שבועי – שבוע {date_str}","date":"{date_str}","sections":[{{"heading":"כותרת","content":"תוכן"}}]}}

כלול בדיוק 4 סעיפים:
1. "השבוע במספרים" — ביצועי מדדים שבועיים: S&P 500, Nasdaq, Dow, Russell 2000 (אחוזי שינוי שבועיים). VIX. תשואת 10Y. דולר. נפט. זהב. ציין מספרים מדויקים.
2. "הסיפורים שעשו את השבוע" — 3-4 אירועים/חדשות שהשפיעו ביותר על השוק השבוע. לכל אחד: מה קרה, מה הייתה ההשפעה, ומה זה אומר קדימה.
3. "מפת הסקטורים" — אילו סקטורים הובילו ואילו פיגרו? מדוע? האם יש רוטציה מתמשכת? ציין 3 סקטורים מובילים ו-3 מפגרים עם אחוזים.
4. "מה חשוב לשבוע הבא" — האירועים המרכזיים שצפויים. האם השבוע הנוכחי שינה את התמונה? מה הסיכון ומה ההזדמנות?"""

    elif review_type == "events":
        return f"""אתה עורך לוח אירועים כלכליים (Economic Calendar) עבור משקיעים בשוק האמריקאי.
המטרה: ליצור רשימת אירועים כלכליים מהותיים לימים הקרובים (5-7 ימים קדימה).

{base_rules}

{tweets_block}

צור JSON בפורמט הבא (שים לב — פורמט שונה מסקירות!):
{{"items":[{{"time":"2026-03-30T15:30:00+03:00","title":"שם האירוע בעברית","impact":"high","description":"הסבר קצר של 1-2 משפטים — מה האירוע ולמה הוא חשוב למשקיעים"}}]}}

כללים לאירועים:
- כלול 6-10 אירועים לשבוע הקרוב.
- impact יכול להיות: "high" (משפיע על כל השוק), "medium" (משפיע על סקטור), "low" (רקע).
- השתמש בשעון ישראל (UTC+3) בשדה time.
- כלול: נתונים מאקרו (NFP, CPI, PMI, GDP), החלטות ריבית, דוחות רבעוניים חשובים, נאומי פד, פקיעות אופציות.
- מיין לפי תאריך (הקרוב ביותר קודם).
- אם אין מידע מדויק על שעה, השתמש ב-15:30 (שעת פתיחת שוק) כברירת מחדל."""

    return ""

def call_gemini(prompt):
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
        print(f"  Raw text (last 300 chars): ...{text[-300:]}")
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

    prompt = get_prompt(tweets, REVIEW_TYPE, date_str, day_name)
    if not prompt:
        print(f"Unknown review type: {REVIEW_TYPE}")
        return

    result = call_gemini(prompt)

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
        # Events format: result should have "items" key
        items = result.get("items", [])
        if items:
            data["events"]["items"] = items
            data["events"]["lastUpdated"] = now.isoformat()
            print(f"  Stored {len(items)} events")
        else:
            # Fallback: maybe Gemini returned sections format, convert
            print(f"  Warning: no 'items' key found. Keys: {list(result.keys())}")
            print(f"  Storing raw result as events data")
    elif REVIEW_TYPE in key_map:
        data[key_map[REVIEW_TYPE]] = result

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Updated {REVIEW_TYPE} successfully.")

if __name__ == "__main__":
    main()
