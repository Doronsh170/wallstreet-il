"""
Wall Street IL — clean-slate review generator.
Single source: 2 X accounts. No Finnhub, no fact-checker, no validation layers.
Five review types: daily_prep, daily_summary, weekly_prep, weekly_summary, live_news.
"""

import json
import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

ISR_TZ = timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy", "StockMKTNewz"]

PY_TO_HEB = {0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}

EXPECTED_FIRST_HEADING = {
    "daily_prep":     "נקודות מרכזיות",
    "daily_summary":  "סיכום המסחר",
    "weekly_prep":    "נקודות מרכזיות לשבוע הקרוב",
    "weekly_summary": "סיכום השבוע",
    "live_news":      "חדשות אחרונות",
}

# Translated prompts (user-authored in Hebrew, output is in Hebrew)
PROMPTS = {
    "daily_prep":     "Summarize in bullet points what investors on Wall Street must know before the market opens.",
    "daily_summary":  "Summarize in bullet points all the important events that occurred during the trading day that just ended.",
    "weekly_prep":    "Summarize in bullet points what investors on Wall Street must know in preparation for the opening of the trading week.",
    "weekly_summary": "Summarize in bullet points the past week on Wall Street for investors. Focus on all the important events that occurred and had an impact.",
    "live_news":      "Summarize in bullet points the important events happening right now on Wall Street.",
}

# ════════════════════════════════════════════════════════════════
# DATE / TITLE HELPERS
# ════════════════════════════════════════════════════════════════

def load_holidays():
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("marketStatus", {}).get("usHolidays2026", [])
    except Exception:
        return []

def is_trading_day(dt, holidays):
    if dt.weekday() >= 5:
        return False
    return dt.strftime("%Y-%m-%d") not in holidays

def get_next_trading_day(now, holidays):
    d = now + timedelta(days=1)
    for _ in range(10):
        if is_trading_day(d, holidays):
            return d
        d += timedelta(days=1)
    return now + timedelta(days=1)

def get_last_trading_day(now, holidays):
    if is_trading_day(now, holidays) and now.hour >= 23:
        return now
    d = now - timedelta(days=1)
    for _ in range(10):
        if is_trading_day(d, holidays):
            return d
        d -= timedelta(days=1)
    return now - timedelta(days=1)

def get_prev_week_range_str(now):
    weekday = now.weekday()
    if weekday >= 5:
        monday = now - timedelta(days=weekday)
    else:
        monday = now - timedelta(days=weekday + 7)
    friday = monday + timedelta(days=4)
    return f"{monday.strftime('%d/%m')}–{friday.strftime('%d/%m/%Y')}"

def build_expected_title(review_type, day_name, date_str, week_range=None, now_time=None):
    if review_type == "daily_prep":
        return f"נקודות חשובות לקראת פתיחת המסחר בוול סטריט 🇺🇸 – יום {day_name} {date_str}"
    if review_type == "daily_summary":
        return f"סיכום יום המסחר בוול סטריט 🇺🇸 – יום {day_name} {date_str}"
    if review_type == "weekly_prep":
        return f"הכנה לשבוע מסחר בוול סטריט 🇺🇸 – {week_range}"
    if review_type == "weekly_summary":
        return f"סיכום שבוע המסחר בוול סטריט 🇺🇸 – {week_range}"
    if review_type == "live_news":
        return f"מה קורה עכשיו בוול סטריט 🇺🇸 – יום {day_name}, {date_str} | {now_time}"
    return ""

# ════════════════════════════════════════════════════════════════
# TWEETS
# ════════════════════════════════════════════════════════════════

def fetch_tweets():
    all_t = []
    for acc in ACCOUNTS:
        try:
            r = requests.get(
                f"https://api.twitterapi.io/twitter/user/last_tweets?userName={acc}",
                headers={"X-API-Key": TWITTER_API_KEY},
                timeout=30,
            )
            print(f"  @{acc}: status={r.status_code}")
            if r.ok:
                data = r.json()
                tweets = data.get("data", {}).get("tweets", [])
                print(f"    -> {len(tweets)} tweets")
                for t in tweets[:15]:
                    text = t.get('text', '')
                    ts = t.get('createdAt') or t.get('created_at') or ''
                    if ts:
                        all_t.append(f"@{acc} [{ts}]: {text}")
                    else:
                        all_t.append(f"@{acc}: {text}")
            else:
                print(f"    -> Error: {r.text[:200]}")
        except Exception as e:
            print(f"  Error fetching {acc}: {e}")
    return "\n\n".join(all_t)

# ════════════════════════════════════════════════════════════════
# GEMINI
# ════════════════════════════════════════════════════════════════

def call_gemini(prompt, max_retries=5):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key={GEMINI_API_KEY}"

    for attempt in range(max_retries):
        try:
            r = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.2, "maxOutputTokens": 8192},
                },
                timeout=180,
            )
            if r.status_code != 200:
                print(f"  Gemini status {r.status_code}, attempt {attempt+1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(30)
                    continue
                raise Exception(f"Gemini returned {r.status_code}: {r.text[:300]}")

            resp_data = r.json()
            candidate = resp_data.get("candidates", [{}])[0]
            parts = candidate.get("content", {}).get("parts", [])
            text = ""
            for part in parts:
                if "text" in part:
                    text = part["text"]

            if not text:
                if attempt < max_retries - 1:
                    print("  Gemini returned no text, retrying in 30s...")
                    time.sleep(30)
                    continue
                raise Exception("Gemini returned no text")

            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
            text = re.sub(r'\s*\[\d+(?:,\s*\d+)*\]', '', text)

            start = text.find('{')
            if start >= 0:
                depth = 0
                end = start
                for i in range(start, len(text)):
                    if text[i] == '{':
                        depth += 1
                    elif text[i] == '}':
                        depth -= 1
                        if depth == 0:
                            end = i + 1
                            break
                text = text[start:end]

            return json.loads(text)

        except json.JSONDecodeError as e:
            print(f"  JSON parse error: {e}, attempt {attempt+1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(30)
                continue
            raise
        except Exception as e:
            print(f"  Error: {e}, attempt {attempt+1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(30)
                continue
            raise

    raise Exception("call_gemini: exhausted all retries")

# ════════════════════════════════════════════════════════════════
# MINIMAL STRUCTURE ENFORCEMENT — JSON shape + bullet format only
# ════════════════════════════════════════════════════════════════

_BULLET_CHARS = r'[•■●▪▫◦‣⁃–—]'

def normalize_bullets(text):
    """Ensure every non-empty line begins with '* ' so the HTML renderer picks it up."""
    if not isinstance(text, str) or not text.strip():
        return text
    lines = text.split("\n")
    result = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        converted = re.sub(rf'^{_BULLET_CHARS}\s+', '* ', stripped)
        converted = re.sub(r'^-\s+', '* ', converted)
        if not converted.startswith('* '):
            converted = '* ' + converted
        result.append(converted)
    return "\n".join(result)

def enforce_structure(result, review_type, expected_title):
    """Force shape: {title, sections: [{heading, content}]}.
    Collapses any extra sections into the first. Forces title and heading from constants."""
    heading = EXPECTED_FIRST_HEADING.get(review_type, "נקודות מרכזיות")

    if not isinstance(result, dict):
        return {"title": expected_title, "sections": [{"heading": heading, "content": ""}]}

    result["title"] = expected_title
    sections = result.get("sections", [])
    if not isinstance(sections, list) or not sections:
        result["sections"] = [{"heading": heading, "content": ""}]
        return result

    first_content = sections[0].get("content", "")
    if isinstance(first_content, list):
        first_content = "\n".join(str(x) for x in first_content)

    # Merge any extra sections into the first
    for s in sections[1:]:
        c = s.get("content", "")
        if isinstance(c, list):
            c = "\n".join(str(x) for x in c)
        if c.strip():
            first_content += "\n" + c

    result["sections"] = [{
        "heading": heading,
        "content": normalize_bullets(first_content),
    }]
    return result

# ════════════════════════════════════════════════════════════════
# PROMPT
# ════════════════════════════════════════════════════════════════

def build_prompt(review_type, tweets):
    instruction = PROMPTS[review_type]
    return f"""{instruction}

Output in Hebrew.

Source — recent posts from X (Twitter):
{tweets}

Output ONLY a valid JSON object with this exact structure:
{{"title": "...", "sections": [{{"heading": "...", "content": "* bullet 1\\n* bullet 2\\n* bullet 3"}}]}}

Rules:
- Exactly one section.
- "content" is bullet points, each starting with "* " (asterisk + space) on its own line.
- No backticks, no explanations — JSON only."""

# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def main():
    if REVIEW_TYPE not in PROMPTS:
        print(f"Review type '{REVIEW_TYPE}' not handled in clean-slate version. Skipping.")
        return

    now = datetime.now(ISR_TZ)
    date_str = now.strftime("%Y-%m-%d")
    day_name = PY_TO_HEB[now.weekday()]

    holidays = load_holidays()

    title_date_str = date_str
    title_day_name = day_name
    week_range = None

    if REVIEW_TYPE == "daily_prep":
        target = now if is_trading_day(now, holidays) else get_next_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]
    elif REVIEW_TYPE == "daily_summary":
        target = get_last_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]
    elif REVIEW_TYPE == "weekly_summary":
        week_range = get_prev_week_range_str(now)
    elif REVIEW_TYPE == "weekly_prep":
        weekday = now.weekday()
        if weekday <= 4:
            monday = now - timedelta(days=weekday)
        else:
            monday = now + timedelta(days=(7 - weekday))
        friday = monday + timedelta(days=4)
        week_range = f"{monday.strftime('%d/%m')}–{friday.strftime('%d/%m/%Y')}"

    now_time_str = now.strftime('%H:%M')
    expected_title = build_expected_title(REVIEW_TYPE, title_day_name, title_date_str, week_range, now_time_str)

    print(f"Running {REVIEW_TYPE} for {date_str} ({day_name})")
    print(f"  Title: {expected_title}")

    tweets = fetch_tweets()
    if not tweets:
        print("No tweets fetched, skipping.")
        return
    print(f"Fetched {len(tweets.split(chr(10)+chr(10)))} tweet blocks")

    prompt = build_prompt(REVIEW_TYPE, tweets)
    result = call_gemini(prompt)
    result = enforce_structure(result, REVIEW_TYPE, expected_title)

    with open("data.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    data["lastUpdated"] = now.isoformat()

    key_map = {
        "daily_prep":     "dailyPrep",
        "daily_summary":  "dailySummary",
        "weekly_prep":    "weeklyPrep",
        "weekly_summary": "weeklySummary",
        "live_news":      "liveNews",
    }
    data[key_map[REVIEW_TYPE]] = result

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Updated {REVIEW_TYPE} successfully.")

if __name__ == "__main__":
    main()
