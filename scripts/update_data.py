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

# ══════════════════════════════════════════════════════════════
# PROMPTS — English prompts, Hebrew output
# ══════════════════════════════════════════════════════════════

SHARED_RULES = """Rules:
- Write ONLY in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and well-known financial terms in parentheses on first use only.
- Be specific: every claim must include a number, percentage, or ticker. Never write vague statements like "the market had an interesting week".
- Do NOT repeat the same information across sections. Each section must contain NEW content.
- No buy/sell recommendations.
- Start each section directly with the key fact. No generic opening sentences.
- Output pure JSON only, no backticks, no explanations."""

def get_prompt(tweets, review_type, date_str, day_name):

    tweets_block = f"Source tweets/posts from X (Twitter) — date: {date_str}:\n{tweets}"

    if review_type == "daily_prep":
        return f"""You are a senior Wall Street market analyst writing a pre-market briefing in Hebrew.

Your task: Summarize what investors need to know before the US market opens today, based on the tweets/posts below. This is a sharp 3-minute morning read for professional investors.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"תדריך בוקר – {day_name} {date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 3 sections:
1. heading: "מה קרה בלילה" — Key overnight developments: futures levels (S&P, Nasdaq, Dow with exact numbers), Asia/Europe session moves, major news that broke after US close. Numbers only, no fluff.
2. heading: "מה יזיז את השוק היום" — Today's catalysts: economic data releases (with Israel times), earnings reports due, Fed speakers, geopolitical developments. For each — one sentence on why it matters.
3. heading: "מניות תחת זרקור" — 3-5 specific stocks with a catalyst today: pre-market moves (%), upgrades/downgrades, insider activity, earnings surprise. Format each as: ticker + number + reason."""

    elif review_type == "daily_summary":
        return f"""You are a senior Wall Street market analyst writing an end-of-day market wrap in Hebrew.

Your task: Summarize the main events of the last trading day on Wall Street, based on the tweets/posts below. Tell investors what happened, why, and what it means for tomorrow.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"סיכום יום מסחר – {day_name} {date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 3 sections:
1. heading: "כך נסגר היום" — Index performance: S&P 500, Nasdaq, Dow, Russell 2000 with exact % changes and point levels. Trading volume vs average. VIX level and change. One sentence on the dominant theme that drove the session.
2. heading: "המניות שעשו את הכותרות" — 4-6 stocks that moved significantly today. For each: $TICKER, % change, and WHY it moved (earnings beat/miss, analyst upgrade/downgrade, news catalyst, sector rotation). Do NOT just list names — explain the story.
3. heading: "מה זה אומר למחר" — The key takeaway from today's session. What shifted in market narrative? What economic data or earnings are due tomorrow? What is the biggest risk or opportunity going into the next session?"""

    elif review_type == "weekly_prep":
        return f"""You are a senior Wall Street strategist writing a weekly outlook in Hebrew.

Your task: Based on the tweets/posts below, prepare investors for the trading week ahead. Focus on the BIG PICTURE — macro themes, scheduled events across the full week, and technical levels. This should NOT read like a daily briefing — it's a strategic weekly view.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"תחזית שבועית – {date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 3 sections:
1. heading: "הנושא המרכזי של השבוע" — The ONE dominant macro theme this week (Fed policy, earnings season, geopolitics, trade war, inflation data). Why it matters THIS week specifically. What is the market pricing in vs what could surprise? 2-3 sentences max, sharp and analytical.
2. heading: "יומן השבוע" — Day-by-day schedule of key events. Format: יום ב': [event + time in Israel]. Include: economic data releases, major earnings reports, Fed speakers, options expiry, Treasury auctions, geopolitical deadlines. Be specific with dates and times.
3. heading: "רמות ותמונה טכנית" — S&P 500 and Nasdaq key support/resistance levels with specific numbers. VIX context — is fear elevated or complacent? Is the market in a clear trend, range-bound, or at a decision point? What price level would change the current narrative?"""

    elif review_type == "weekly_summary":
        return f"""You are a senior Wall Street strategist writing a weekly review in Hebrew.

Your task: Based on the tweets/posts below, summarize what happened in the US stock market THIS WEEK. Focus on weekly performance numbers, sector rotation, and what changed in the market narrative. This must NOT repeat daily headlines — zoom out and show the weekly picture.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"סיכום שבועי – {date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 3 sections:
1. heading: "השבוע במספרים" — Weekly performance table: S&P 500, Nasdaq, Dow, Russell 2000 (weekly % change). Then: 10Y Treasury yield, US Dollar (DXY), WTI Oil, Gold — each with weekly change. Pure data, minimal commentary. This section is about NUMBERS.
2. heading: "מה הניע את השוק השבוע" — The 2-3 biggest stories that actually drove markets this week. For each: what happened, what was the market reaction, and what are the forward implications. Be analytical — explain WHY the market moved, not just WHAT happened. Do NOT list every daily headline.
3. heading: "מפת סקטורים ומבט קדימה" — Leading sectors vs lagging sectors this week with % performance. Is there a rotation happening (growth→value, large→small, US→international)? End with: the 2-3 most important things to watch next week and why."""

    elif review_type == "events":
        return f"""You are a financial calendar editor creating an economic events calendar in Hebrew.

Your task: Based on the tweets/posts below AND your knowledge of the US economic calendar, create a list of upcoming economic events and market catalysts for the next 5-7 days.

Rules:
- Write event titles and descriptions in Hebrew.
- Include 6-10 events sorted by date (earliest first).
- Use Israel time (UTC+3) for all times.
- If exact time is unknown, use 15:30 (US market open in Israel time) as default.
- impact levels: "high" = moves entire market (Fed decision, NFP, CPI), "medium" = moves a sector (earnings, PMI), "low" = background data.

{tweets_block}

Output JSON format — THIS IS DIFFERENT FROM OTHER REVIEWS:
{{"items":[{{"time":"2026-03-30T15:30:00+03:00","title":"שם האירוע בעברית","impact":"high","description":"1-2 משפטים בעברית — מה זה ולמה זה חשוב למשקיעים"}}]}}

Event types to include: macro data (NFP, CPI, PPI, PMI, GDP, jobless claims), Fed rate decisions and Fed speaker appearances, major earnings reports (mega-cap and market-moving companies), options/futures expiry dates, Treasury auctions, geopolitical deadlines or summits."""

    return ""

# ══════════════════════════════════════════════════════════════
# GEMINI CALL
# ══════════════════════════════════════════════════════════════

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

    candidate = resp_data.get("candidates", [{}])[0]
    content = candidate.get("content", {})
    parts = content.get("parts", [])

    text = ""
    for part in parts:
        if "text" in part:
            text = part["text"]

    if not text:
        print(f"  Gemini raw response: {str(resp_data)[:500]}")
        raise Exception("Gemini returned no text")

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

# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

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
        items = result.get("items", [])
        if items:
            data["events"]["items"] = items
            data["events"]["lastUpdated"] = now.isoformat()
            print(f"  Stored {len(items)} events")
        else:
            print(f"  Warning: no 'items' key. Keys: {list(result.keys())}")
    elif REVIEW_TYPE in key_map:
        data[key_map[REVIEW_TYPE]] = result

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Updated {REVIEW_TYPE} successfully.")

if __name__ == "__main__":
    main()
