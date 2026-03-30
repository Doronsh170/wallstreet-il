import json, os, requests
from datetime import datetime, timezone, timedelta

ISR_TZ = timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

PY_TO_HEB = {0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}

def load_holidays():
    """Load US holidays from data.json"""
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("marketStatus", {}).get("usHolidays2026", [])
    except:
        return []

def is_trading_day(dt, holidays):
    """Check if a date is a US trading day (Mon-Fri, not a holiday)"""
    if dt.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    return dt.strftime("%Y-%m-%d") not in holidays

def get_next_trading_day(now, holidays):
    """Find the next trading day from now"""
    d = now + timedelta(days=1)
    for _ in range(10):
        if is_trading_day(d, holidays):
            return d
        d += timedelta(days=1)
    return now + timedelta(days=1)

def get_last_trading_day(now, holidays):
    """Find the most recent trading day (today if trading, else look back)"""
    d = now
    for _ in range(10):
        if is_trading_day(d, holidays):
            return d
        d -= timedelta(days=1)
    return now

def get_week_range_str(now):
    """Get the Mon-Fri date range for the current/most recent trading week.
    If today is Sat/Sun, use the week that just ended (Mon-Fri).
    If today is Mon-Fri, use this week's Mon-Fri."""
    weekday = now.weekday()  # 0=Mon
    if weekday >= 5:  # Weekend — refer to the week that just ended
        days_since_monday = weekday  # Sat=5, Sun=6
        monday = now - timedelta(days=days_since_monday)
    else:
        monday = now - timedelta(days=weekday)
    friday = monday + timedelta(days=4)
    return f"{monday.strftime('%d/%m')}–{friday.strftime('%d/%m/%Y')}"

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
- Output pure JSON only, no backticks, no explanations.

CRITICAL — DATA ACCURACY:
- ONLY use numbers, prices, percentages, and data points that explicitly appear in the source tweets/posts below.
- NEVER invent, estimate, or recall prices from memory. If a specific price (gold, oil, index level, stock price) does not appear in the tweets, do NOT include it. Skip it or write "לא דווח".
- If the tweets mention a percentage move but no absolute price, report only the percentage — do NOT guess the price.
- Double-check every number you write: if you cannot point to a specific tweet that contains that number, remove it.
- Getting a number wrong (e.g. writing $2,400 gold when it is actually $4,400) destroys credibility. When in doubt, omit."""

def get_prompt(tweets, review_type, date_str, day_name, title_date_str=None, title_day_name=None, week_range=None):
    """
    date_str / day_name = when the script runs (today)
    title_date_str / title_day_name = the trading day the title should reference
    week_range = e.g. "24/03–28/03/2026" for weekly reviews
    """
    if not title_date_str:
        title_date_str = date_str
    if not title_day_name:
        title_day_name = day_name

    tweets_block = f"Source tweets/posts from X (Twitter) — date: {date_str}:\n{tweets}"

    # Day-aware headings
    is_non_trading = day_name in ["שישי", "שבת", "ראשון"]

    # daily_prep headings
    overnight_heading = "מה קרה בסוף השבוע" if is_non_trading else "מה קרה בלילה"
    market_today_heading = "מה יזיז את השוק ביום המסחר הקרוב" if is_non_trading else "מה יזיז את השוק היום"
    overnight_desc = "Key developments over the weekend" if is_non_trading else "Key overnight developments"
    today_catalysts = "Next trading day's catalysts" if is_non_trading else "Today's catalysts"
    today_ref = "the next trading day" if is_non_trading else "today"

    # daily_summary headings
    close_heading = "כך נסגר יום המסחר האחרון" if is_non_trading else "כך נסגר היום"
    tomorrow_heading = "מה זה אומר ליום המסחר הבא" if is_non_trading else "מה זה אומר למחר"
    close_desc = "the last trading day" if is_non_trading else "today"
    tomorrow_ref = "the next trading session" if is_non_trading else "tomorrow"

    if review_type == "daily_prep":
        return f"""You are a senior Wall Street market analyst writing a pre-market briefing in Hebrew.

Your task: Summarize what investors need to know before the US market opens, based on the tweets/posts below. This is a sharp 3-minute morning read for professional investors.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"הכנה ליום מסחר – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 3 sections:
1. heading: "{overnight_heading}" — {overnight_desc}: futures levels (S&P, Nasdaq, Dow with exact numbers), Asia/Europe session moves, major news that broke after US close. Numbers only, no fluff.
2. heading: "{market_today_heading}" — {today_catalysts}: economic data releases (with Israel times), earnings reports due, Fed speakers, geopolitical developments. For each — one sentence on why it matters.
3. heading: "מניות תחת זרקור" — 3-5 specific stocks with a catalyst for {today_ref}: pre-market moves (%), upgrades/downgrades, insider activity, earnings surprise. Format each as: ticker + number + reason."""

    elif review_type == "daily_summary":
        return f"""You are a senior Wall Street market analyst writing an end-of-day market wrap in Hebrew.

Your task: Summarize the main events of {close_desc} on Wall Street, based on the tweets/posts below. Tell investors what happened, why, and what it means for {tomorrow_ref}.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"סיכום יום מסחר – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 3 sections:
1. heading: "{close_heading}" — Index performance: S&P 500, Nasdaq, Dow, Russell 2000 with exact % changes and point levels. Trading volume vs average. VIX level and change. One sentence on the dominant theme that drove the session.
2. heading: "המניות שעשו את הכותרות" — 4-6 stocks that moved significantly. For each: $TICKER, % change, and WHY it moved (earnings beat/miss, analyst upgrade/downgrade, news catalyst, sector rotation). Do NOT just list names — explain the story.
3. heading: "{tomorrow_heading}" — The key takeaway from the session. What shifted in market narrative? What economic data or earnings are due for {tomorrow_ref}? What is the biggest risk or opportunity going into the next session?"""

    elif review_type == "weekly_prep":
        return f"""You are a senior Wall Street strategist writing a weekly outlook in Hebrew.

Your task: Based on the tweets/posts below, prepare investors for the trading week ahead. Focus on the BIG PICTURE — macro themes, scheduled events across the full week, and technical levels. This should NOT read like a daily briefing — it's a strategic weekly view.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"תחזית שבועית – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

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
{{"title":"סיכום שבועי – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

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
            "tools": [{"google_search": {}}],
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 8192
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
    day_name = PY_TO_HEB[now.weekday()]

    # Load holidays for trading day calculations
    holidays = load_holidays()
    today_is_trading = is_trading_day(now, holidays)

    print(f"Running {REVIEW_TYPE} for {date_str} ({day_name}), trading day: {today_is_trading}")

    # ── Compute the correct title date for each review type ──
    title_date_str = date_str
    title_day_name = day_name
    week_range = None

    if REVIEW_TYPE == "daily_prep":
        # Title should reference the NEXT trading day
        if today_is_trading:
            target = now  # today IS the trading day we're preparing for
        else:
            target = get_next_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]

    elif REVIEW_TYPE == "daily_summary":
        # Title should reference the LAST trading day
        target = get_last_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]

    elif REVIEW_TYPE in ("weekly_prep", "weekly_summary"):
        week_range = get_week_range_str(now)

    if REVIEW_TYPE == "weekly_prep":
        # For weekly prep, compute next week's range
        # Find next Monday
        days_ahead = (0 - now.weekday()) % 7  # 0=Monday
        if days_ahead == 0 and now.weekday() == 0:
            next_monday = now  # already Monday
        else:
            next_monday = now + timedelta(days=days_ahead if days_ahead > 0 else 7)
        next_friday = next_monday + timedelta(days=4)
        week_range = f"{next_monday.strftime('%d/%m')}–{next_friday.strftime('%d/%m/%Y')}"

    print(f"  Title date: {title_date_str} ({title_day_name}), week_range: {week_range}")

    tweets = fetch_tweets()
    if not tweets:
        print("No tweets fetched, skipping.")
        return

    print(f"Fetched {len(tweets.split(chr(10)+chr(10)))} tweet blocks")

    prompt = get_prompt(tweets, REVIEW_TYPE, date_str, day_name,
                        title_date_str=title_date_str,
                        title_day_name=title_day_name,
                        week_range=week_range)
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
