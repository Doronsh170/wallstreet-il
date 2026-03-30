import json, os, requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

ISR_TZ = ZoneInfo("Asia/Jerusalem")
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
    """Find the most recent COMPLETED trading day.
    If today is a trading day AND market has closed (after 23:00 Israel), return today.
    Otherwise, look backwards from yesterday."""
    if is_trading_day(now, holidays):
        hour = now.hour
        if hour >= 23:  # Market closed at 23:00 Israel time
            return now
    # Start from yesterday and go back
    d = now - timedelta(days=1)
    for _ in range(10):
        if is_trading_day(d, holidays):
            return d
        d -= timedelta(days=1)
    return now - timedelta(days=1)

def get_prev_week_range_str(now):
    """Get the Mon-Fri date range for the PREVIOUS completed trading week."""
    weekday = now.weekday()  # 0=Mon
    # Go to last week's Monday
    days_to_last_monday = weekday + 7
    last_monday = now - timedelta(days=days_to_last_monday)
    last_friday = last_monday + timedelta(days=4)
    return f"{last_monday.strftime('%d/%m')}–{last_friday.strftime('%d/%m/%Y')}"

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
    # Monday should also be treated as "after gap" since the last trading was Friday
    is_non_trading = day_name in ["שישי", "שבת", "ראשון", "שני"]

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

Your task: Summarize in bullet points everything investors need to know before today's US market open, based on the tweets/posts below. Write a flat list of the most important facts — no topic groupings, no sub-sections, just one sharp bullet after another.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: one concise fact." The sub-heading is a short topic label (2-4 words), plain text, no formatting tags.
- Do NOT use <b> tags or any HTML formatting. Plain text only.
- Include 7-12 bullets covering the most important topics: geopolitics, macro data, index/futures moves, notable stocks, commodities, Fed/rates, sentiment — whatever is in the tweets.
- Do NOT group bullets under sub-headings. Do NOT use 📍 or any section dividers. Just a flat list.
- Order bullets from most market-moving to least important.
- The second section is "שורה תחתונה" — a single concise paragraph (NOT bullets), 2-3 sentences summarizing the dominant theme and main risk.

{tweets_block}

Output JSON format:
{{"title":"נקודות חשובות לקראת פתיחת המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"נקודות מרכזיות","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

    elif review_type == "daily_summary":
        return f"""You are a senior Wall Street market analyst writing an end-of-day market wrap in Hebrew.

Your task: Summarize in bullet points everything that happened in today's trading session on Wall Street, based on the tweets/posts below. Write a flat list of the most important facts — no topic groupings, no sub-sections, just one sharp bullet after another.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: one concise fact." The sub-heading is a short topic label (2-4 words), plain text, no formatting tags.
- Do NOT use <b> tags or any HTML formatting. Plain text only.
- Include 7-12 bullets covering: index performance (S&P 500, Nasdaq, Dow with %), notable stock moves ($TICKER +/- %), VIX, commodities, geopolitical impact, macro data, sector moves, institutional activity — whatever is in the tweets.
- Do NOT group bullets under sub-headings. Do NOT use 📍 or any section dividers. Just a flat list.
- Start with index performance bullets, then order by market impact.
- The second section is "שורה תחתונה" — a single concise paragraph (NOT bullets), 2-3 sentences summarizing the key takeaway and what to watch for tomorrow.

{tweets_block}

Output JSON format:
{{"title":"סיכום יום המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"סיכום המסחר","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

    elif review_type == "weekly_prep":
        return f"""You are a senior Wall Street strategist writing a weekly outlook for Israeli investors in Hebrew.

Your task: Summarize in bullet points what investors need to know ahead of the coming trading week on Wall Street, based on the tweets/posts below. Write a flat list — each bullet has a short sub-heading followed by a colon, then the detail. Write in FUTURE TENSE.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet format: "* Sub-heading: detail text." — the sub-heading is a short topic label (2-4 words), NOT bold, NOT wrapped in tags.
- Do NOT use <b> tags, ■ markers, 📍 emojis, or any special formatting. Plain text only.
- Include 8-14 bullets, in this order:
  1. START with 1-2 bullets summarizing how last week ended (index performance with %, dominant theme) — this gives context.
  2. Then the key events coming THIS week: Fed decisions, economic data (NFP, CPI, PMI, GDP), earnings reports, trade/tariff deadlines, geopolitical developments.
  3. For each event: include the specific day and Israel time when known (e.g. "יום רביעי 21:00 שעון ישראל").
  4. End with notable companies expected to report earnings this week.
- Do NOT repeat information across bullets. Each bullet = one unique fact.
- The second section is "שורה תחתונה" — a single concise paragraph (NOT bullets), 2-3 sentences on the ONE thing to focus on this week and what would be bullish vs bearish.

{tweets_block}

Output JSON format:
{{"title":"הכנה לשבוע מסחר בוול סטריט 🇺🇸 – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"נקודות מרכזיות לשבוע הקרוב","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

    elif review_type == "weekly_summary":
        return f"""You are a senior Wall Street strategist writing a weekly review for Israeli investors in Hebrew.

Your task: Summarize in bullet points the key events from this week's trading on Wall Street, based on the tweets/posts below. Write a flat list — each bullet has a short sub-heading followed by a colon, then the detail. Write in PAST TENSE.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet format: "* Sub-heading: detail text." — the sub-heading is a short topic label (2-4 words), NOT bold, NOT wrapped in tags.
- Do NOT use <b> tags, ■ markers, 📍 emojis, or any special formatting. Plain text only.
- Include 8-14 bullets, in this order:
  1. START with index performance: S&P 500, Nasdaq, Dow — weekly % changes, notable milestones (highs, correction territory).
  2. Then macro data published this week: employment, inflation, PMI, GDP, consumer sentiment — actual numbers and market reaction.
  3. Then key events that moved markets: geopolitics, Fed comments, trade/tariff news, commodity moves (oil, gold with numbers).
  4. Then notable earnings reports: company ($TICKER), stock move %, key result.
  5. End with sector rotation or institutional activity if relevant.
- Do NOT repeat information across bullets. Each bullet = one unique fact.
- The second section is "שורה תחתונה" — a single concise paragraph (NOT bullets), 2-3 sentences on the key takeaway from the week and what to watch next week.

{tweets_block}

Output JSON format:
{{"title":"סיכום שבוע המסחר בוול סטריט 🇺🇸 – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"סיכום השבוע","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

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
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}",
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
        if REVIEW_TYPE == "weekly_summary":
            week_range = get_week_range_str(now)
        else:
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
