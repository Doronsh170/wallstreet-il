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
    """Get the Mon-Fri date range for the most recently COMPLETED trading week.
    Sat/Sun: the week that just ended (this week's Mon-Fri).
    Mon-Fri: previous week's Mon-Fri."""
    weekday = now.weekday()  # 0=Mon
    if weekday >= 5:  # Sat/Sun: the week that just ended
        monday = now - timedelta(days=weekday)
    else:  # Mon-Fri: previous week
        monday = now - timedelta(days=weekday + 7)
    friday = monday + timedelta(days=4)
    return f"{monday.strftime('%d/%m')}–{friday.strftime('%d/%m/%Y')}"

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

CRITICAL — KEY MARKET DATA (MANDATORY VERIFICATION):
- You MUST always include exact data for S&P 500, Nasdaq, and Dow Jones with % change and point levels.
- You MUST verify via Google Search the current prices of: Brent crude oil, WTI crude oil, gold, and any other commodity you mention.
- If a tweet states a price that seems extreme or unusual (e.g. Brent at $141 when it was recently at $110), you MUST verify it via Google Search before including it.
- NEVER trust a single tweet for major price data. Always cross-reference with Google Search.
- NEVER write vague descriptions like "the market closed in green territory" or "mixed trading" without exact numbers.

CRITICAL — DATA ACCURACY:
- For stock-specific data ($TICKER moves, earnings, upgrades): use numbers from the tweets.
- NEVER invent, estimate, or recall prices from memory.
- If the tweets mention a percentage move but no absolute price, report only the percentage — do NOT guess the price.
- Double-check every number you write. If a price seems unusually high or low compared to recent levels, verify it via Google Search before including it. Getting a number wrong destroys credibility.

CRITICAL — CONSISTENCY:
- The "שורה תחתונה" paragraph MUST be consistent with the bullet points above it. If the bullets show the market rose sharply, do NOT call it "mixed trading" in the summary.
- Read your own bullets before writing the bottom line to ensure no contradictions."""

def get_prompt(tweets, review_type, date_str, day_name, title_date_str=None, title_day_name=None, week_range=None, is_trading=True):
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

    if review_type == "daily_prep":
        trading_status = "TODAY IS A REGULAR TRADING DAY." if is_trading else "TODAY IS NOT A TRADING DAY (weekend or US holiday). There is NO market open today. Make this clear in the first bullet."
        return f"""You are a senior Wall Street market analyst writing a PRE-MARKET briefing in Hebrew for the trading day of {title_date_str}.

TRADING STATUS: {trading_status}

CRITICAL — THIS IS A FORWARD-LOOKING BRIEFING, NOT A SUMMARY:
- This is an "הכנה ליום מסחר" — what investors need to know BEFORE the market opens.
- Focus on: what is EXPECTED today, what CATALYSTS are scheduled, what RISKS lie ahead, what happened OVERNIGHT that will affect today's open.
- Do NOT write a summary of yesterday's trading. Yesterday's data (closing prices, moves) should only appear as CONTEXT in 1-2 bullets max, not as the main content.
- The majority of bullets (at least 70%) must be FORWARD-LOOKING: upcoming events, scheduled data releases, pre-market moves, overnight developments, geopolitical risks for today.
- If today is a US holiday or weekend (no trading), state this clearly in the first bullet and focus on what happened since the last session and what to watch for the next trading day.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: one concise fact." The sub-heading is a short topic label (2-4 words), plain text, no formatting tags.
- Do NOT use <b> tags or any HTML formatting. Plain text only.
- Include 7-12 bullets. Order:
  1. If no trading today — first bullet must state this clearly.
  2. 1-2 bullets of context: how did the last session close (index levels with %).
  3. Pre-market/futures data if available.
  4. Scheduled events TODAY: economic data releases (with Israel time), earnings reports, Fed speakers.
  5. Overnight developments: geopolitical, commodity moves, Asia/Europe markets.
  6. Notable stock catalysts for today: pre-market moves, upgrades/downgrades, news.
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences (NOT bullets).
  - If today IS a trading day: Start with the dominant theme for today's session. Then: what is the key risk, what scenario would change the picture, and what specific level or event should investors watch.
  - If today is NOT a trading day: Do NOT write "השוק נפתח היום" or anything implying the market is open. Instead, summarize the key developments since the last session, what risks are building over the break, and what to watch for when trading resumes. Start with "אין מסחר היום" or "השוק סגור היום".
  - CRITICAL: The bottom line MUST be consistent with the bullets above. If the first bullet says "אין מסחר", the bottom line CANNOT say "השוק נפתח".

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
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences (NOT bullets). Start with the key takeaway from the session. Then analyze: what shifted in the market narrative today, what does the price action signal about investor positioning, and what specific data or event tomorrow could change the trend. Give forward-looking insight, not just a recap.

{tweets_block}

Output JSON format:
{{"title":"סיכום יום המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"סיכום המסחר","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

    elif review_type == "weekly_prep":
        return f"""You are a senior Wall Street strategist writing a weekly outlook for Israeli investors in Hebrew.

Your task: Summarize in bullet points what investors need to know ahead of the trading week of {week_range if week_range else date_str} on Wall Street, based on the tweets/posts below. Write a flat list — each bullet has a short sub-heading followed by a colon, then the detail. Write in FUTURE TENSE.

CRITICAL — TIME FRAME:
- This preview covers the trading week {week_range if week_range else date_str} ONLY.
- Include ONLY events, data releases, and catalysts scheduled for THIS specific week.
- Do NOT include events from previous weeks or events beyond this week's Friday.
- When referencing "last week" for context, refer to the trading week BEFORE {week_range if week_range else date_str}.

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
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences (NOT bullets). Start with the ONE dominant theme for the week. Then analyze: what is the market currently pricing in, what could surprise to the upside or downside, and what combination of events could trigger a significant move. End with a clear framework — what scenario is bullish and what is bearish.

{tweets_block}

Output JSON format:
{{"title":"הכנה לשבוע מסחר בוול סטריט 🇺🇸 – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"נקודות מרכזיות לשבוע הקרוב","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

    elif review_type == "weekly_summary":
        return f"""You are a senior Wall Street strategist writing a weekly review for Israeli investors in Hebrew.

Your task: Summarize in bullet points the key events from the trading week of {week_range if week_range else date_str} on Wall Street, based on the tweets/posts below. Write a flat list — each bullet has a short sub-heading followed by a colon, then the detail. Write in PAST TENSE.

CRITICAL — TIME FRAME:
- This summary covers the trading week {week_range if week_range else date_str} ONLY.
- Include ONLY events, data, and market moves that occurred during THIS specific week.
- Do NOT include events from the current or upcoming week.
- If the tweets contain information from outside this date range, IGNORE it.

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
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences (NOT bullets). Start with the key narrative shift from this week. Then analyze: how did investor positioning change, what does the weekly price action imply about the medium-term trend, and what are the 2-3 most important things to watch next week and why they matter. Give strategic perspective, not just a data recap.

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
    import time
    max_retries = 3
    for attempt in range(max_retries):
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
        print(f"  Gemini status: {r.status_code} (attempt {attempt+1}/{max_retries})")

        if r.status_code == 503 or r.status_code == 429:
            if attempt < max_retries - 1:
                wait = 30 * (attempt + 1)
                print(f"  Gemini overloaded, retrying in {wait}s...")
                time.sleep(wait)
                continue
            else:
                print(f"  Gemini still unavailable after {max_retries} attempts")
                raise Exception(f"Gemini returned {r.status_code} after {max_retries} retries")

        candidate = resp_data.get("candidates", [{}])[0]
        content = candidate.get("content", {})
        parts = content.get("parts", [])

        text = ""
        for part in parts:
            if "text" in part:
                text = part["text"]

        if not text:
            if attempt < max_retries - 1:
                print(f"  Gemini returned no text, retrying in 30s...")
                time.sleep(30)
                continue
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

    raise Exception("call_gemini: exhausted all retries")

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
    target_is_trading = today_is_trading

    if REVIEW_TYPE == "daily_prep":
        # Title should reference the NEXT trading day
        if today_is_trading:
            target = now  # today IS the trading day we're preparing for
        else:
            target = get_next_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]
        # is_trading should reflect the TARGET date, not today
        target_is_trading = is_trading_day(target, holidays)

    elif REVIEW_TYPE == "daily_summary":
        # Title should reference the LAST trading day
        target = get_last_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]

    elif REVIEW_TYPE in ("weekly_prep", "weekly_summary"):
        if REVIEW_TYPE == "weekly_summary":
            week_range = get_prev_week_range_str(now)
        else:
            # weekly_prep: Mon-Fri = current week, Sat/Sun = next week
            weekday = now.weekday()
            if weekday <= 4:  # Mon-Fri: current week
                monday = now - timedelta(days=weekday)
            else:  # Sat/Sun: next week
                monday = now + timedelta(days=(7 - weekday))
            friday = monday + timedelta(days=4)
            week_range = f"{monday.strftime('%d/%m')}–{friday.strftime('%d/%m/%Y')}"

    print(f"  Title date: {title_date_str} ({title_day_name}), week_range: {week_range}")

    tweets = fetch_tweets()
    if not tweets:
        print("No tweets fetched, skipping.")
        return

    print(f"Fetched {len(tweets.split(chr(10)+chr(10)))} tweet blocks")

    prompt = get_prompt(tweets, REVIEW_TYPE, date_str, day_name,
                        title_date_str=title_date_str,
                        title_day_name=title_day_name,
                        week_range=week_range,
                        is_trading=target_is_trading if REVIEW_TYPE == "daily_prep" else today_is_trading)
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
