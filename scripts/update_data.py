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

Your task: Create a structured, bullet-point briefing of key points investors need before the US market opens, based on the tweets/posts below. Format it like a professional investor newsletter with clear topic sections and bullet points.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Each section "content" field MUST use bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet should be a single concise point with a bold label followed by a colon, then the detail.
- Format bold labels like this: <b>Label</b>: detail text.
- Keep bullets sharp — one key fact per bullet with specific numbers.
- Do NOT write long paragraphs. Use ONLY bullet points in the content.

{tweets_block}

Output JSON format:
{{"title":"נקודות חשובות לקראת פתיחת המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create 5-7 sections based on the topics available in the tweets. Structure:

FIRST SECTION:
- heading: "📍 {overnight_heading}" — 2-3 bullets summarizing the main overnight/weekend developments: futures, Asia/Europe moves, major breaking news. Each bullet = one fact with a number.

MIDDLE SECTIONS (3-5 sections, pick the most relevant topics from the tweets):
Choose from topics like these (use 📍 emoji before each heading):
- "📍 גיאופוליטיקה וסחר עולמי" — geopolitical risks, trade war updates, sanctions, military developments
- "📍 אינפלציה ומדיניות מוניטרית" — inflation data, Fed policy, interest rate expectations, central bank actions
- "📍 אמון צרכנים ומאקרו" — consumer sentiment, GDP, employment data, economic indicators
- "📍 מניות וסקטורים בולטים" — notable stock moves, sector rotation, earnings, upgrades/downgrades
- "📍 סחורות ושווקים גלובליים" — oil, gold, commodities, currency moves, global markets
- "📍 שוק העבודה" — jobs data, unemployment, labor market trends
- "📍 טכנולוגיה ובינה מלאכותית" — tech sector news, AI developments, mega-cap moves
- "📍 אג\"ח וסיכוני אשראי" — bond yields, credit spreads, CDS, Treasury moves
Pick ONLY topics that have real data in the tweets. Do NOT create empty sections. Each section: 2-4 bullets.

LAST SECTION:
- heading: "שורה תחתונה" — A single concise paragraph (NOT bullets) summarizing the key takeaway: what is the dominant theme, what should investors focus on, and what is the main risk. 2-3 sentences max."""

    elif review_type == "daily_summary":
        return f"""You are a senior Wall Street market analyst writing an end-of-day market wrap in Hebrew.

Your task: Create a structured, bullet-point summary of {close_desc}'s trading session on Wall Street, based on the tweets/posts below. Format it like a professional investor newsletter with clear topic sections and bullet points.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Each section "content" field MUST use bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet should be a single concise point with a bold label followed by a colon, then the detail.
- Format bold labels like this: <b>Label</b>: detail text.
- Keep bullets sharp — one key fact per bullet with specific numbers.
- Do NOT write long paragraphs. Use ONLY bullet points in the content.

{tweets_block}

Output JSON format:
{{"title":"סיכום יום המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create 5-7 sections based on the topics available in the tweets. Structure:

FIRST SECTION:
- heading: "📍 {close_heading}" — 3-4 bullets: S&P 500, Nasdaq, Dow, Russell 2000 with exact % changes. VIX level. Trading volume note. One bullet on the dominant theme.

MIDDLE SECTIONS (3-5 sections, pick the most relevant topics from the tweets):
Choose from topics like these (use 📍 emoji before each heading):
- "📍 המניות שעשו את הכותרות" — 3-5 specific stocks: $TICKER, % move, and WHY (earnings, upgrade, news). One bullet per stock.
- "📍 גיאופוליטיקה והשפעה על השוק" — geopolitical developments that moved markets today
- "📍 אינפלציה ומדיניות מוניטרית" — inflation data, Fed comments, rate expectations
- "📍 סחורות ומט\"ח" — oil, gold, dollar, bond yields with exact numbers
- "📍 סקטורים בולטים" — which sectors led, which lagged, rotation signals
- "📍 סנטימנט ותנודתיות" — VIX, put/call ratio, fund flows, institutional moves
- "📍 נתוני מאקרו" — economic data released today and market reaction
- "📍 טכנולוגיה ובינה מלאכותית" — tech sector and AI-related moves
Pick ONLY topics that have real data in the tweets. Do NOT create empty sections. Each section: 2-4 bullets.

LAST SECTION:
- heading: "שורה תחתונה" — A single concise paragraph (NOT bullets) summarizing: what was the key takeaway from the session, what shifted in the market narrative, and what is the main thing to watch for {tomorrow_ref}. 2-3 sentences max."""

    elif review_type == "weekly_prep":
        return f"""You are a senior Wall Street strategist writing a comprehensive weekly outlook newsletter in Hebrew.

Your task: Based on the tweets/posts below, create a professional weekly preview that reads like a premium investor newsletter. Write in FUTURE TENSE — this is about what's COMING this week. The tone should be analytical, data-driven, and narrative (not bullet points). Use full paragraphs with specific numbers.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Write in flowing Hebrew paragraphs, NOT bullet points.
- For earnings/company mentions, use "■ " (■ + space) at the start of each company paragraph.
- Use <b>bold</b> tags for key terms, company names, and important numbers.
- Each section should be 2-4 paragraphs of substantive analysis.
- Write in FUTURE TENSE (צפוי, ידווחו, יתפרסם, יתמקדו).

{tweets_block}

Output JSON format:
{{"title":"הכנה לשבוע מסחר בוול סטריט 🇺🇸 – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 5 sections:

1. heading: "פתיח לשבוע"
Context paragraph: How did last week end? What are the key index levels (S&P 500, Nasdaq, Dow with % changes)? What is the dominant sentiment going into this week? What is the ONE big question the market is trying to answer? Write 1-2 paragraphs with specific numbers.

2. heading: "מה צפוי בשבוע הקרוב"
The key macro events and catalysts for this week. Include: Fed decisions/speakers, economic data releases (GDP, NFP, CPI, PMI, jobless claims — with expected dates), trade/tariff deadlines, geopolitical milestones. For each — explain WHY it matters and what the market expects. Write 2-3 detailed paragraphs.

3. heading: "אירועים חשובים שישפיעו על השוק"
Non-macro events that could move markets: geopolitical developments, regulatory decisions, trade negotiations, sector-specific catalysts, technical levels. Be specific about what could surprise. Write 1-2 paragraphs.

4. heading: "חברות חשובות שצפויות לדווח"
Major earnings reports expected this week. Format each company as a separate paragraph starting with "■ ". For each: company name ($TICKER), expected report date, key metrics to watch, and why the report matters. Include 4-6 companies, prioritizing mega-caps and market-moving names.

5. heading: "בשורה התחתונה"
A concise closing paragraph: What is the ONE thing investors should focus on this week? What scenario would be bullish vs bearish? 2-3 sentences max."""

    elif review_type == "weekly_summary":
        return f"""You are a senior Wall Street strategist writing a comprehensive weekly review newsletter in Hebrew.

Your task: Based on the tweets/posts below, create a professional weekly summary that reads like a premium investor newsletter. Write in PAST TENSE — this is about what HAPPENED this week. The tone should be analytical, data-driven, and narrative (not bullet points). Use full paragraphs with specific numbers.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Write in flowing Hebrew paragraphs, NOT bullet points.
- For earnings/company mentions, use "■ " (■ + space) at the start of each company paragraph.
- Use <b>bold</b> tags for key terms, company names, and important numbers.
- Each section should be 2-4 paragraphs of substantive analysis.
- Write in PAST TENSE (רשם, עלה, ירד, דיווחה, פורסם).

{tweets_block}

Output JSON format:
{{"title":"סיכום שבוע המסחר בוול סטריט 🇺🇸 – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"heading","content":"content"}}]}}

Create exactly 5 sections:

1. heading: "פתיח שבועי"
Opening summary: How did the major indices perform this week? S&P 500, Nasdaq, Dow — each with weekly % change and notable milestones (new highs, correction territory, streaks). What was the dominant narrative/theme of the week? What drove sentiment? Write 1-2 paragraphs with specific numbers.

2. heading: "אירועי המאקרו שפורסמו השבוע"
The key economic data releases this week and their significance. Include: employment data, inflation readings, PMI, GDP, consumer sentiment, housing — whichever were published. For each: the actual number, what was expected, and how the market reacted. Write 2-3 detailed paragraphs.

3. heading: "אירועים חשובים שהשפיעו על השוק"
Non-macro events that moved markets: geopolitical developments, Fed speaker comments, trade/tariff news, regulatory actions, sector-specific catalysts, commodity moves (oil, gold). Be specific about cause and effect. Write 1-2 paragraphs.

4. heading: "דוחות רבעוניים בולטים"
The most notable earnings reports this week. Format each company as a separate paragraph starting with "■ ". For each: company name ($TICKER), stock move (%), key results (revenue, EPS vs estimates), and the story behind the move. Include 4-6 companies, prioritizing the biggest movers and market-relevant names.

5. heading: "בשורה התחתונה"
A concise closing paragraph: What is the key takeaway from this week? How did the market narrative shift? What are the main risks and opportunities heading into next week? 2-3 sentences max."""

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
        if REVIEW_TYPE == "weekly_summary":
            week_range = get_prev_week_range_str(now)
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
