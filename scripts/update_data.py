import json, os, requests
from datetime import datetime, timezone, timedelta

ISR_TZ = timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

PY_TO_HEB = {0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}

def fetch_market_data(weekly=False):
    """Fetch verified market data from Finnhub API.
    If weekly=True, also fetches weekly performance data using candle endpoint."""
    if not FINNHUB_API_KEY:
        print("  No FINNHUB_API_KEY, skipping market data fetch")
        return ""

    symbols = {
        # Major indices
        "SPY": "S&P 500 (SPY ETF)",
        "QQQ": "Nasdaq 100 (QQQ ETF)",
        "DIA": "Dow Jones (DIA ETF)",
        "IWM": "Russell 2000 (IWM ETF)",
        # Commodities
        "USO": "WTI Crude Oil (USO ETF)",
        "BNO": "Brent Crude Oil (BNO ETF)",
        "GLD": "Gold (GLD ETF)",
        # Crypto
        "IBIT": "Bitcoin (IBIT ETF)",
        # Volatility
        "VIXY": "VIX (VIXY ETF)",
        # Bonds
        "TLT": "US 20Y+ Bonds (TLT ETF)",
        # Dollar
        "UUP": "US Dollar (UUP ETF)",
    }

    # Daily quotes
    lines = []
    for symbol, label in symbols.items():
        try:
            r = requests.get(
                f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}",
                timeout=5
            )
            if r.ok:
                d = r.json()
                price = d.get("c", 0)
                pct = d.get("dp", 0)
                prev = d.get("pc", 0)
                if price > 0:
                    direction = "+" if pct >= 0 else ""
                    lines.append(f"  {label}: ${price:.2f} (daily: {direction}{pct:.2f}%), prev close: ${prev:.2f}")
                    print(f"  Finnhub {symbol}: ${price:.2f} ({direction}{pct:.2f}%)")
        except Exception as e:
            print(f"  Finnhub error for {symbol}: {e}")

    # Weekly performance using candle endpoint
    weekly_lines = []
    if weekly:
        import time as time_module
        now_ts = int(time_module.time())
        from_ts = now_ts - (14 * 86400)  # 14 days back to ensure we capture the full week

        key_symbols = ["SPY", "QQQ", "DIA", "IWM", "USO", "BNO", "GLD", "IBIT"]
        for symbol in key_symbols:
            label = symbols.get(symbol, symbol)
            try:
                r = requests.get(
                    f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=D&from={from_ts}&to={now_ts}&token={FINNHUB_API_KEY}",
                    timeout=5
                )
                if r.ok:
                    d = r.json()
                    closes = d.get("c", [])
                    timestamps = d.get("t", [])
                    if len(closes) >= 5:
                        # Find this week's Monday (or first trading day) and Friday (or last trading day)
                        # Get the dates
                        from datetime import datetime as dt_cls
                        dated_closes = []
                        for i, ts in enumerate(timestamps):
                            date = dt_cls.utcfromtimestamp(ts)
                            dated_closes.append((date, closes[i]))

                        # Split into weeks (Mon=0)
                        current_week = []
                        prev_week = []
                        today = dt_cls.utcnow()
                        # Current week = same ISO week as today or latest Friday
                        for date, close in dated_closes:
                            if date.isocalendar()[1] == today.isocalendar()[1]:
                                current_week.append((date, close))
                            elif date.isocalendar()[1] == today.isocalendar()[1] - 1 or \
                                 (today.isocalendar()[1] == 1 and date.isocalendar()[1] >= 52):
                                prev_week.append((date, close))

                        # If running on Saturday (week summary), current_week might be empty
                        # Use the latest complete week
                        if not current_week and prev_week:
                            current_week = prev_week
                            prev_week = []

                        if current_week and len(dated_closes) > len(current_week):
                            week_open = current_week[0][1]  # Monday open ≈ Monday close of prev day
                            week_close = current_week[-1][1]  # Friday close
                            # Find the close before this week started
                            all_before = [c for d, c in dated_closes if d < current_week[0][0]]
                            if all_before:
                                prev_friday_close = all_before[-1]
                                weekly_pct = ((week_close - prev_friday_close) / prev_friday_close) * 100
                                direction = "+" if weekly_pct >= 0 else ""
                                weekly_lines.append(f"  {label}: weekly {direction}{weekly_pct:.2f}% (from ${prev_friday_close:.2f} to ${week_close:.2f})")
                                print(f"  Finnhub {symbol} WEEKLY: {direction}{weekly_pct:.2f}%")
            except Exception as e:
                print(f"  Finnhub weekly error for {symbol}: {e}")

    if not lines:
        return ""

    result_lines = [
        "\n══ VERIFIED MARKET DATA (from Finnhub API — these are FACTS, do NOT override with guesses) ══",
        "DAILY PERFORMANCE:",
        *lines,
    ]

    if weekly_lines:
        result_lines.append("")
        result_lines.append("WEEKLY PERFORMANCE (use these for weekly summary, NOT the daily numbers):")
        result_lines.extend(weekly_lines)

    result_lines.extend([
        "",
        "CONVERSION GUIDE (ETF price → actual value):",
        "  S&P 500 index ≈ SPY × 10 | Nasdaq 100 index ≈ QQQ × 80 | Dow index ≈ DIA × 100",
        "  Gold $/oz ≈ GLD × 24 | Bitcoin ≈ IBIT × 550",
        "  For exact index levels, oil prices ($/barrel), and VIX level: use Google Search.",
        "  The % changes above are ACCURATE — use them for direction and magnitude.",
        "  If ANY number you write contradicts the data above, you are WRONG. Fix it.",
        "══════════════════════════════════════════════════════════════════════════════════════════\n"
    ])

    return "\n".join(result_lines)

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
- Do NOT mention the same ticker or company in multiple separate bullets. If a company has multiple news items, combine them into ONE bullet. For example, if $SNDK is joining Nasdaq 100 AND got a price target upgrade, put both facts in one bullet about $SNDK.
- No buy/sell recommendations.
- Start each section directly with the key fact. No generic opening sentences.
- Output pure JSON only, no backticks, no explanations.

CRITICAL — KEY MARKET DATA (MANDATORY VERIFICATION):
- If VERIFIED MARKET DATA from Finnhub API is provided above the tweets, you MUST use those numbers for index performance (% change). Do NOT override them with numbers from tweets or from memory.
- Use the verified % changes as-is. For exact index point levels, use Google Search to convert ETF prices to index levels (S&P 500 ≈ SPY × 10, Nasdaq 100 ≈ QQQ × ~80, Dow ≈ DIA × ~100).
- You MUST verify via Google Search the current prices of: Brent crude oil, WTI crude oil, gold, and any other commodity you mention.
- If a tweet states a price that seems extreme or unusual, you MUST verify it via Google Search before including it.
- NEVER trust a single tweet for major price data. Always cross-reference.
- NEVER write vague descriptions like "the market closed in green territory" or "mixed trading" without exact numbers.
- NEVER claim an index or stock is at an "all-time high" (שיא / שיא כל הזמנים) unless you verify it via Google Search. A positive day does NOT automatically mean a record.

CRITICAL — MAJOR ECONOMIC DATA (DO NOT MISS):
- Use Google Search to check if any major US economic data was released today: CPI, PPI, NFP, GDP, Jobless Claims, ISM PMI, Consumer Confidence, Retail Sales, FOMC minutes/decision.
- If major data WAS released today, it MUST appear in the review — even if no tweet mentions it. This is non-negotiable.
- CPI and NFP are the two most important data releases. Missing them from a daily review is a critical failure.
- When mentioning economic data, ALWAYS include the actual numbers: percentage change, absolute value, and comparison to forecast. For example: "מדד המחירים לצרכן (CPI) עלה ב-0.9% על בסיס חודשי וב-3.3% על בסיס שנתי" — NOT just "מדד המחירים הצביע על עלייה חדה". Vague descriptions without numbers are unacceptable.

CRITICAL — DATA ACCURACY:
- EVERY number in the review must come from one of these sources: (1) Finnhub verified data above, (2) a specific tweet, or (3) Google Search verification.
- NEVER invent, estimate, or recall prices from memory. If you cannot point to a source, do NOT include the number.
- For stock-specific data ($TICKER moves, earnings, upgrades): use numbers from the tweets.
- If the tweets mention a percentage move but no absolute price, report only the percentage — do NOT guess the price.
- If a number from a tweet contradicts the Finnhub verified data, the Finnhub data is correct — the tweet is wrong.
- Getting a number wrong destroys credibility. When in doubt, omit.

CRITICAL — CONSISTENCY:
- The "שורה תחתונה" paragraph MUST be consistent with the bullet points above it. If the bullets show the market rose sharply, do NOT call it "mixed trading" in the summary.
- Read your own bullets before writing the bottom line to ensure no contradictions.

CRITICAL — FINANCIAL TERMINOLOGY:
- Use precise Hebrew financial terms. Getting terminology wrong destroys credibility.
- IPO (הנפקה ראשונית לציבור) is NOT the same as ETF (תעודת סל). Never confuse them.
- A private company planning an IPO is issuing shares — it does NOT have an ETF.
- SPO = הנפקה משנית, SPAC = חברת רכש ייעודית, M&A = מיזוג ורכישה.
- Futures = חוזים עתידיים, Options = אופציות, Bonds = אגרות חוב.
- If unsure about the correct Hebrew term, use the English term with Hebrew explanation in parentheses.

CRITICAL — US-ISRAEL TIME CONVERSION:
- US market opens at 9:30 AM ET, closes at 4:00 PM ET.
- To convert US Eastern Time to Israel time, use the offset provided below.
- NEVER guess the time offset — use ONLY the value calculated for today."""

def get_us_israel_offset(now):
    """Calculate the current US Eastern → Israel time offset in hours.
    Handles DST transitions for both US and Israel."""
    import calendar

    year = now.year

    # US DST: 2nd Sunday of March → 1st Sunday of November
    mar1 = datetime(year, 3, 1)
    us_dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7)  # 2nd Sunday
    nov1 = datetime(year, 11, 1)
    us_dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)  # 1st Sunday

    # Israel DST: last Friday before April 2 → last Sunday of October
    apr2 = datetime(year, 4, 2)
    il_dst_start = apr2 - timedelta(days=(apr2.weekday() + 3) % 7)  # last Friday before Apr 2
    oct31 = datetime(year, 10, 31)
    il_dst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)  # last Sunday of October

    today = now.replace(tzinfo=None)
    us_is_dst = us_dst_start <= today.replace(hour=0, minute=0, second=0) < us_dst_end
    il_is_dst = il_dst_start <= today.replace(hour=0, minute=0, second=0) < il_dst_end

    us_offset = -4 if us_is_dst else -5  # EDT or EST
    il_offset = 3 if il_is_dst else 2    # IDT or IST

    return il_offset - us_offset  # hours to add to ET to get Israel time

def get_time_conversion_block(now):
    """Generate the time conversion info for the prompt."""
    offset = get_us_israel_offset(now)

    def convert(hour, minute):
        ih = hour + offset
        return f"{ih}:{minute:02d}"

    return f"""
US-ISRAEL TIME OFFSET TODAY: +{offset} hours (add {offset} hours to US Eastern Time)
Key times in Israel time today:
- US economic data releases (CPI, NFP, PPI, GDP, Jobless Claims): {convert(8,30)} שעון ישראל
- ISM PMI, JOLTS, Consumer Confidence: {convert(10,0)} שעון ישראל
- FOMC rate decision / FOMC minutes: {convert(14,0)} שעון ישראל
- Fed Chair press conference: {convert(14,30)} שעון ישראל
- US market open: {convert(9,30)} שעון ישראל
- US market close: {convert(16,0)} שעון ישראל
USE ONLY THESE TIMES. Do NOT calculate your own offset."""

def get_prompt(tweets, review_type, date_str, day_name, title_date_str=None, title_day_name=None, week_range=None, is_trading=True, market_data=""):
    """
    date_str / day_name = when the script runs (today)
    title_date_str / title_day_name = the trading day the title should reference
    week_range = e.g. "24/03–28/03/2026" for weekly reviews
    market_data = verified data from Finnhub API
    """
    if not title_date_str:
        title_date_str = date_str
    if not title_day_name:
        title_day_name = day_name

    tweets_block = f"Source tweets/posts from X (Twitter) — date: {date_str}:\n{tweets}"
    if market_data:
        tweets_block = market_data + "\n" + tweets_block

    # Add time conversion info
    from datetime import datetime as dt_class
    time_block = get_time_conversion_block(dt_class.now(ISR_TZ))
    tweets_block = time_block + "\n" + tweets_block

    if review_type == "daily_prep":
        is_same_day = (date_str == title_date_str)
        if is_trading:
            if is_same_day:
                trading_status = "The briefing is for TODAY — a regular trading day. Use 'היום' and 'הבוקר' freely."
            else:
                trading_status = f"IMPORTANT: This script runs on {date_str} ({day_name}) but the briefing is for the NEXT trading day: {title_date_str} ({title_day_name}). Do NOT use 'היום' or 'הבוקר' — use 'ביום {title_day_name}' or 'בפתיחת המסחר ביום {title_day_name}' instead. Do NOT mention futures or pre-market data as if they are live right now — they are not available yet."
        else:
            trading_status = f"The target date {title_date_str} is NOT a trading day (weekend or US holiday). State this clearly in the first bullet."
        return f"""You are a senior Wall Street market analyst writing a PRE-MARKET briefing in Hebrew.

DATES:
- Script run date: {date_str} ({day_name})
- Briefing target date: {title_date_str} ({title_day_name})
- {trading_status}

CRITICAL — THIS IS A FORWARD-LOOKING BRIEFING, NOT A SUMMARY:
- This is an "הכנה ליום מסחר" — what investors need to know BEFORE the market opens.
- DO NOT summarize the last trading session. There is a separate "סיכום יומי" review for that.
- The ONLY backward-looking data allowed is ONE bullet with index closing levels for context. Nothing more.
- ALL other bullets must be FORWARD-LOOKING:
  * What economic data is scheduled for the target trading day (with Israel times)?
  * What earnings reports are expected?
  * What geopolitical risks or developments could move markets?
  * What overnight news broke that will affect the open?
  * What sectors or stocks have catalysts coming?
  * What are analysts/strategists saying about today's expected moves?
- Think of it as: "מה צפוי? מה הסיכונים? מה לעקוב אחריו?" — NOT "מה קרה?"

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: one concise fact." The sub-heading is a short topic label (2-4 words), plain text, no formatting tags.
- Do NOT use <b> tags or any HTML formatting. Plain text only.
- Include 7-12 bullets. Order:
  1. If the briefing is for a future date, first bullet states when trading resumes.
  2. ONE bullet of context only: last session closing levels (S&P 500, Nasdaq, Dow with %). This is the ONLY backward-looking bullet allowed.
  3. Then ALL remaining bullets must be forward-looking:
     - Scheduled economic data for the target day (with Israel times)
     - Expected earnings reports
     - Geopolitical risks and overnight developments
     - Commodity/currency moves that signal market direction
     - Notable stock catalysts: upgrades, downgrades, news
     - Pre-market/futures data ONLY if the briefing is for today
  4. Do NOT include multiple bullets about what happened yesterday. One context bullet is enough.
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences (NOT bullets).
  - If the briefing is for a same-day trading session: Start with the dominant theme for today's session. What is the key risk, what scenario would change the picture, what level or event to watch.
  - If the briefing is prepared in advance (e.g. Sunday for Monday): Summarize key developments since the last session, what risks are building, and what to watch when trading resumes on the target day. Use "עם פתיחת המסחר ביום {title_day_name}" — NOT "היום".
  - If the target day is NOT a trading day: Start with "אין מסחר" and focus on what to watch for the next session.
  - CRITICAL: The language MUST match the timing. Never say "היום" or "הבוקר" if the trading day hasn't arrived yet.

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

CRITICAL — WEEKLY PERFORMANCE:
- If WEEKLY PERFORMANCE data is provided in the verified market data section above, use those % changes for the weekly index performance. These show the change from previous Friday's close to this Friday's close.
- Do NOT use the DAILY performance numbers for the weekly summary — they only show the last day's change, not the full week.
- Do NOT confuse Friday's daily change with the weekly change. A week can be strongly positive even if Friday was slightly negative.

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

    elif review_type == "live_news":
        return f"""You are a Wall Street news desk editor delivering a real-time market snapshot in Hebrew.

Your task: Based on the tweets/posts below, write a rapid-fire summary of what is happening RIGHT NOW in the markets. This is a "מה קורה עכשיו" update — not a daily summary, not a preview. Just the most important things happening at this moment.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with the news bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: one concise fact." Plain text, no formatting tags.
- Include 6-10 bullets, ordered by importance and recency.
- Cover: current index levels/moves, breaking news, geopolitical developments, notable stock moves, commodity prices, anything market-moving happening RIGHT NOW.
- Be concise — this is a quick snapshot, not a deep analysis.
- The second section is "שורה תחתונה" — 2-3 sentences MAX. What is the dominant story right now and what should investors watch in the next few hours.

{tweets_block}

Output JSON format:
{{"title":"מה קורה עכשיו בוול סטריט 🇺🇸 – {day_name} {date_str}, {datetime.now(ISR_TZ).strftime('%H:%M')}","date":"{date_str}","sections":[{{"heading":"חדשות אחרונות","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

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

        # Remove Google Search grounding citations like [7, 9, 33] or [12]
        import re
        text = re.sub(r'\s*\[\d+(?:,\s*\d+)*\]', '', text)

        # Extract only the JSON object — Gemini sometimes appends HTML or extra text
        # Find the opening { and the matching closing }
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

    # Fetch verified market data from Finnhub
    is_weekly = REVIEW_TYPE in ("weekly_summary", "weekly_prep")
    market_data = fetch_market_data(weekly=is_weekly)

    prompt = get_prompt(tweets, REVIEW_TYPE, date_str, day_name,
                        title_date_str=title_date_str,
                        title_day_name=title_day_name,
                        week_range=week_range,
                        is_trading=target_is_trading if REVIEW_TYPE == "daily_prep" else today_is_trading,
                        market_data=market_data)
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
        "weekly_summary": "weeklySummary",
        "live_news": "liveNews"
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
