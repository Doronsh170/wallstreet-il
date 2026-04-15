import json, os, re, requests
from datetime import datetime, timezone, timedelta

ISR_TZ = timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

# Global storage for validated market data (used by post-processing validation)
_LAST_MARKET_DATA = {"prices": {}, "pcts": {}}

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
    etf_prices = {}  # Track ETF prices for index level calculation
    etf_pcts = {}    # Track % changes for validation
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
                    etf_prices[symbol] = price
                    etf_pcts[symbol] = pct
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

    # Pre-calculate approximate index levels from ETF prices
    INDEX_CALC = {
        "SPY": ("S&P 500", 10),
        "QQQ": ("Nasdaq 100", 40),
        "DIA": ("Dow Jones", 100),
        "GLD": ("Gold ($/oz)", 24),
        "IBIT": ("Bitcoin ($)", 550),
    }
    index_lines = []
    for sym, (name, mult) in INDEX_CALC.items():
        if sym in etf_prices:
            approx = etf_prices[sym] * mult
            if approx > 10000:
                index_lines.append(f"  {name} ≈ {approx:,.0f}")
            else:
                index_lines.append(f"  {name} ≈ ${approx:,.0f}")
            print(f"  Calculated {name}: {approx:,.0f}")

    result_lines.extend([
        "",
        "PRE-CALCULATED INDEX LEVELS (approximate, from ETF prices × multiplier):",
        *index_lines,
        "  NOTE: These are APPROXIMATE. For reporting, use these levels OR verify via Google Search.",
        "  The % changes above are ACCURATE — use them for direction and magnitude.",
        "  If ANY number you write contradicts the data above, you are WRONG. Fix it.",
        "══════════════════════════════════════════════════════════════════════════════════════════\n"
    ])

    # Store validated data for post-processing validation
    global _LAST_MARKET_DATA
    _LAST_MARKET_DATA = {"prices": etf_prices, "pcts": etf_pcts}

    return "\n".join(result_lines)

def fetch_economic_data(days_back=1, days_forward=0):
    """Fetch US economic calendar from Finnhub API.
    For daily reviews: days_back=1, days_forward=0 (today's data)
    For weekly reviews: days_back=7, days_forward=0 (full week)"""
    if not FINNHUB_API_KEY:
        return ""

    now = datetime.now(ISR_TZ)
    from_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_date = (now + timedelta(days=days_forward)).strftime("%Y-%m-%d")

    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/calendar/economic?from={from_date}&to={to_date}&token={FINNHUB_API_KEY}",
            timeout=10
        )
        if not r.ok:
            print(f"  Finnhub economic calendar: status {r.status_code}")
            return ""

        data = r.json()
        events = data.get("economicCalendar", [])

        # Filter: US only, with actual values, medium/high impact
        us_events = []
        for e in events:
            if e.get("country", "") != "US":
                continue
            actual = e.get("actual")
            if actual is None:
                continue
            event_name = e.get("event", "")
            estimate = e.get("estimate")
            prev = e.get("prev")
            unit = e.get("unit", "")
            impact = e.get("impact", "")
            date = e.get("time", "")[:10]

            # Format the line
            line = f"  {date} | {event_name}: actual={actual}{unit}"
            if estimate is not None:
                line += f", forecast={estimate}{unit}"
            if prev is not None:
                line += f", previous={prev}{unit}"
            if impact:
                line += f" [{impact} impact]"
            us_events.append(line)
            print(f"  Econ: {event_name} = {actual}{unit} (est: {estimate})")

        if not us_events:
            print("  No US economic events with actual values found")
            return ""

        return "\n".join([
            "\n══ VERIFIED US ECONOMIC DATA (from Finnhub — these are FACTS, you MUST include them) ══",
            *us_events,
            "INSTRUCTIONS FOR USING THIS DATA:",
            "- Every data point above MUST appear in the review — do NOT skip any.",
            "- Do NOT list them as raw numbers. Weave them naturally into analytical bullets.",
            "- Good example: 'נתוני אינפלציה: מדד המחירים לצרכן (CPI) לחודש מרץ עלה ב-0.9% על בסיס חודשי, מעל הצפי של 0.8%, בעיקר עקב מחירי האנרגיה. מדד הליבה (Core CPI) עלה ב-0.2% בלבד, נמוך מהצפי של 0.3%, מה שמרמז כי ללא אפקט האנרגיה לחצי האינפלציה מתונים יותר.'",
            "- Bad example: 'CPI: actual=0.9%, forecast=0.8%' — this is raw data, not analysis.",
            "- Always explain WHY the number matters: what it means for Fed policy, markets, or investors.",
            "- Do NOT say data 'is expected' or 'will be released' if it already has an actual value above — it was ALREADY released.",
            "══════════════════════════════════════════════════════════════════════════════════════════\n"
        ])

    except Exception as e:
        print(f"  Finnhub economic calendar error: {e}")
        return ""

def get_macro_checklist(review_type, date_str, week_range=None):
    """Generate a mandatory checklist of macro data Gemini must search for."""
    if review_type == "daily_summary":
        return f"""
══ MANDATORY MACRO DATA CHECK ══
You MUST use Google Search to find if ANY of these were released on {date_str}:
- CPI (headline AND Core CPI separately)
- PPI (headline AND Core PPI separately)
- NFP (Non-Farm Payrolls)
- Jobless Claims (initial + continuing)
- Consumer Sentiment (Michigan)
- ISM PMI (Manufacturing or Services)
- GDP
- Retail Sales
- FOMC decision or minutes
If ANY of these were released today, include them with: actual number, forecast, previous, AND what it means for markets/Fed.
If NONE were released today, skip this section — but you MUST check first.
══════════════════════════════════\n"""
    elif review_type == "weekly_summary":
        return f"""
══ MANDATORY MACRO DATA CHECK ══
You MUST use Google Search to find ALL major US economic data released during the week of {week_range if week_range else date_str}.
Specifically search for EACH of these:
1. CPI — was it released this week? If yes: headline monthly %, headline annual %, Core CPI monthly %, Core CPI annual %, vs forecast. BOTH headline and core are mandatory.
2. PPI — was it released this week? If yes: headline and core, monthly and annual, vs forecast.
3. NFP / Employment — was it released this week? If yes: jobs added, unemployment rate, vs consensus, revisions.
4. Jobless Claims — weekly initial claims number, vs forecast, continuing claims.
5. Consumer Sentiment (Michigan) — was it released this week? If yes: actual vs forecast vs previous, inflation expectations.
6. ISM PMI — was it released this week? If yes: manufacturing or services, actual vs forecast.
7. FOMC — was there a decision or minutes released this week?
8. Any other major data release (GDP, Retail Sales, etc.)

For EVERY data point found: include actual number, forecast, previous period, AND explain the market implication.
Do NOT write 'data is expected' if it was already released — check the date.
Do NOT skip Core CPI if headline CPI was released — they are equally important.
══════════════════════════════════\n"""
    elif review_type == "daily_prep":
        return f"""
══ SCHEDULED DATA CHECK ══
Use Google Search to find what US economic data is scheduled for release on {date_str}.
Include the release time in Israel time and what the market consensus/forecast is.
══════════════════════════════════\n"""
    return ""

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
- Use the verified % changes as-is. For exact index point levels, use Google Search to convert ETF prices to index levels (S&P 500 ≈ SPY × 10, Nasdaq 100 ≈ QQQ × ~40, Dow ≈ DIA × ~100).
- You MUST verify via Google Search the current prices of: Brent crude oil, WTI crude oil, gold, and any other commodity you mention.
- If a tweet states a price that seems extreme or unusual, you MUST verify it via Google Search before including it.
- NEVER trust a single tweet for major price data. Always cross-reference.
- NEVER write vague descriptions like "the market closed in green territory" or "mixed trading" without exact numbers.
- NEVER claim an index or stock is at an "all-time high" (שיא / שיא כל הזמנים) unless you verify it via Google Search. A positive day does NOT automatically mean a record.

CRITICAL — MAJOR ECONOMIC DATA (DO NOT MISS):
- Use Google Search to check if any major US economic data was released today: CPI, PPI, NFP, GDP, Jobless Claims, ISM PMI, Consumer Confidence, Retail Sales, FOMC minutes/decision.
- If major data WAS released today, it MUST appear in the review — even if no tweet mentions it. This is non-negotiable.
- CPI and NFP are the two most important data releases. Missing them from a daily review is a critical failure.
- When CPI is mentioned, ALWAYS report BOTH headline CPI AND Core CPI (excluding food and energy). These often tell different stories — headline can be high due to energy while core is tame. Both numbers are critical for understanding Fed policy direction.
- When mentioning economic data, ALWAYS include the actual numbers: percentage change (monthly AND annual), comparison to forecast, and comparison to previous period. For example: "מדד המחירים לצרכן (CPI) עלה ב-0.9% על בסיס חודשי וב-3.3% על בסיס שנתי, מעל הצפי של 3.4%. מדד הליבה (Core CPI) עלה ב-0.2% בלבד, נמוך מהצפי" — NOT just "מדד המחירים הצביע על עלייה חדה". Vague descriptions without numbers are unacceptable.

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

CRITICAL — FACTUAL ACCURACY (ATTRIBUTION):
- NEVER attribute a product, model, or technology to the wrong company. This is a critical error that destroys credibility.
- If a tweet mentions a product, use Google Search to verify WHO made it before writing.
- Common mistakes to avoid:
  * Claude / Claude Mythos is made by ANTHROPIC, not by Amazon/AWS. AWS only hosts it on Bedrock.
  * ChatGPT / GPT models are made by OPENAI, not by Microsoft. Microsoft only invested in OpenAI.
  * Gemini is made by GOOGLE, not by Alphabet directly.
  * A product available ON a platform is NOT made BY that platform.
- When in doubt about who made what, use Google Search to verify. Getting attribution wrong is as bad as getting a number wrong.

CRITICAL — CURRENT POLITICAL LEADERS (DO NOT GET WRONG):
- Donald Trump is the CURRENT President of the United States (inaugurated January 2025). He is NOT a former president. Write "הנשיא טראמפ" or "נשיא ארה"ב טראמפ" — NEVER "הנשיא לשעבר".
- If you are unsure about any political leader's current title, use Google Search to verify. Writing "לשעבר" for a sitting leader is a critical credibility error.

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
- DO NOT include yesterday's index performance, closing levels, or any backward-looking data. ZERO. There is a separate "סיכום יומי" for that.
- DO NOT repeat news or events that already appeared in yesterday's daily summary (סיכום יומי). If something was already covered there, skip it unless there is a NEW development about it.
- ALL bullets must be FORWARD-LOOKING or about NEW overnight developments:
  * What economic data is scheduled for the target trading day (with Israel times)?
  * What earnings reports are expected TODAY and what does the market expect?
  * What NEW geopolitical developments broke overnight?
  * What NEW company news, upgrades/downgrades, or analyst calls came out overnight?
  * Pre-market/futures direction ONLY if the briefing is for today.
- Think of it as: "מה חדש? מה צפוי? מה הסיכונים?" — NOT "מה קרה אתמול?"

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: one concise fact." The sub-heading is a short topic label (2-4 words), plain text, no formatting tags.
- Do NOT use <b> tags or any HTML formatting. Plain text only.
- Include 7-12 bullets. ALL forward-looking or overnight news:
  1. If the briefing is for a future date, first bullet states when trading resumes.
  2. Pre-market/futures sentiment ONLY if the briefing is for today.
  3. Scheduled economic data for the target day (with Israel times and consensus forecast).
  4. Expected earnings reports TODAY.
  5. NEW overnight geopolitical developments.
  6. NEW overnight company news, analyst upgrades/downgrades.
  7. Commodity/currency moves that signal market direction.
- Do NOT include ANY bullets about yesterday's closing levels or yesterday's performance.
- Do NOT repeat information that was in yesterday's daily summary.
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences (NOT bullets).
  - If the briefing is for a same-day trading session: Start with the dominant theme for today's session. What is the key risk, what scenario would change the picture, what level or event to watch.
  - If the briefing is prepared in advance (e.g. Sunday for Monday): Summarize key developments since the last session, what risks are building, and what to watch when trading resumes on the target day. Use "עם פתיחת המסחר ביום {title_day_name}" — NOT "היום".
  - If the target day is NOT a trading day: Start with "אין מסחר" and focus on what to watch for the next session.
  - CRITICAL: The language MUST match the timing. Never say "היום" or "הבוקר" if the trading day hasn't arrived yet.

{tweets_block}

Output JSON format:
{{"title":"נקודות חשובות לקראת פתיחת המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}","date":"{title_date_str}","sections":[{{"heading":"נקודות מרכזיות","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

    elif review_type == "daily_summary":
        return f"""You are a senior Wall Street market analyst writing a comprehensive end-of-day market wrap in Hebrew. Your goal is not just to report what happened, but to explain WHY it matters and WHAT it signals for investors.

Your task: Provide a detailed summary of today's trading session on Wall Street, highlighting critical information for investors. For each development, explain its market significance. Write in PAST TENSE.

{SHARED_RULES}

CRITICAL — ANALYTICAL DEPTH:
- For index performance: include exact % and point levels, note if it's the best/worst day in X period, explain what drove the move.
- For macro data released today: include actual number, forecast, previous, AND explain the market implication (what does it mean for Fed policy, rate expectations, sector impact).
- For stock moves: explain WHY the stock moved, not just the % change.
- For geopolitical events: explain the transmission mechanism — HOW did the event affect prices (e.g., geopolitics → oil → inflation expectations → rate expectations → equity valuations).
- Connect the dots between different developments — don't just list isolated facts.

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: analytical fact." The sub-heading is a short topic label (2-4 words), plain text, no formatting tags.
- Do NOT use <b> tags or any HTML formatting. Plain text only.
- Include 7-12 bullets, ordered by market impact:
  1. Index performance (S&P 500, Nasdaq, Dow with %, point levels, context).
  2. Macro data released today with FULL numbers (actual vs forecast vs previous) and market reaction.
  3. Key market-moving events: geopolitics, Fed comments, trade news — with cause-and-effect.
  4. Commodities and currencies: oil, gold, Bitcoin, VIX — with % and explanation.
  5. Notable stock moves with WHY ($TICKER +/- %, what caused it).
  6. Sector rotation or institutional activity if relevant.
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences. Structure:
  1. The key narrative of today's session — what was the dominant force.
  2. What shifted in investor positioning (risk-on/off, sector preference, rate expectations).
  3. The key tension or contradiction in today's price action.
  4. What specific data, event, or level to watch tomorrow and why it matters.

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
- Do NOT include last week's index performance or closing levels — there is a separate "סיכום שבועי" for that.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet format: "* Sub-heading: detail text." — the sub-heading is a short topic label (2-4 words), NOT bold, NOT wrapped in tags.
- Do NOT use <b> tags, ■ markers, 📍 emojis, or any special formatting. Plain text only.
- Include 8-14 bullets, ALL forward-looking:
  1. Key events coming THIS week: Fed decisions, economic data (NFP, CPI, PMI, GDP, PPI), earnings reports, trade/tariff deadlines, geopolitical developments.
  2. For each event: include the specific day and Israel time when known (e.g. "יום רביעי 21:00 שעון ישראל").
  3. Geopolitical risks and what to watch for.
  4. Notable companies expected to report earnings this week.
- Do NOT include any bullets about last week's performance. Zero backward-looking data.
- The second section is "שורה תחתונה" — a paragraph of 4-5 sentences (NOT bullets). Start with the ONE dominant theme for the week. Then analyze: what is the market currently pricing in, what could surprise to the upside or downside, and what combination of events could trigger a significant move. End with a clear framework — what scenario is bullish and what is bearish.

{tweets_block}

Output JSON format:
{{"title":"הכנה לשבוע מסחר בוול סטריט 🇺🇸 – {week_range if week_range else date_str}","date":"{date_str}","sections":[{{"heading":"נקודות מרכזיות לשבוע הקרוב","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

    elif review_type == "weekly_summary":
        return f"""You are a senior Wall Street strategist writing a comprehensive weekly review for Israeli investors in Hebrew. Your goal is to highlight critical information for investors — not just what happened, but WHY it matters and WHAT it means for positioning.

Your task: Provide a detailed summary of all significant developments on Wall Street over the trading week of {week_range if week_range else date_str}. For each development, explain its market significance and investor implications. Write in PAST TENSE.

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

CRITICAL — ANALYTICAL DEPTH:
- For EVERY macro data point (CPI, NFP, GDP, PMI, etc.), include: the actual number, the forecast/consensus, the comparison to previous period, AND what it means for Fed policy and markets.
- For index performance: include weekly % change, mention if it's the best/worst week in X months, and note which sectors led and which lagged.
- For geopolitical events: explain the market mechanism — HOW did the event move prices (oil → inflation expectations → rate expectations → equity valuations).
- For earnings: note the broader trend — what does it signal about the sector/economy, not just the individual company.
- Always connect the dots: don't just list facts, explain the cause-and-effect chain.

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with all the bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet format: "* Sub-heading: detail text." — the sub-heading is a short topic label (2-4 words), NOT bold, NOT wrapped in tags.
- Do NOT use <b> tags, ■ markers, 📍 emojis, or any special formatting. Plain text only.
- Include 8-14 bullets, in this order:
  1. START with index performance: S&P 500, Nasdaq, Dow, Russell 2000 — weekly % changes, context (best week since X), leading/lagging sectors, total market cap change if notable.
  2. Then macro data published this week with FULL numbers: CPI (headline AND core, monthly AND annual, vs forecast), NFP (actual vs consensus, revisions), jobless claims, consumer sentiment — actual numbers, forecasts, AND market reaction.
  3. Then key events that moved markets: geopolitics, Fed comments, trade/tariff news — explain the transmission mechanism to prices.
  4. Then commodities with context: oil (weekly change + why), gold, Bitcoin.
  5. Then notable company news, earnings, M&A — combine related items.
  6. End with earnings season outlook or institutional positioning if relevant.
- Do NOT repeat information across bullets. Each bullet = one unique fact.
- The second section is "שורה תחתונה" — a paragraph of 5-6 sentences. Structure:
  1. The dominant narrative of the week and what drove it.
  2. What shifted in investor positioning (risk-on/off, sector rotation, rate expectations).
  3. The key tension or contradiction in the market (e.g. strong jobs vs high inflation).
  4. What are the 2-3 specific risks that could reverse the trend.
  5. What to watch next week and why it matters for the medium-term outlook.

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
        now_time = datetime.now(ISR_TZ).strftime('%H:%M')
        return f"""You are a Wall Street news desk editor delivering a real-time news update in Hebrew.

Your task: Based on the tweets/posts below, write a rapid-fire summary of the most important events and developments happening RIGHT NOW that are relevant to Wall Street investors. This is a "מה קורה עכשיו" update — just the news, no market data or index levels.

{SHARED_RULES}

CRITICAL — OUTPUT FORMAT:
- Output exactly 2 JSON sections: one with the news bullets, one with the bottom line.
- The first section's "content" field MUST be a flat list of bullet points. Every bullet starts with "* " (asterisk + space).
- Each bullet: "* Sub-heading: one concise fact." Plain text, no formatting tags.
- Include 6-10 bullets of NEWS AND EVENTS ONLY:
  * Breaking news and developments
  * Geopolitical events affecting markets
  * Notable company news, deals, earnings surprises
  * Fed/central bank comments or actions
  * Regulatory developments
  * Analyst calls or institutional moves
- Do NOT include index levels, % changes, commodity prices, or VIX. Only events and news.
- Order by importance and recency.
- Be concise — this is a quick snapshot, not a deep analysis.
- The second section is "שורה תחתונה" — 2-3 sentences MAX. What is the dominant story right now and what should investors watch in the next few hours.

{tweets_block}

Output JSON format:
{{"title":"מה קורה עכשיו בוול סטריט 🇺🇸 – יום {day_name}, {date_str} | {now_time}","date":"{date_str}","sections":[{{"heading":"חדשות אחרונות","content":"* sub-heading: fact\\n* sub-heading: fact..."}},{{"heading":"שורה תחתונה","content":"paragraph"}}]}}"""

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
                    "temperature": 0.35,
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
            print(f"  Raw text (first 200 chars): {text[:200]}")
            print(f"  Raw text (last 300 chars): ...{text[-300:]}")
            if attempt < max_retries - 1:
                print(f"  Retrying due to JSON error in 30s...")
                time.sleep(30)
                continue
            raise

    raise Exception("call_gemini: exhausted all retries")

# ══════════════════════════════════════════════════════════════
# POST-PROCESSING VALIDATION & AUTO-FIX
# ══════════════════════════════════════════════════════════════

# ── Known text errors: (pattern, replacement, description) ──
TEXT_FIXES = [
    # Political leaders — current titles
    (r'הנשיא\s+לשעבר\s+טראמפ', 'הנשיא טראמפ', 'Trump is the current president'),
    (r'נשיא\s+ארה"ב\s+לשעבר\s+טראמפ', 'נשיא ארה"ב טראמפ', 'Trump is the current president'),
    (r'הנשיא\s+לשעבר\s+דונלד\s+טראמפ', 'הנשיא דונלד טראמפ', 'Trump is the current president'),
    (r'טראמפ\s*,?\s*הנשיא\s+לשעבר', 'טראמפ, הנשיא', 'Trump is the current president'),
    (r'הנשיא\s+ביידן', 'הנשיא לשעבר ביידן', 'Biden is the FORMER president'),
    (r'נשיא\s+ארה"ב\s+ביידן', 'הנשיא לשעבר ביידן', 'Biden is the FORMER president'),
    # Common terminology mistakes
    (r'הנפקה\s+ראשונית\s+לציבור\s*\(ETF\)', 'תעודת סל (ETF)', 'IPO ≠ ETF'),
    (r'תעודת\s+סל\s*\(IPO\)', 'הנפקה ראשונית (IPO)', 'ETF ≠ IPO'),
    # Attribution mistakes
    (r'אמזון\s+השיקה?\s+את\s+Claude', 'Anthropic השיקה את Claude', 'Claude is by Anthropic'),
    (r'מיקרוסופט\s+השיקה?\s+את\s+ChatGPT', 'OpenAI השיקה את ChatGPT', 'ChatGPT is by OpenAI'),
    (r'AWS\s+השיקה?\s+את\s+Claude', 'Anthropic השיקה את Claude', 'Claude is by Anthropic'),
]

# ── Index sanity ranges (approximate, updated periodically) ──
INDEX_RANGES = {
    # (Hebrew pattern, min_reasonable, max_reasonable, description)
    r'(?:S&P\s*500|אס[\-&]?אנד[\-]?פי)\s*[\-–:]\s*([\d,\.]+)': (4000, 7000, 'S&P 500'),
    r'(?:נסדק|נאסד"ק|Nasdaq)\s*(?:100|הקומפוזיט|Composite)?\s*[\-–:]\s*([\d,\.]+)': (12000, 25000, 'Nasdaq'),
    r'(?:דאו\s*ג\'?ונס|Dow\s*Jones?|DJIA)\s*[\-–:]\s*([\d,\.]+)': (30000, 50000, 'Dow Jones'),
    r'(?:ראסל|Russell)\s*2000\s*[\-–:]\s*([\d,\.]+)': (1500, 3500, 'Russell 2000'),
}

# ── Percentage sanity: daily % change should not exceed these ──
PCT_MAX_DAILY = 8.0   # Flag if daily index move > 8%
PCT_MAX_WEEKLY = 15.0  # Flag if weekly index move > 15%


def validate_and_fix(result, review_type):
    """
    Post-process Gemini output: fix known text errors, validate numbers.
    Returns (fixed_result, warnings_list).
    """
    warnings = []
    fix_count = 0

    def process_text(text):
        nonlocal fix_count
        if not isinstance(text, str):
            return text

        original = text

        # ── Apply known text fixes ──
        for pattern, replacement, desc in TEXT_FIXES:
            new_text = re.sub(pattern, replacement, text)
            if new_text != text:
                fix_count += 1
                warnings.append(f"AUTO-FIXED: {desc}")
                print(f"  ✅ Auto-fixed: {desc}")
                text = new_text

        # ── Validate index levels ──
        for idx_pattern, (lo, hi, name) in INDEX_RANGES.items():
            for match in re.finditer(idx_pattern, text):
                raw_num = match.group(1).replace(',', '').replace('.', '')
                try:
                    val = float(raw_num)
                    if val < lo or val > hi:
                        warn = f"SUSPICIOUS NUMBER: {name} = {raw_num} (expected range {lo:,}-{hi:,})"
                        warnings.append(warn)
                        print(f"  ⚠️  {warn}")

                        # Try to auto-fix using Finnhub data if available
                        etf_map = {'S&P 500': 'SPY', 'Nasdaq': 'QQQ', 'Dow Jones': 'DIA', 'Russell 2000': 'IWM'}
                        etf_sym = etf_map.get(name)
                        multipliers = {'SPY': 10, 'QQQ': 40, 'DIA': 100, 'IWM': 1}
                        if etf_sym and etf_sym in _LAST_MARKET_DATA.get("prices", {}):
                            correct_approx = _LAST_MARKET_DATA["prices"][etf_sym] * multipliers.get(etf_sym, 1)
                            if lo <= correct_approx <= hi:
                                old_str = match.group(0)
                                new_num = f"{correct_approx:,.0f}"
                                new_str = old_str.replace(match.group(1), new_num)
                                text = text.replace(old_str, new_str, 1)
                                fix_count += 1
                                fix_warn = f"AUTO-FIXED: {name} {raw_num} → {new_num}"
                                warnings.append(fix_warn)
                                print(f"  ✅ {fix_warn}")
                except (ValueError, IndexError):
                    pass

        # ── Validate percentage claims ──
        pct_pattern = r'(?:עלייה|ירידה|עלה|ירד|זינק|צנח|איבד|הוסיף|קפץ)\s+(?:של?\s*)?(?:כ[--]?)?([\d\.]+)%'
        for match in re.finditer(pct_pattern, text):
            try:
                pct_val = float(match.group(1))
                # Check if this is about a major index (not individual stocks)
                # Look at surrounding context (30 chars before)
                start = max(0, match.start() - 60)
                context = text[start:match.start()].lower()
                is_index = any(idx in context for idx in ['s&p', 'נסדק', 'נאסד"ק', 'nasdaq', 'דאו', 'dow', 'ראסל', 'russell'])
                if is_index and review_type in ('daily_prep', 'daily_summary') and pct_val > PCT_MAX_DAILY:
                    warn = f"SUSPICIOUS: Index daily move of {pct_val}% exceeds {PCT_MAX_DAILY}% threshold"
                    warnings.append(warn)
                    print(f"  ⚠️  {warn}")
                elif is_index and review_type in ('weekly_prep', 'weekly_summary') and pct_val > PCT_MAX_WEEKLY:
                    warn = f"SUSPICIOUS: Index weekly move of {pct_val}% exceeds {PCT_MAX_WEEKLY}% threshold"
                    warnings.append(warn)
                    print(f"  ⚠️  {warn}")
            except ValueError:
                pass

        return text

    # ── Walk the JSON structure and fix all text fields ──
    if isinstance(result, dict):
        # Fix title
        if "title" in result and isinstance(result["title"], str):
            result["title"] = process_text(result["title"])

        # Fix sections
        for section in result.get("sections", []):
            if "content" in section:
                if isinstance(section["content"], str):
                    section["content"] = process_text(section["content"])
                elif isinstance(section["content"], list):
                    section["content"] = [process_text(item) if isinstance(item, str) else item for item in section["content"]]
            if "heading" in section and isinstance(section["heading"], str):
                section["heading"] = process_text(section["heading"])

        # Fix events items
        for item in result.get("items", []):
            if "title" in item:
                item["title"] = process_text(item["title"])
            if "description" in item:
                item["description"] = process_text(item["description"])

    if warnings:
        print(f"\n  📋 Validation summary: {fix_count} auto-fixes, {len(warnings)} total warnings")
        for w in warnings:
            print(f"     • {w}")
    else:
        print("  ✅ Validation passed — no issues found")

    return result, warnings


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

    # Fetch economic calendar data from Finnhub
    if REVIEW_TYPE == "daily_summary":
        econ_data = fetch_economic_data(days_back=1, days_forward=0)
    elif REVIEW_TYPE in ("weekly_summary", "weekly_prep"):
        econ_data = fetch_economic_data(days_back=7, days_forward=0)
    elif REVIEW_TYPE == "daily_prep":
        econ_data = fetch_economic_data(days_back=1, days_forward=1)
    elif REVIEW_TYPE == "live_news":
        econ_data = fetch_economic_data(days_back=1, days_forward=0)
    else:
        econ_data = ""

    # Combine market data and economic data
    if econ_data:
        market_data = market_data + "\n" + econ_data if market_data else econ_data

    # Add mandatory macro data checklist
    macro_checklist = get_macro_checklist(REVIEW_TYPE, date_str, week_range)
    if macro_checklist:
        market_data = market_data + "\n" + macro_checklist if market_data else macro_checklist

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

    # ── Post-processing validation & auto-fix ──
    print("\n── Running post-processing validation ──")
    result, validation_warnings = validate_and_fix(result, REVIEW_TYPE)
    if validation_warnings:
        print(f"  ⚠️  {len(validation_warnings)} issue(s) found and handled")
    print("── Validation complete ──\n")

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
