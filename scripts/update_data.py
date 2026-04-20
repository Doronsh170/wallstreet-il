import json, os, re, requests
from datetime import datetime, timezone, timedelta

ISR_TZ = timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

PY_TO_HEB = {0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}

# ══════════════════════════════════════════════════════════════
# EXPECTED STRUCTURE — single source of truth for output format
# ══════════════════════════════════════════════════════════════

EXPECTED_FIRST_HEADING = {
    "daily_prep":     "נקודות מרכזיות",
    "daily_summary":  "סיכום המסחר",
    "weekly_prep":    "נקודות מרכזיות לשבוע הקרוב",
    "weekly_summary": "סיכום השבוע",
    "live_news":      "חדשות אחרונות",
}

def build_expected_title(review_type, title_day_name, title_date_str, week_range=None, now_time=None):
    """Build the exact title string we expect the output to have.
    This is enforced post-hoc in enforce_structure(), overriding whatever Gemini returned."""
    if review_type == "daily_prep":
        return f"נקודות חשובות לקראת פתיחת המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}"
    elif review_type == "daily_summary":
        return f"סיכום יום המסחר בוול סטריט 🇺🇸 – יום {title_day_name} {title_date_str}"
    elif review_type == "weekly_prep":
        return f"הכנה לשבוע מסחר בוול סטריט 🇺🇸 – {week_range}"
    elif review_type == "weekly_summary":
        return f"סיכום שבוע המסחר בוול סטריט 🇺🇸 – {week_range}"
    elif review_type == "live_news":
        return f"מה קורה עכשיו בוול סטריט 🇺🇸 – יום {title_day_name}, {title_date_str} | {now_time}"
    return ""

_LAST_MARKET_DATA = {"prices": {}, "pcts": {}}

# ══════════════════════════════════════════════════════════════
# MARKET DATA — FINNHUB
# ══════════════════════════════════════════════════════════════

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
        # Sector ETFs
        "XLE": "Energy Sector (XLE ETF)",
        "XLK": "Technology Sector (XLK ETF)",
        "XLF": "Financials Sector (XLF ETF)",
        "XLY": "Consumer Discretionary Sector (XLY ETF)",
        "XLV": "Healthcare Sector (XLV ETF)",
        "XLI": "Industrials Sector (XLI ETF)",
        "XLP": "Consumer Staples Sector (XLP ETF)",
        "XLU": "Utilities Sector (XLU ETF)",
        # Commodities
        "USO": "WTI Crude Oil (USO ETF)",
        "BNO": "Brent Crude Oil (BNO ETF)",
        "GLD": "Gold (GLD ETF)",
        "SLV": "Silver (SLV ETF)",
        # Crypto
        "IBIT": "Bitcoin (IBIT ETF)",
        # Bonds
        "TLT": "US 20Y+ Bonds (TLT ETF)",
        # Dollar
        "UUP": "US Dollar (UUP ETF)",
        # VIX proxy
        "VIXY": "VIX Volatility (VIXY ETF)",
    }

    # Daily quotes
    lines = []
    etf_prices = {}
    etf_pcts = {}
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

        key_symbols = ["SPY", "QQQ", "DIA", "IWM", "XLE", "XLK", "XLF", "XLY", "XLV", "USO", "BNO", "GLD", "IBIT", "TLT"]
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
                        from datetime import datetime as dt_cls
                        dated_closes = []
                        for i, ts in enumerate(timestamps):
                            date = dt_cls.utcfromtimestamp(ts)
                            dated_closes.append((date, closes[i]))

                        current_week = []
                        prev_week = []
                        today = dt_cls.utcnow()
                        for date, close in dated_closes:
                            if date.isocalendar()[1] == today.isocalendar()[1]:
                                current_week.append((date, close))
                            elif date.isocalendar()[1] == today.isocalendar()[1] - 1 or \
                                 (today.isocalendar()[1] == 1 and date.isocalendar()[1] >= 52):
                                prev_week.append((date, close))

                        if not current_week and prev_week:
                            current_week = prev_week
                            prev_week = []

                        if current_week and len(dated_closes) > len(current_week):
                            week_close = current_week[-1][1]
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
        "The % changes above are ACCURATE — use them for direction and magnitude.",
        "For exact index LEVELS (points), gold price ($/oz), oil price ($/barrel), VIX level, and Bitcoin price: ALWAYS use Google Search. Do NOT calculate or estimate them from ETF prices.",
        "For sector performance (XLE/XLK/XLF/XLY/XLV/XLI/XLP/XLU): USE ONLY the Finnhub numbers above. Do NOT invent sector percentages.",
        "For 10-year Treasury yield: use Google Search to verify the current level — do NOT estimate from TLT price.",
        "If ANY percentage you write contradicts the data above, you are WRONG. Fix it.",
        "══════════════════════════════════════════════════════════════════════════════════════════\n"
    ])

    global _LAST_MARKET_DATA
    _LAST_MARKET_DATA = {"prices": etf_prices, "pcts": etf_pcts}

    return "\n".join(result_lines)

def fetch_economic_data(days_back=1, days_forward=0):
    """Fetch US economic calendar from Finnhub API."""
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

        us_events = []
        for e in events:
            if e.get("country", "") != "US":
                continue
            actual = e.get("actual")
            event_name = e.get("event", "")
            estimate = e.get("estimate")
            prev = e.get("prev")
            unit = e.get("unit", "")
            impact = e.get("impact", "")
            date = e.get("time", "")[:10]

            # For forward-looking reviews, we want scheduled events (actual=None is fine)
            # For backward-looking, we only want events with actuals
            if days_forward == 0 and actual is None:
                continue

            if actual is not None:
                line = f"  {date} | {event_name}: actual={actual}{unit}"
                if estimate is not None:
                    line += f", forecast={estimate}{unit}"
                if prev is not None:
                    line += f", previous={prev}{unit}"
            else:
                line = f"  {date} | {event_name}: SCHEDULED"
                if estimate is not None:
                    line += f", forecast={estimate}{unit}"
                if prev is not None:
                    line += f", previous={prev}{unit}"
            if impact:
                line += f" [{impact} impact]"
            us_events.append(line)
            print(f"  Econ: {event_name} (actual={actual}, est={estimate})")

        if not us_events:
            print("  No relevant US economic events found")
            return ""

        header = "VERIFIED US ECONOMIC DATA (released)" if days_forward == 0 else "SCHEDULED US ECONOMIC CALENDAR (upcoming)"
        return "\n".join([
            f"\n══ {header} (from Finnhub — these are FACTS, you MUST include them) ══",
            *us_events,
            "INSTRUCTIONS FOR USING THIS DATA:",
            "- Every data point above MUST appear in the review — do NOT skip any.",
            "- Do NOT list them as raw numbers. Weave them naturally into analytical bullets.",
            "- Good example: 'נתוני אינפלציה: מדד המחירים לצרכן (CPI) לחודש מרץ עלה ב-0.9% על בסיס חודשי, מעל הצפי של 0.8%, בעיקר עקב מחירי האנרגיה. מדד הליבה (Core CPI) עלה ב-0.2% בלבד, נמוך מהצפי של 0.3%.'",
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
    elif review_type == "weekly_prep":
        return f"""
══ SCHEDULED DATA CHECK ══
Use Google Search to find ALL major US economic data SCHEDULED for release during the week of {week_range if week_range else date_str}.
For each: day of week, release time in Israel time, consensus forecast, and previous reading.
══════════════════════════════════\n"""
    return ""

# ══════════════════════════════════════════════════════════════
# TRADING DAY / DATE HELPERS
# ══════════════════════════════════════════════════════════════

def load_holidays():
    """Load US holidays from data.json"""
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("marketStatus", {}).get("usHolidays2026", [])
    except:
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
    if is_trading_day(now, holidays):
        hour = now.hour
        if hour >= 23:
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

# ══════════════════════════════════════════════════════════════
# TWEETS
# ══════════════════════════════════════════════════════════════

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
# CONTEXT INJECTION — previous reviews (avoid duplication)
# ══════════════════════════════════════════════════════════════

def get_prior_review_context(review_type, data):
    """Inject yesterday's/last week's review so Gemini doesn't repeat the same news."""
    if review_type == "daily_prep":
        prior = data.get("dailySummary")
        if prior and prior.get("sections"):
            sections = prior["sections"]
            content = "\n\n".join(
                f"[{s.get('heading', '')}]\n{s.get('content', '')}"
                for s in sections
            )
            return f"""
══ CONTEXT: YESTERDAY'S DAILY SUMMARY — DO NOT REPEAT THIS CONTENT ══
The text below was already published yesterday. Your briefing is FORWARD-LOOKING.
Do NOT re-describe events, news items, or market moves that already appear below.
Mention something from yesterday ONLY if there is a genuinely NEW development about it overnight.

{content}
══════════════════════════════════════════════════════════════════════════════
"""
    elif review_type == "weekly_prep":
        prior = data.get("weeklySummary")
        if prior and prior.get("sections"):
            sections = prior["sections"]
            content = "\n\n".join(
                f"[{s.get('heading', '')}]\n{s.get('content', '')}"
                for s in sections
            )
            return f"""
══ CONTEXT: LAST WEEK'S SUMMARY — DO NOT REPEAT THIS CONTENT ══
The text below was already published at the end of last week. Your preview is FORWARD-LOOKING.
Do NOT recap last week's performance, events, or moves — those are in the weekly summary.
Focus ENTIRELY on what is scheduled and what to watch in the week ahead.

{content}
══════════════════════════════════════════════════════════════════════════════
"""
    elif review_type == "daily_summary":
        prior = data.get("dailyPrep")
        if prior and prior.get("sections"):
            sections = prior["sections"]
            content = "\n\n".join(
                f"[{s.get('heading', '')}]\n{s.get('content', '')}"
                for s in sections
            )
            return f"""
══ CONTEXT: THIS MORNING'S PRE-MARKET BRIEFING ══
The text below was published before today's trading session. Reference it to resolve scheduled items
(e.g. "CPI was expected at 15:30 — actual came in at X") but do NOT quote the briefing verbatim.

{content}
══════════════════════════════════════════════════════════════════════════════
"""
    return ""

# ══════════════════════════════════════════════════════════════
# TIME CONVERSION (US-IL)
# ══════════════════════════════════════════════════════════════

def get_us_israel_offset(now):
    year = now.year
    mar1 = datetime(year, 3, 1)
    us_dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7)
    nov1 = datetime(year, 11, 1)
    us_dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)
    apr2 = datetime(year, 4, 2)
    il_dst_start = apr2 - timedelta(days=(apr2.weekday() + 3) % 7)
    oct31 = datetime(year, 10, 31)
    il_dst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)

    today = now.replace(tzinfo=None)
    us_is_dst = us_dst_start <= today.replace(hour=0, minute=0, second=0) < us_dst_end
    il_is_dst = il_dst_start <= today.replace(hour=0, minute=0, second=0) < il_dst_end

    us_offset = -4 if us_is_dst else -5
    il_offset = 3 if il_is_dst else 2

    return il_offset - us_offset

def get_time_conversion_block(now):
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

# ══════════════════════════════════════════════════════════════
# PROMPTS
# ══════════════════════════════════════════════════════════════

def get_prompt(tweets, review_type, date_str, day_name, title_date_str=None, title_day_name=None,
               week_range=None, is_trading=True, market_data="", prior_context="", expected_title="",
               window_start=None):
    if not title_date_str:
        title_date_str = date_str
    if not title_day_name:
        title_day_name = day_name

    tweets_block = f"Source tweets/posts from X (Twitter) — date: {date_str}:\n{tweets}"
    if market_data:
        tweets_block = market_data + "\n" + tweets_block
    if prior_context:
        tweets_block = prior_context + "\n" + tweets_block

    from datetime import datetime as dt_class
    time_block = get_time_conversion_block(dt_class.now(ISR_TZ))
    tweets_block = time_block + "\n" + tweets_block

    # ══════════════════════════════════════════════════════════════
    # daily_prep
    # ══════════════════════════════════════════════════════════════
    if review_type == "daily_prep":
        is_same_day = (date_str == title_date_str)
        if is_trading:
            if is_same_day:
                trading_status = """The briefing is for TODAY — a regular trading day.

⚠️ CRITICAL TENSE RULE — THE MARKET HAS NOT OPENED YET:
This briefing is written BEFORE the US market opens (16:30 Israel time).
- You MAY use 'היום' to refer to today's date.
- You MAY use 'הבוקר' for overnight news and pre-market data.
- ❌ You MUST NOT describe the US market itself as already open, already trading, or having reacted.
- ❌ FORBIDDEN phrases: 'השוק נפתח הבוקר לסנטימנט...', 'המסחר התנהל...', 'המדד פתח בעלייה', 'המשקיעים הגיבו ב...'
- ✅ REQUIRED phrases: 'השוק צפוי להיפתח...', 'עם פתיחת המסחר...', 'המשקיעים יעקבו אחר...', 'התגובה הצפויה...'
- Futures trading is pre-market and CAN be described in present tense ('החוזים נסחרים בעלייה'). The cash market cannot."""
            else:
                trading_status = f"IMPORTANT: This script runs on {date_str} ({day_name}) but the briefing is for the NEXT trading day: {title_date_str} ({title_day_name}). Do NOT use 'היום' or 'הבוקר' — use 'ביום {title_day_name}' or 'בפתיחת המסחר ביום {title_day_name}' instead. Do NOT mention futures or pre-market data as if they are live right now."
        else:
            trading_status = f"The target date {title_date_str} is NOT a trading day (weekend or US holiday). State this clearly in the first bullet."

        return f"""Summarize in bullets what investors must know before the market opens today on Wall Street, based on the source posts from X provided below.

DATES:
- Script run date: {date_str} ({day_name})
- Briefing target date: {title_date_str} ({title_day_name})
- {trading_status}

RULES:
- Output in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and financial terms in parentheses on first use.
- Source hierarchy: (1) Finnhub verified data above, (2) the X posts below, (3) Google Search for verification. Do NOT invent items that have no source.
- Forward-looking only. Do NOT include yesterday's index performance, closing levels, or any backward-looking data.
- Do not repeat items already covered in the prior_context block above.
- Every bullet needs specifics: a number, %, ticker, or Israel time. No vague claims.
- Combine related items about the same company into one bullet.
- No buy/sell recommendations.
- Output pure JSON only — no backticks, no explanations.

DATA ACCURACY:
- For % changes on indices / ETFs / sectors: use ONLY the Finnhub verified data above. Never override with memory or tweets.
- For exact index point levels, oil & gold $/unit prices, VIX level, 10Y yield: verify via Google Search. Do NOT estimate from ETF prices.
- For tweet-sourced numbers that look extreme or unusual: cross-check via Google Search before including.
- When reporting economic data: always include actual, forecast, and previous. CPI: always include BOTH headline and core.
- Never claim an all-time high without Google Search verification.
- If a number cannot be verified: omit it.
- Do not attribute products to the wrong company (e.g. Claude is by Anthropic, not AWS; ChatGPT by OpenAI; Gemini by Google).

US-ISRAEL TIME:
Use ONLY the offsets in the time_block above. Never calculate your own offset.

OUTPUT FORMAT (MANDATORY):
- Output EXACTLY 1 section in the "sections" array.
- Section heading MUST be EXACTLY "נקודות מרכזיות" (no variations, no emojis).
- The "title" field MUST be EXACTLY: "{expected_title}"
- "content": a list of bullet points, each on its own line starting with "* " (asterisk + space).
- Each bullet: "* Short topic label: one concise analytical sentence with numbers."
- Do NOT use <b>, <strong>, **, or any HTML/markdown formatting inside content.
- Do NOT add a "שורה תחתונה" section, a summary paragraph, or any second section.

COVERAGE — include 8-14 bullets in priority order:
1. Scheduled US economic data today: Israel time + consensus forecast + previous.
2. Earnings reports scheduled today (pre-open or after-close).
3. NEW overnight developments: geopolitics, regulatory actions, major company news, analyst upgrades/downgrades.
4. Pre-market movers (>3% gap) — only if the catalyst is clear.
5. Commodity / FX / yield moves signaling macro positioning.
6. Fed speakers or secondary catalysts.
If today has no major catalyst, state this explicitly in the first bullet ("יום דל קטליזטורים — המוקד יהיה ...").

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections."""

    # ══════════════════════════════════════════════════════════════
    # daily_summary
    # ══════════════════════════════════════════════════════════════
    elif review_type == "daily_summary":
        return f"""Summarize in bullets the trading day that concluded today on Wall Street for investors, with an emphasis on all the important events that occurred, based on the provided sources from X.

DATES:
- Script run date: {date_str} ({day_name})
- Summary target date: {title_date_str} ({title_day_name})

RULES:
- Output in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and financial terms in parentheses on first use.
- Source hierarchy: (1) Finnhub verified data above, (2) the X posts below, (3) Google Search for verification. Do NOT invent items that have no source.
- Write in PAST TENSE — the trading session has ended.
- Backward-looking. Describe what happened. Do NOT speculate on tomorrow.
- Do not repeat items already covered in the prior_context block above.
- Every bullet needs specifics: a number, %, ticker, or time. No vague claims.
- Combine related items about the same company into one bullet.
- No buy/sell recommendations.
- Output pure JSON only — no backticks, no explanations.

DATA ACCURACY:
- For daily % changes on indices / ETFs / sectors: use ONLY the Finnhub verified data above. These are authoritative — never override with tweets or memory.
- For exact index closing point levels, oil & gold $/unit prices, VIX level, 10Y yield: verify via Google Search. Do NOT estimate from ETF prices.
- For tweet-sourced numbers that look extreme or unusual: cross-check via Google Search before including.
- When reporting economic data: always include actual, forecast, and previous. CPI: always include BOTH headline and core.
- Never claim an all-time high or record low without Google Search verification.
- If a number cannot be verified: omit it.
- Do not attribute products to the wrong company (e.g. Claude is by Anthropic, not AWS; ChatGPT by OpenAI; Gemini by Google).

US-ISRAEL TIME:
Use ONLY the offsets in the time_block above. Never calculate your own offset.

ANALYTICAL DEPTH — every bullet must answer:
(a) WHAT happened (with specific numbers).
(b) WHY it happened (catalyst or cause).
(c) SO WHAT — the market implication or broader signal.
A bullet that only reports a fact without explanation is insufficient.

OUTPUT FORMAT (MANDATORY):
- Output EXACTLY 1 section in the "sections" array.
- Section heading MUST be EXACTLY "סיכום המסחר" (no variations, no emojis).
- The "title" field MUST be EXACTLY: "{expected_title}"
- "content": a list of bullet points, each on its own line starting with "* " (asterisk + space).
- Each bullet: "* Short topic label: one concise analytical sentence with numbers."
- Do NOT use <b>, <strong>, **, or any HTML/markdown formatting.
- Do NOT add a "שורה תחתונה" section, a summary paragraph, or any second section.

COVERAGE — include 8-14 bullets in priority order:
1. Index performance (MANDATORY first bullet): S&P 500, Nasdaq, Dow, Russell 2000 — daily % from Finnhub + closing point levels (Google Search for the levels).
2. Sector rotation: leading and lagging sectors using ONLY the Finnhub XLE/XLK/XLF/XLY/XLV/XLI/XLP/XLU % data.
3. Macro data released today: actual vs forecast vs previous, with market reaction.
4. Top stock movers today ($TICKER +/- %, with the specific catalyst that drove the move).
5. Earnings reports that moved stocks today.
6. Commodities: oil, gold, Bitcoin — % changes and the catalyst.
7. Yields & FX: 10Y yield, DXY — direction + what drove them.
8. Fed commentary, geopolitical events, trade news that affected the session.
9. Notable after-hours movers if a major company reported after close.

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections."""

    # ══════════════════════════════════════════════════════════════
    # weekly_prep
    # ══════════════════════════════════════════════════════════════
    elif review_type == "weekly_prep":
        return f"""Summarize in bullets what investors must know before the start of the new trading week in Wall Street, including upcoming key events, important earnings reports, and everything essential for investors to know, based on the sources from X.

DATES:
- Script run date: {date_str} ({day_name})
- Target trading week: {week_range}

RULES:
- Output in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and financial terms in parentheses on first use.
- Source grounding: CONTENT (events, companies, earnings, narratives) must come from (a) the X posts below, or (b) the economic calendar provided in market_data above. Do NOT invent events, companies, or catalysts not represented in these inputs. Do NOT use Google Search to introduce new events.
- Google Search is allowed ONLY to verify specific times, consensus forecasts, or earnings dates for items already in the X posts or calendar.
- Write in FUTURE TENSE — the week has not started.
- Forward-looking ONLY. Do NOT include last week's index performance, closing levels, or any backward-looking data. The weekly_summary covers that.
- Do not repeat items already covered in the prior_context block above.
- Every bullet needs specifics: a day, Israel time, number, %, or ticker. No vague claims.
- Combine related items about the same company into one bullet.
- No buy/sell recommendations.
- Output pure JSON only — no backticks, no explanations.

TIME FRAME (STRICT):
- This preview covers the trading week of {week_range} ONLY.
- Include ONLY events, data releases, and catalysts scheduled for this specific week.
- Do NOT include events from the previous week or beyond this week's Friday.

DATA ACCURACY:
- For scheduled event times: use the Israel time offsets from the time_block above. Never calculate your own offset.
- For macro consensus/forecast: use the figures from the economic calendar block above, or verify via Google Search if missing.
- For scheduled earnings dates and pre-open vs after-close timing: verify via Google Search when the X posts are ambiguous.
- Do NOT attribute products to the wrong company (e.g. Claude is by Anthropic; ChatGPT by OpenAI; Gemini by Google).
- US holidays during the target week affect scheduling — if NFP or CPI would fall on a holiday, it is typically shifted. Verify.

ANALYTICAL DEPTH — every bullet must answer:
(a) WHAT is scheduled (day, Israel time, consensus if macro, EPS/revenue consensus if earnings).
(b) WHY it matters (what position or narrative is at stake).
(c) SO WHAT — what print/outcome would shift market pricing, Fed odds, or sector positioning.

OUTPUT FORMAT (MANDATORY):
- Output EXACTLY 1 section in the "sections" array.
- Section heading MUST be EXACTLY "נקודות מרכזיות לשבוע הקרוב" (no variations, no emojis).
- The "title" field MUST be EXACTLY: "{expected_title}"
- "content": a list of bullet points, each on its own line starting with "* " (asterisk + space).
- Each bullet: "* Short topic label: one concise analytical sentence with numbers."
- Do NOT use <b>, <strong>, **, or any HTML/markdown formatting.
- Do NOT add a "שורה תחתונה" section or any second section.

COVERAGE — include 10-14 bullets in priority order:
1. The dominant catalyst of the week (MANDATORY first bullet): the single most important scheduled event — Fed decision > major macro print > mega-cap earnings — with day, Israel time, consensus, and what is priced in.
2. Economic calendar for the week: other major data releases (NFP, CPI, PPI, PMI, GDP, jobless claims, retail sales, FOMC minutes) with day, Israel time, consensus, and previous.
3. Earnings calendar for the week: the most important names scheduled to report, with day (pre-open / after-close) and EPS/revenue consensus.
4. Fed speakers scheduled this week: day, Israel time, and why the speech matters in the current context.
5. Geopolitical / trade / tariff deadlines falling this week.
6. Corporate events: investor days, product launches, IPO pricings, options expiries.
7. Current market pricing: what Fed-funds futures imply for the next rate decision, what options markets imply for key tickers.
8. Wild cards from the X posts: less obvious catalysts that could move markets.
If the week has no obvious dominant catalyst, state this explicitly in the first bullet ("שבוע דל קטליזטורים — המוקד יעבור ל...").

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections."""

    # ══════════════════════════════════════════════════════════════
    # weekly_summary
    # ══════════════════════════════════════════════════════════════
    elif review_type == "weekly_summary":
        return f"""Summarize in bullets for investors the past week that concluded today on Wall Street, with an emphasis on the past week's performance, key events that occurred, important earnings reports released, and everything essential for investors to know — based ONLY on the source posts from X provided below.

DATES:
- Script run date: {date_str} ({day_name})
- Summary target week: {week_range}

RULES:
- Output in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and financial terms in parentheses on first use.
- Source grounding: CONTENT (events, companies, earnings, narratives) must come from (a) the X posts below, or (b) the economic calendar provided in market_data above. Do NOT invent events, companies, or stories that are not represented in these inputs. Do NOT use Google Search to introduce new events not in the X posts.
- Authoritative numbers: Use Finnhub WEEKLY PERFORMANCE data above for index / ETF / sector % changes — these override everything else.
- Google Search is allowed ONLY to verify specific point levels or cross-check extreme-looking numbers from tweets.
- Write in PAST TENSE — the week has concluded.
- Backward-looking. Describe what happened this week. Do NOT speculate on next week.
- Do not repeat items already covered in the prior_context block above.
- Every bullet needs specifics: a number, %, ticker, or date. No vague claims.
- Combine related items about the same company into one bullet.
- No buy/sell recommendations.
- Output pure JSON only — no backticks, no explanations.

TIME FRAME (STRICT):
- This summary covers the trading week of {week_range} ONLY.
- Include ONLY events, data, and market moves that occurred during this specific week.
- Do NOT include the previous week or the upcoming week.

DATA ACCURACY:
- For WEEKLY % changes on indices / ETFs / sectors: use ONLY the Finnhub WEEKLY PERFORMANCE data above. NEVER use the daily numbers for the weekly summary.
- Do NOT confuse Friday's daily change with the weekly change.
- For index point levels (Friday's close): verify via Google Search. Do NOT estimate from ETF prices.
- For tweet-sourced numbers that look extreme: cross-check via Google Search.
- When reporting macro data from this week: always include actual, forecast, and previous. CPI: always include BOTH headline and core.
- Never claim a weekly high/low or record without verification.
- If a number cannot be verified: omit it.
- Do not attribute products to the wrong company (e.g. Claude is by Anthropic; ChatGPT by OpenAI; Gemini by Google).

US-ISRAEL TIME:
Use ONLY the offsets in the time_block above.

ANALYTICAL DEPTH — every bullet must answer:
(a) WHAT happened (with specific numbers).
(b) WHY it happened (catalyst or cause).
(c) SO WHAT — the implication for Fed policy, sector rotation, positioning, or the broader cycle.
A bullet that only reports a fact without explanation is insufficient.

OUTPUT FORMAT (MANDATORY):
- Output EXACTLY 1 section in the "sections" array.
- Section heading MUST be EXACTLY "סיכום השבוע" (no variations, no emojis).
- The "title" field MUST be EXACTLY: "{expected_title}"
- "content": a list of bullet points, each on its own line starting with "* " (asterisk + space).
- Each bullet: "* Short topic label: one concise analytical sentence with numbers."
- Do NOT use <b>, <strong>, **, or any HTML/markdown formatting.
- Do NOT add a "שורה תחתונה" section or any second section.

COVERAGE — include 10-14 bullets in priority order:
1. Weekly index performance (MANDATORY first bullet): S&P 500, Nasdaq, Dow, Russell 2000 — weekly % from Finnhub WEEKLY data + Friday's closing levels. Note if this was best/worst week in X months.
2. Sector leaders / laggards for the week: use ONLY the Finnhub weekly % data for XLE/XLK/XLF/XLY/XLV/XLI. Name the rotation theme if present.
3. Macro data published this week: FULL numbers (actual / forecast / previous) + market reaction for each. CPI headline + core mandatory if released.
4. Key earnings reports this week: major names, beat/miss on revenue and EPS, stock reaction, and what it signals for the sector.
5. Top weekly stock movers ($TICKER +/- weekly %, with the specific catalyst).
6. Commodities: weekly moves in oil (with catalyst), gold, Bitcoin.
7. Yields & FX: 10Y yield weekly move, DXY direction, rate-cut probability shifts.
8. Fed commentary and policy signals this week: speeches, minutes, or decisions.
9. Geopolitical, trade / tariff developments that moved markets this week.
10. M&A, IPOs, or major corporate actions this week if relevant.

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections."""

    # ══════════════════════════════════════════════════════════════
    # events — UNCHANGED
    # ══════════════════════════════════════════════════════════════
    elif review_type == "events":
        return f"""You are a financial calendar editor creating an economic events calendar in Hebrew.

Your task: Based on the tweets/posts below AND your knowledge of the US economic calendar, create a list of upcoming economic events and market catalysts for the next 5-7 days.

Rules:
- Write event titles and descriptions in Hebrew.
- Include 6-10 events sorted by date (earliest first).
- Use Israel time (UTC+3) for all times.
- If exact time is unknown, use 15:30 (US market open in Israel time) as default.
- IMPORTANT: Cross-check each scheduled economic release against US market holidays. NFP on a market holiday (like Good Friday) is typically shifted — verify via Google Search if unsure.
- impact levels: "high" = moves entire market (Fed decision, NFP, CPI), "medium" = moves a sector (earnings, PMI), "low" = background data.

{tweets_block}

Output JSON format — THIS IS DIFFERENT FROM OTHER REVIEWS (uses "items", not "sections"):
{{"items":[{{"time":"2026-03-30T15:30:00+03:00","title":"שם האירוע בעברית","impact":"high","description":"1-2 משפטים בעברית — מה זה ולמה זה חשוב למשקיעים"}}]}}

Event types: macro data (NFP, CPI, PPI, PMI, GDP, jobless claims), Fed rate decisions and Fed speakers, major earnings (mega-cap), options/futures expiry, Treasury auctions, geopolitical deadlines."""

    # ══════════════════════════════════════════════════════════════
    # live_news
    # ══════════════════════════════════════════════════════════════
    elif review_type == "live_news":
        now_time = datetime.now(ISR_TZ).strftime('%H:%M')
        return f"""Summarize in bullets the important Wall Street events from the PAST HOUR for investors, based on the sources from X, with an emphasis on what is happening right now.

CURRENT DATE AND TIME: {date_str} at {now_time} Israel time.
WINDOW START: {window_start} Israel time (60 minutes ago).

RULES:
- Output in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and financial terms in parentheses on first use.
- Source grounding: CONTENT must come from the X posts below. Do NOT invent events. Do NOT use Google Search to introduce new stories.
- Google Search is allowed ONLY to verify specific numbers, names, or times from the X posts.
- TENSE:
  * PRESENT tense for events unfolding right now ("Powell נואם כעת").
  * PAST tense for events that happened within the past hour ("Apple הודיעה לפני 20 דקות").
  * NEVER future tense — scheduled events do not belong here.
- Every bullet must include a specific Israel time within the past-hour window.
- No buy/sell recommendations.
- Output pure JSON only — no backticks, no explanations.

STRICT WINDOW — PAST 60 MINUTES ONLY:
- Include ONLY events that occurred between {window_start} and {now_time} Israel time today.
- This is NOT a daily summary. Do NOT include events from earlier today, from overnight, or from yesterday.
- Anything older than 60 minutes belongs in daily_summary or daily_prep — not here.
- Do NOT pad. If only 2 events happened in the past hour, output 2 bullets. If only 1, output 1.
- If the past hour was quiet, output ONE bullet: "שעה שקטה — אין אירועים מהותיים ב-60 הדקות האחרונות". Do NOT reach back to fill.

CONTENT SCOPE:
- News and events only — no index levels, % changes, commodity prices, or VIX.
- Do not attribute products to the wrong company (Claude is by Anthropic; ChatGPT by OpenAI; Gemini by Google).

US-ISRAEL TIME:
Use ONLY the offsets in the time_block above.

OUTPUT FORMAT (MANDATORY):
- Output EXACTLY 1 section in the "sections" array.
- Section heading MUST be EXACTLY "חדשות אחרונות".
- The "title" field MUST be EXACTLY: "{expected_title}"
- "content": bullets starting with "* ", each formatted as: "* Short label (IL time): one sentence with specifics."
- No HTML/markdown formatting. No "שורה תחתונה".

COVERAGE — 0 to 6 bullets from the past 60 minutes only:
1. Breaking news just reported.
2. Corporate actions announced in the past hour (M&A, product launches, guidance updates, layoffs, executive changes, SEC filings).
3. Regulatory or legal developments just announced.
4. Fed / central bank comments just delivered.
5. Geopolitical events that just broke.
6. Analyst calls or institutional moves just published.

Honesty over volume. 2 solid bullets beat 6 padded ones.

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections."""

    return ""

# ══════════════════════════════════════════════════════════════
# GEMINI CALL
# ══════════════════════════════════════════════════════════════

def call_gemini(prompt, temperature=0.2):
    """Lower default temperature (0.2 vs 0.7) for factual journalism.
    Callers pass higher temperature for events calendar where mild variety is fine."""
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
                    "temperature": temperature,
                    "maxOutputTokens": 8192
                }
            }
        )

        resp_data = r.json()
        print(f"  Gemini status: {r.status_code} (attempt {attempt+1}/{max_retries}, temp={temperature})")

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
# POST-PROCESSING — STRUCTURE ENFORCEMENT
# ══════════════════════════════════════════════════════════════

_BULLET_CHARS = r'[•■●▪▫◦‣⁃–—]'

def normalize_bullets(text):
    """Convert mixed bullet styles (•, ■, -, etc.) to `* ` so the HTML renderer picks them up."""
    if not isinstance(text, str) or not text.strip():
        return text

    lines = text.split("\n")
    result = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            result.append("")
            continue

        converted = re.sub(rf'^{_BULLET_CHARS}\s+', '* ', stripped)
        converted = re.sub(r'^-\s+', '* ', converted)

        if converted.startswith('* '):
            result.append(converted)
        else:
            if re.match(r'^\$[A-Z]{1,5}\s*:', stripped):
                result.append('* ' + stripped)
            elif re.match(r'^[^\n]{2,35}:\s+\S', stripped) and len(lines) >= 3:
                result.append('* ' + stripped)
            else:
                result.append(stripped)

    return "\n".join(l for l in result if l.strip() or not l)

def enforce_structure(result, review_type, expected_title):
    """Force the Gemini output to match the single-section structure expected by the HTML renderer.
    All reviews (except events) must have EXACTLY 1 section with bullets — no 'שורה תחתונה'."""

    if not isinstance(result, dict):
        print("  ⚠️ enforce_structure: result is not a dict — returning unchanged")
        return result

    # events uses a different structure (items array) — no enforcement here
    if review_type == "events":
        return result

    first_heading = EXPECTED_FIRST_HEADING.get(review_type, "נקודות מרכזיות")

    # 1. Force title
    original_title = result.get("title", "")
    result["title"] = expected_title
    if original_title != expected_title:
        print(f"  ✅ Title overridden: '{original_title}' → '{expected_title}'")

    # 2. Work on sections
    sections = result.get("sections", [])
    if not isinstance(sections, list) or len(sections) == 0:
        print("  ⚠️ enforce_structure: no sections — creating empty structure")
        result["sections"] = [{"heading": first_heading, "content": ""}]
        return result

    # 3. If more than 1 section, consolidate all BULLET content into the first, drop prose paragraphs
    if len(sections) > 1:
        print(f"  ✅ Consolidating {len(sections)} sections → 1 (single-section spec)")
        merged_bullets = []
        for s in sections:
            c = s.get("content", "")
            if isinstance(c, list):
                c = "\n".join(str(x) for x in c)
            if not isinstance(c, str) or not c.strip():
                continue
            for line in c.split("\n"):
                stripped = line.strip()
                if not stripped:
                    continue
                # Take only bullet-like lines; prose paragraphs (שורה תחתונה leftovers) are dropped
                if re.match(rf'^(\*|-|\$[A-Z]{{1,5}}\s*:|{_BULLET_CHARS})', stripped):
                    merged_bullets.append(stripped)
        sections = [{"heading": first_heading, "content": "\n".join(merged_bullets)}]

    # 4. Force heading on the single section
    original_h0 = sections[0].get("heading", "")
    sections[0]["heading"] = first_heading
    if original_h0 != first_heading:
        print(f"  ✅ Section heading overridden: '{original_h0}' → '{first_heading}'")

    # 5. Normalize bullets in the content
    c0 = sections[0].get("content", "")
    if isinstance(c0, list):
        c0 = "\n".join(str(x) for x in c0)
    sections[0]["content"] = normalize_bullets(c0)

    result["sections"] = sections
    return result

# ══════════════════════════════════════════════════════════════
# POST-PROCESSING — REGEX FIXES & VALIDATION
# ══════════════════════════════════════════════════════════════

TEXT_FIXES = [
    # Political leaders — current titles
    (r'הנשיא\s+לשעבר\s+טראמפ', 'הנשיא טראמפ', 'Trump is the current president'),
    (r'נשיא\s+ארה"ב\s+לשעבר\s+טראמפ', 'נשיא ארה"ב טראמפ', 'Trump is the current president'),
    (r'הנשיא\s+לשעבר\s+דונלד\s+טראמפ', 'הנשיא דונלד טראמפ', 'Trump is the current president'),
    (r'טראמפ\s*,?\s*הנשיא\s+לשעבר', 'טראמפ, הנשיא', 'Trump is the current president'),
    (r'הנשיא\s+ביידן', 'הנשיא לשעבר ביידן', 'Biden is the FORMER president'),
    (r'נשיא\s+ארה"ב\s+ביידן', 'הנשיא לשעבר ביידן', 'Biden is the FORMER president'),
    # Attribution mistakes
    (r'אמזון\s+השיקה?\s+את\s+Claude', 'Anthropic השיקה את Claude', 'Claude is by Anthropic'),
    (r'מיקרוסופט\s+השיקה?\s+את\s+ChatGPT', 'OpenAI השיקה את ChatGPT', 'ChatGPT is by OpenAI'),
    (r'AWS\s+השיקה?\s+את\s+Claude', 'Anthropic השיקה את Claude', 'Claude is by Anthropic'),
    # Terminology mistakes
    (r'הנפקה\s+ראשונית\s+לציבור\s*\(ETF\)', 'תעודת סל (ETF)', 'IPO ≠ ETF'),
    (r'תעודת\s+סל\s*\(IPO\)', 'הנפקה ראשונית (IPO)', 'ETF ≠ IPO'),
]

INDEX_RANGES = {
    r'(?:S&P\s*500|אס[\-&]?אנד[\-]?פי)\s*[\-–:]\s*([\d,\.]+)': (4000, 8000, 'S&P 500'),
    r'(?:נסדק|נאסד"ק|Nasdaq)\s*(?:100|הקומפוזיט|Composite)?\s*[\-–:]\s*([\d,\.]+)': (12000, 30000, 'Nasdaq'),
    r'(?:דאו\s*ג\'?ונס|Dow\s*Jones?|DJIA)\s*[\-–:]\s*([\d,\.]+)': (30000, 55000, 'Dow Jones'),
    r'(?:ראסל|Russell)\s*2000\s*[\-–:]\s*([\d,\.]+)': (1500, 3500, 'Russell 2000'),
}

PCT_MAX_DAILY = 8.0
PCT_MAX_WEEKLY = 15.0

# Pre-market tense guards for daily_prep
PRE_MARKET_TENSE_FIXES = [
    (r'השוק\s+נפתח\s+הבוקר', 'השוק צפוי להיפתח', 'market has not opened yet'),
    (r'השווקים\s+נפתחו\s+הבוקר', 'השווקים צפויים להיפתח', 'markets have not opened yet'),
    (r'המסחר\s+נפתח\s+הבוקר', 'המסחר צפוי להיפתח', 'trading has not opened yet'),
    (r'המדדים?\s+(?:פתחו?|פותח|נפתחו?)\s+(?:את\s+)?(?:היום|הבוקר|המסחר)', 'המדדים צפויים להיפתח', 'indices have not opened yet'),
    (r'וול\s+סטריט\s+נפתחה\s+הבוקר', 'וול סטריט צפויה להיפתח', 'Wall Street has not opened yet'),
    (r'פתיחת\s+המסחר\s+היתה', 'פתיחת המסחר צפויה להיות', 'opening has not happened yet'),
    (r'המסחר\s+היום\s+התנהל', 'המסחר היום צפוי להתנהל', 'trading has not happened yet'),
    (r'המשקיעים\s+הגיבו\s+הבוקר', 'המשקיעים צפויים להגיב', 'no reaction yet — market closed'),
    (r'הגיבו\s+בפתיחה', 'יגיבו בפתיחה', 'no reaction yet — market closed'),
]

def is_before_us_market_open(now):
    """Is it currently before 16:30 Israel time on a weekday (= US market hasn't opened)?"""
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return minutes < (16 * 60 + 30)

def apply_pre_market_tense_guard(result, review_type):
    """For daily_prep runs that finish BEFORE US market open, fix any accidental
    past-tense descriptions of market activity."""
    if review_type != "daily_prep":
        return result

    now = datetime.now(ISR_TZ)
    if not is_before_us_market_open(now):
        return result

    def fix_text(text):
        if not isinstance(text, str):
            return text
        for pattern, replacement, desc in PRE_MARKET_TENSE_FIXES:
            new_text = re.sub(pattern, replacement, text)
            if new_text != text:
                print(f"  ✅ Pre-market tense fixed: {desc}")
                text = new_text
        return text

    if isinstance(result, dict):
        if "title" in result:
            result["title"] = fix_text(result["title"])
        for section in result.get("sections", []):
            if "content" in section:
                if isinstance(section["content"], str):
                    section["content"] = fix_text(section["content"])
                elif isinstance(section["content"], list):
                    section["content"] = [fix_text(i) if isinstance(i, str) else i for i in section["content"]]

    return result

def validate_and_fix(result, review_type):
    warnings = []
    fix_count = 0

    def process_text(text):
        nonlocal fix_count
        if not isinstance(text, str):
            return text

        for pattern, replacement, desc in TEXT_FIXES:
            new_text = re.sub(pattern, replacement, text)
            if new_text != text:
                fix_count += 1
                warnings.append(f"AUTO-FIXED: {desc}")
                print(f"  ✅ Auto-fixed: {desc}")
                text = new_text

        for idx_pattern, (lo, hi, name) in INDEX_RANGES.items():
            for match in re.finditer(idx_pattern, text):
                raw_num = match.group(1).replace(',', '')
                try:
                    val = float(raw_num)
                    if val < lo or val > hi:
                        warn = f"SUSPICIOUS NUMBER: {name} = {raw_num} (expected range {lo:,}-{hi:,})"
                        warnings.append(warn)
                        print(f"  ⚠️  {warn}")
                except (ValueError, IndexError):
                    pass

        pct_pattern = r'(?:עלייה|ירידה|עלה|ירד|זינק|צנח|איבד|הוסיף|קפץ)\s+(?:של?\s*)?(?:כ[\-]?)?([\d\.]+)%'
        for match in re.finditer(pct_pattern, text):
            try:
                pct_val = float(match.group(1))
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

    if isinstance(result, dict):
        if "title" in result and isinstance(result["title"], str):
            result["title"] = process_text(result["title"])
        for section in result.get("sections", []):
            if "content" in section:
                if isinstance(section["content"], str):
                    section["content"] = process_text(section["content"])
                elif isinstance(section["content"], list):
                    section["content"] = [process_text(item) if isinstance(item, str) else item for item in section["content"]]
            if "heading" in section and isinstance(section["heading"], str):
                section["heading"] = process_text(section["heading"])
        for item in result.get("items", []):
            if "title" in item:
                item["title"] = process_text(item["title"])
            if "description" in item:
                item["description"] = process_text(item["description"])

    if warnings:
        print(f"\n  📋 Validation summary: {fix_count} auto-fixes, {len(warnings)} total warnings")
    else:
        print("  ✅ Validation passed — no issues found")

    return result, warnings

# ══════════════════════════════════════════════════════════════
# FACT-CHECKER
# ══════════════════════════════════════════════════════════════

def fact_check_with_gemini(result, market_data, review_type):
    """Flash-based fact check. Runs AFTER enforce_structure so the structure is already correct."""
    review_json = json.dumps(result, ensure_ascii=False, indent=2)

    prompt = f"""You are a FACT-CHECKER for a Hebrew financial market review. Your ONLY job is to find and fix factual errors.

VERIFIED MARKET DATA (100% correct, sourced from Finnhub API):
{market_data if market_data else "(No Finnhub data available for this run)"}

THE REVIEW TO CHECK:
{review_json}

YOUR TASK:
- Compare EVERY number, percentage, and factual claim in the review against the verified data and your own knowledge.
- Fix any number that contradicts the verified data.
- Fix any factual error (wrong company attribution, wrong political titles, wrong dates, wrong terminology).
- For sector ETF percentages (XLE/XLK/XLF/XLY/XLV/XLI): if a specific sector number appears in the review that does NOT match the Finnhub data, REMOVE that claim or replace it with a number from the Finnhub data.
- For 10-year Treasury yield, commodity absolute prices ($/barrel, $/oz), and DXY level: these are NOT in Finnhub. Only keep them if they are clearly reasonable; otherwise remove.
- DO NOT change the writing style, structure, section count, or section heading.
- DO NOT remove content — only fix errors or remove clearly-hallucinated numbers.
- DO NOT change the "title" field or section heading — those are already enforced.
- If everything is correct, return the review unchanged.

STRUCTURE NOTE:
- Reviews (except "events") have EXACTLY ONE section named by the review type's designated heading (e.g. "נקודות מרכזיות", "סיכום המסחר", "חדשות אחרונות"). Do NOT add a second section.
- The content is a bullet list. Do NOT convert it to prose or add a summary paragraph.

COMMON ERRORS:
- Donald Trump is the CURRENT US President (since Jan 2025). NOT a former president.
- Claude is by Anthropic, ChatGPT is by OpenAI, Gemini is by Google.
- IPO ≠ ETF.

OUTPUT: Return the corrected review as valid JSON in EXACTLY the same structure (same title, same single section heading, same number of sections). No backticks, no explanations — pure JSON only."""

    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1, "maxOutputTokens": 8192}
            },
            timeout=120
        )
        if not r.ok:
            print(f"  Fact-check Gemini returned status {r.status_code}, skipping")
            return result

        resp_data = r.json()
        candidate = resp_data.get("candidates", [{}])[0]
        parts = candidate.get("content", {}).get("parts", [])
        text = ""
        for part in parts:
            if "text" in part:
                text = part["text"]

        if not text:
            print("  Fact-check returned no text, skipping")
            return result

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

        checked = json.loads(text)

        if "sections" in result and "sections" not in checked:
            print("  Fact-check broke JSON structure, skipping")
            return result

        original_str = json.dumps(result, ensure_ascii=False)
        checked_str = json.dumps(checked, ensure_ascii=False)
        if original_str != checked_str:
            print("  ✅ Fact-checker made corrections")
        else:
            print("  ✅ Fact-checker confirmed — no errors found")

        return checked

    except json.JSONDecodeError as e:
        print(f"  Fact-check JSON parse error: {e}, using original")
        return result
    except Exception as e:
        print(f"  Fact-check failed: {e}, using original")
        return result

# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    now = datetime.now(ISR_TZ)
    date_str = now.strftime("%Y-%m-%d")
    day_name = PY_TO_HEB[now.weekday()]

    holidays = load_holidays()
    today_is_trading = is_trading_day(now, holidays)

    print(f"Running {REVIEW_TYPE} for {date_str} ({day_name}), trading day: {today_is_trading}")

    # Compute title date/week range
    title_date_str = date_str
    title_day_name = day_name
    week_range = None
    target_is_trading = today_is_trading

    if REVIEW_TYPE == "daily_prep":
        if today_is_trading:
            target = now
        else:
            target = get_next_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]
        target_is_trading = is_trading_day(target, holidays)

    elif REVIEW_TYPE == "daily_summary":
        target = get_last_trading_day(now, holidays)
        title_date_str = target.strftime("%Y-%m-%d")
        title_day_name = PY_TO_HEB[target.weekday()]

    elif REVIEW_TYPE in ("weekly_prep", "weekly_summary"):
        if REVIEW_TYPE == "weekly_summary":
            week_range = get_prev_week_range_str(now)
        else:
            weekday = now.weekday()
            if weekday <= 4:
                monday = now - timedelta(days=weekday)
            else:
                monday = now + timedelta(days=(7 - weekday))
            friday = monday + timedelta(days=4)
            week_range = f"{monday.strftime('%d/%m')}–{friday.strftime('%d/%m/%Y')}"

    print(f"  Title date: {title_date_str} ({title_day_name}), week_range: {week_range}")

    # Build the exact title we will force onto the output
    now_time_str = now.strftime('%H:%M')
    expected_title = build_expected_title(REVIEW_TYPE, title_day_name, title_date_str, week_range, now_time_str)
    print(f"  Expected title: {expected_title}")

    tweets = fetch_tweets()
    if not tweets:
        print("No tweets fetched, skipping.")
        return

    print(f"Fetched {len(tweets.split(chr(10)+chr(10)))} tweet blocks")

    # ── BUG FIX #2: Finnhub weekly data only for weekly_summary, NOT weekly_prep ──
    # (weekly_prep is forward-looking — doesn't use last week's % changes)
    is_weekly = REVIEW_TYPE == "weekly_summary"
    market_data = fetch_market_data(weekly=is_weekly)

    # ── BUG FIX #1: Economic calendar routing ──
    # weekly_prep must look FORWARD (days_forward=7), not backward
    if REVIEW_TYPE == "daily_summary":
        econ_data = fetch_economic_data(days_back=1, days_forward=0)
    elif REVIEW_TYPE == "weekly_summary":
        econ_data = fetch_economic_data(days_back=7, days_forward=0)
    elif REVIEW_TYPE == "weekly_prep":
        econ_data = fetch_economic_data(days_back=0, days_forward=7)
    elif REVIEW_TYPE == "daily_prep":
        econ_data = fetch_economic_data(days_back=1, days_forward=1)
    elif REVIEW_TYPE == "live_news":
        econ_data = fetch_economic_data(days_back=0, days_forward=0)
    else:
        econ_data = ""

    if econ_data:
        market_data = market_data + "\n" + econ_data if market_data else econ_data

    macro_checklist = get_macro_checklist(REVIEW_TYPE, date_str, week_range)
    if macro_checklist:
        market_data = market_data + "\n" + macro_checklist if market_data else macro_checklist

    # Load existing data.json to inject prior review context
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    except Exception as e:
        print(f"  Could not load data.json for context: {e}")
        existing_data = {}

    prior_context = get_prior_review_context(REVIEW_TYPE, existing_data)
    if prior_context:
        print(f"  Injected prior-review context ({len(prior_context)} chars)")

    # ── BUG FIX #3: Compute 60-minute window start for live_news ──
    if REVIEW_TYPE == "live_news":
        window_start = (now - timedelta(minutes=60)).strftime('%H:%M')
        print(f"  live_news window: {window_start} → {now.strftime('%H:%M')} IL")
    else:
        window_start = None

    prompt = get_prompt(
        tweets, REVIEW_TYPE, date_str, day_name,
        title_date_str=title_date_str,
        title_day_name=title_day_name,
        week_range=week_range,
        is_trading=target_is_trading if REVIEW_TYPE == "daily_prep" else today_is_trading,
        market_data=market_data,
        prior_context=prior_context,
        expected_title=expected_title,
        window_start=window_start,
    )
    if not prompt:
        print(f"Unknown review type: {REVIEW_TYPE}")
        return

    # Temperature: 0.2 for factual journalism, 0.4 for events (allow mild variety)
    gen_temp = 0.4 if REVIEW_TYPE == "events" else 0.2
    result = call_gemini(prompt, temperature=gen_temp)

    # Layer 1: Regex-based auto-fix (instant, deterministic)
    print("\n── Layer 1: Regex validation ──")
    result, validation_warnings = validate_and_fix(result, REVIEW_TYPE)

    # Layer 2: Structure enforcement — forces title, single section, heading name, bullet format
    print("\n── Layer 2: Structure enforcement ──")
    result = enforce_structure(result, REVIEW_TYPE, expected_title)

    # Layer 3: Gemini Flash fact-checker
    print("\n── Layer 3: Gemini fact-checker ──")
    result = fact_check_with_gemini(result, market_data, REVIEW_TYPE)

    # Layer 4: Re-enforce structure (defensive — fact-checker sometimes alters section headings)
    print("\n── Layer 4: Final structure enforcement ──")
    result = enforce_structure(result, REVIEW_TYPE, expected_title)

    # Layer 5: Pre-market tense guard (daily_prep only, only if run before US market open)
    print("\n── Layer 5: Pre-market tense guard ──")
    result = apply_pre_market_tense_guard(result, REVIEW_TYPE)
    print("── Validation complete ──\n")

    # Save
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
