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
        # Sector ETFs — NEW (was missing, causing XLE/XLK/XLY hallucinations)
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
            if actual is None:
                continue
            event_name = e.get("event", "")
            estimate = e.get("estimate")
            prev = e.get("prev")
            unit = e.get("unit", "")
            impact = e.get("impact", "")
            date = e.get("time", "")[:10]

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
    """Inject yesterday's/last week's review so Gemini doesn't repeat the same news.
    This is the fix for: 'daily_prep keeps summarizing what was already in daily_summary'."""
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
# PROMPTS
# ══════════════════════════════════════════════════════════════

SHARED_RULES = """Rules:
- Write ONLY in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and well-known financial terms in parentheses on first use only.
- Be specific: every claim must include a number, percentage, or ticker. Never write vague statements like "the market had an interesting week".
- Do NOT repeat the same information across sections. Each section must contain NEW content.
- Do NOT mention the same ticker or company in multiple separate bullets. If a company has multiple news items, combine them into ONE bullet.
- No buy/sell recommendations.
- Start each section directly with the key fact. No generic opening sentences.
- Output pure JSON only, no backticks, no explanations.

CRITICAL — KEY MARKET DATA (MANDATORY VERIFICATION):
- If VERIFIED MARKET DATA from Finnhub API is provided above the tweets, you MUST use those numbers for index performance (% change). Do NOT override them with numbers from tweets or from memory.
- Use the verified % changes as-is. For exact index point levels, gold price, oil price, and VIX level: ALWAYS use Google Search. Do NOT calculate them from ETF prices.
- You MUST verify via Google Search the current prices of: Brent crude oil, WTI crude oil, gold, and any other commodity you mention.
- If a tweet states a price that seems extreme or unusual, you MUST verify it via Google Search before including it.
- NEVER trust a single tweet for major price data. Always cross-reference.
- NEVER write vague descriptions like "the market closed in green territory" or "mixed trading" without exact numbers.
- NEVER claim an index or stock is at an "all-time high" (שיא / שיא כל הזמנים) unless you verify it via Google Search.

CRITICAL — SECTOR PERFORMANCE (NEW RULE):
- For sector ETF performance (XLE/XLK/XLF/XLY/XLV/XLI/XLP/XLU), use ONLY the percentages provided in the Finnhub verified data above.
- If the Finnhub data does not include a specific sector, do NOT invent a number. Either omit it or use Google Search to verify.
- NEVER write a specific sector percentage without a source — this is a common hallucination.

CRITICAL — MAJOR ECONOMIC DATA (DO NOT MISS):
- Use Google Search to check if any major US economic data was released today: CPI, PPI, NFP, GDP, Jobless Claims, ISM PMI, Consumer Confidence, Retail Sales, FOMC minutes/decision.
- If major data WAS released today, it MUST appear in the review — even if no tweet mentions it. This is non-negotiable.
- CPI and NFP are the two most important data releases. Missing them from a daily review is a critical failure.
- When CPI is mentioned, ALWAYS report BOTH headline CPI AND Core CPI (excluding food and energy).
- When mentioning economic data, ALWAYS include: actual % (monthly AND annual), comparison to forecast, comparison to previous. Vague descriptions without numbers are unacceptable.

CRITICAL — DATA ACCURACY:
- EVERY number in the review must come from one of these sources: (1) Finnhub verified data above, (2) a specific tweet, or (3) Google Search verification.
- NEVER invent, estimate, or recall prices from memory. If you cannot point to a source, do NOT include the number.
- For the 10-year Treasury yield: verify via Google Search. Do NOT estimate from TLT.
- For commodity absolute prices (oil $/barrel, gold $/oz): verify via Google Search — do NOT estimate from ETF prices.
- If a number from a tweet contradicts the Finnhub verified data, the Finnhub data is correct — the tweet is wrong.
- Getting a number wrong destroys credibility. When in doubt, omit.

CRITICAL — CONSISTENCY:
- The "שורה תחתונה" paragraph MUST be consistent with the bullet points above it.
- Read your own bullets before writing the bottom line.

CRITICAL — FINANCIAL TERMINOLOGY:
- Use precise Hebrew financial terms. IPO (הנפקה ראשונית לציבור) is NOT the same as ETF (תעודת סל).
- A private company planning an IPO is issuing shares — it does NOT have an ETF.
- SPO = הנפקה משנית, SPAC = חברת רכש ייעודית, M&A = מיזוג ורכישה.
- Futures = חוזים עתידיים, Options = אופציות, Bonds = אגרות חוב.
- NASDAQ INDICES — there are TWO different indices, do NOT confuse them:
  * נאסד"ק 100 (Nasdaq 100 / NDX) — 100 החברות הגדולות בבורסת נאסד"ק (ללא פיננסיים). QQQ עוקב אחרי מדד זה. רמה בסביבות 25,000-26,000 נקודות.
  * נאסד"ק קומפוזיט (Nasdaq Composite / IXIC) — כל החברות בבורסת נאסד"ק. רמה בסביבות 23,000-24,000 נקודות.
  * The Finnhub data uses QQQ which tracks the Nasdaq 100. When reporting QQQ % change, label it as "נאסד"ק 100" or "Nasdaq 100".
  * If you report an index LEVEL (points), verify via Google Search which index the number belongs to. A level of ~24,000 is the Composite, not the 100. A level of ~25,500 is the 100, not the Composite.
  * NEVER mix them — do not write "נאסד"ק 100" and then give the Composite level.

CRITICAL — FACTUAL ACCURACY (ATTRIBUTION):
- NEVER attribute a product, model, or technology to the wrong company.
- Claude is made by ANTHROPIC, not Amazon/AWS. ChatGPT is by OPENAI, not Microsoft. Gemini is by GOOGLE.
- A product available ON a platform is NOT made BY that platform.

CRITICAL — CURRENT POLITICAL LEADERS:
- Donald Trump is the CURRENT President of the United States (inaugurated January 2025). He is NOT a former president.
- Write "הנשיא טראמפ" or "נשיא ארה\"ב טראמפ" — NEVER "הנשיא לשעבר".

CRITICAL — US-ISRAEL TIME CONVERSION:
- US market opens at 9:30 AM ET, closes at 4:00 PM ET.
- To convert US Eastern Time to Israel time, use the offset provided below.
- NEVER guess the time offset — use ONLY the value calculated for today."""

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

# ── Output format block — uniform across all review types ──
def get_output_format_block(first_heading, expected_title):
    """Standard, rigid output-format spec. This is what fixes the '3 sections instead of 2' bug
    and the 'bottom line never gets special styling' bug."""
    return f"""
CRITICAL — OUTPUT FORMAT (MANDATORY, NOT NEGOTIABLE):
- Output EXACTLY 2 sections in the "sections" array. Not 1, not 3, not 4. EXACTLY 2.
- Section 1: heading MUST be EXACTLY "{first_heading}" (no variations, no emojis, no added words).
- Section 2: heading MUST be EXACTLY "שורה תחתונה" (no variations).
- The "title" field MUST be EXACTLY: "{expected_title}"
- Section 1 "content": a list of bullet points, each on its own line, each starting with "* " (asterisk + space).
- Each bullet: "* Short topic label: one concise analytical sentence with numbers."
- Section 2 "content": a flowing paragraph (4-6 sentences). NO bullets in section 2.
- Do NOT use <b>, <strong>, **, ■, 📍, or any HTML/markdown formatting inside content.
- Do NOT add extra sections. If you are tempted to add a third section, MERGE that content into section 1 as more bullets.
"""

def get_prompt(tweets, review_type, date_str, day_name, title_date_str=None, title_day_name=None,
               week_range=None, is_trading=True, market_data="", prior_context="", expected_title=""):
    if not title_date_str:
        title_date_str = date_str
    if not title_day_name:
        title_day_name = day_name

    first_heading = EXPECTED_FIRST_HEADING.get(review_type, "נקודות מרכזיות")
    format_block = get_output_format_block(first_heading, expected_title)

    tweets_block = f"Source tweets/posts from X (Twitter) — date: {date_str}:\n{tweets}"
    if market_data:
        tweets_block = market_data + "\n" + tweets_block
    if prior_context:
        tweets_block = prior_context + "\n" + tweets_block

    from datetime import datetime as dt_class
    time_block = get_time_conversion_block(dt_class.now(ISR_TZ))
    tweets_block = time_block + "\n" + tweets_block

    if review_type == "daily_prep":
        is_same_day = (date_str == title_date_str)
        if is_trading:
            if is_same_day:
                trading_status = """The briefing is for TODAY — a regular trading day.

⚠️ CRITICAL TENSE RULE — THE MARKET HAS NOT OPENED YET:
This briefing is written BEFORE the US market opens. The US market opens at 16:30 Israel time.
- You MAY use 'היום' to refer to today's date (e.g., 'היום מתפרסמים נתוני תביעות אבטלה').
- You MAY use 'הבוקר' for overnight news and pre-market data (e.g., 'החוזים העתידיים נסחרים הבוקר בעלייה').
- ❌ You MUST NOT describe the US market itself as already open, already trading, or having reacted.
- ❌ FORBIDDEN phrases: 'השוק נפתח הבוקר לסנטימנט...', 'המסחר התנהל...', 'המדד פתח בעלייה', 'המשקיעים הגיבו ב...'
- ✅ REQUIRED phrases: 'השוק צפוי להיפתח...', 'עם פתיחת המסחר...', 'המשקיעים יעקבו אחר...', 'התגובה הצפויה...'
- Futures trading is pre-market and CAN be described in present tense ('החוזים נסחרים בעלייה'). The cash market cannot.
- When writing the 'שורה תחתונה', assume the cash market has NOT opened yet. Use future tense for all market activity."""
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
- DO NOT include yesterday's index performance, closing levels, or any backward-looking data. ZERO.
- DO NOT repeat news or events that already appeared in the prior context block above.
- ALL bullets must be FORWARD-LOOKING or about NEW overnight developments.

{SHARED_RULES}

{format_block}

Include 7-12 bullets in section 1. Topics:
1. If the briefing is for a future date, first bullet states when trading resumes.
2. Pre-market/futures sentiment ONLY if the briefing is for today.
3. Scheduled economic data for the target day (Israel times + consensus forecast).
4. Expected earnings reports today.
5. NEW overnight geopolitical developments.
6. NEW overnight company news, analyst upgrades/downgrades.
7. Commodity/currency moves that signal market direction.

Section 2 ("שורה תחתונה"): 4-5 sentences. IMPORTANT — THE CASH MARKET HAS NOT OPENED YET:
- Same-day trading session (before 16:30 Israel time): Describe what is EXPECTED at the open, the key tension awaiting the market, what will dictate direction. Use FUTURE TENSE for all market activity ('צפוי להיפתח', 'יעמדו במוקד', 'התגובה תהיה'). FORBIDDEN: 'השוק נפתח', 'המסחר התנהל', 'המדדים הגיבו'.
- Prepared in advance (e.g., Sunday for Monday): Summarize developments since last session, risks building, what to watch when trading resumes on {title_day_name}. Use "עם פתיחת המסחר ביום {title_day_name}" — NOT "היום".
- Target day NOT a trading day: Start with "אין מסחר" and focus on the next session.

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections. No other text."""

    elif review_type == "daily_summary":
        return f"""You are a senior Wall Street market analyst writing a comprehensive end-of-day market wrap in Hebrew. Your goal is not just to report what happened, but to explain WHY it matters and WHAT it signals for investors. Write in PAST TENSE.

{SHARED_RULES}

CRITICAL — ANALYTICAL DEPTH:
- For index performance: include exact % and point levels, note if it's the best/worst day in X period, explain what drove the move.
- For macro data released today: actual number, forecast, previous, AND explain the market implication.
- For stock moves: explain WHY the stock moved, not just the % change.
- For geopolitical events: explain the transmission mechanism (event → oil → inflation expectations → rate expectations → equity valuations).
- Connect the dots between different developments — don't just list isolated facts.

{format_block}

Include 7-12 bullets in section 1, ordered by market impact:
1. Index performance (S&P 500, Nasdaq, Dow with %, point levels, context).
2. Macro data released today with FULL numbers (actual vs forecast vs previous) and market reaction.
3. Key market-moving events: geopolitics, Fed comments, trade news — with cause-and-effect.
4. Commodities and currencies: oil, gold, Bitcoin, VIX — with % and explanation.
5. Notable stock moves with WHY ($TICKER +/- %, what caused it).
6. Sector rotation (using ONLY Finnhub-provided sector ETF data) or institutional activity if relevant.

Section 2 ("שורה תחתונה"): 4-5 sentences.
1. The key narrative of today's session — what was the dominant force.
2. What shifted in investor positioning (risk-on/off, sector preference, rate expectations).
3. The key tension or contradiction in today's price action.
4. What specific data, event, or level to watch tomorrow and why it matters.

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections. No other text."""

    elif review_type == "weekly_prep":
        return f"""You are a senior Wall Street strategist writing a weekly outlook for Israeli investors in Hebrew.

Your task: Summarize what investors need to know ahead of the trading week of {week_range if week_range else date_str} on Wall Street. Write in FUTURE TENSE.

CRITICAL — TIME FRAME:
- This preview covers the trading week {week_range if week_range else date_str} ONLY.
- Include ONLY events, data releases, and catalysts scheduled for THIS specific week.
- Do NOT include events from previous weeks or events beyond this week's Friday.
- Do NOT include last week's index performance or closing levels — there is a separate "סיכום שבועי" for that.

{SHARED_RULES}

{format_block}

Include 8-14 bullets in section 1, ALL forward-looking:
1. Key events coming THIS week: Fed decisions, economic data (NFP, CPI, PMI, GDP, PPI), earnings reports, trade/tariff deadlines, geopolitical developments.
2. For each event: specific day and Israel time when known.
3. Geopolitical risks and what to watch for.
4. Notable companies expected to report earnings this week.
Do NOT include any bullets about last week's performance. Zero backward-looking data.

Section 2 ("שורה תחתונה"): 4-5 sentences. Start with the ONE dominant theme. Analyze: what is the market currently pricing in, what could surprise to the upside/downside, what combination of events could trigger a significant move. End with a bullish/bearish scenario framework.

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections. No other text."""

    elif review_type == "weekly_summary":
        return f"""You are a senior Wall Street strategist writing a comprehensive weekly review for Israeli investors in Hebrew. Write in PAST TENSE.

Your task: Summarize all significant developments on Wall Street over the trading week of {week_range if week_range else date_str}.

CRITICAL — TIME FRAME:
- This summary covers the trading week {week_range if week_range else date_str} ONLY.
- Include ONLY events, data, and market moves that occurred during THIS specific week.
- Do NOT include events from the current or upcoming week.

CRITICAL — WEEKLY PERFORMANCE:
- If WEEKLY PERFORMANCE data is provided above, use those % changes for the weekly index performance.
- Do NOT use the DAILY performance numbers for the weekly summary.
- Do NOT confuse Friday's daily change with the weekly change.

{SHARED_RULES}

CRITICAL — ANALYTICAL DEPTH:
- For EVERY macro data point: actual, forecast/consensus, comparison to previous, AND what it means for Fed policy and markets.
- For index performance: weekly % change, mention if best/worst week in X months, leading/lagging sectors.
- For geopolitical events: explain the market mechanism (oil → inflation → rates → equity valuations).
- For earnings: note the broader trend for the sector/economy.
- Always connect the dots.

{format_block}

Include 8-14 bullets in section 1:
1. Index performance: S&P 500, Nasdaq, Dow, Russell 2000 — weekly % changes, context, leading/lagging sectors.
2. Macro data published this week with FULL numbers (CPI headline AND core, NFP, claims, sentiment — actuals, forecasts, market reaction).
3. Key events that moved markets: geopolitics, Fed comments, trade/tariff news — transmission mechanism.
4. Commodities with context: oil (weekly change + why), gold, Bitcoin.
5. Notable company news, earnings, M&A — combined where related.
6. Earnings season outlook or institutional positioning.

Section 2 ("שורה תחתונה"): 5-6 sentences.
1. Dominant narrative of the week and what drove it.
2. Shift in investor positioning.
3. Key tension/contradiction in the market.
4. 2-3 specific risks that could reverse the trend.
5. What to watch next week and why it matters.

{tweets_block}

Output ONLY a JSON object with keys: title, date, sections. No other text."""

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

    elif review_type == "live_news":
        now_time = datetime.now(ISR_TZ).strftime('%H:%M')
        return f"""You are a Wall Street news desk editor delivering a real-time news update in Hebrew.

CURRENT DATE AND TIME: {date_str} at {now_time} Israel time.

Your task: Short, sharp bullets of BREAKING NEWS and events happening TODAY ONLY. This is a quick "מה קורה עכשיו" snapshot.

CRITICAL RULES:
- ONLY include news and events from TODAY ({date_str}). Nothing from yesterday or earlier.
- NO economic data that was released on previous days. If data was released yesterday, it is NOT news anymore.
- NO market data, index levels, percentages, commodity prices, or VIX.
- NO "שורה תחתונה" section. Output ONLY ONE section with the news bullets.
- Keep each bullet to 1-2 sentences MAX. This is a news ticker, not analysis.
- 6-10 bullets of: breaking news, geopolitical developments, company announcements, analyst calls, regulatory news.

{SHARED_RULES}

{tweets_block}

Output JSON format:
{{"title":"מה קורה עכשיו בוול סטריט 🇺🇸 – יום {day_name}, {date_str} | {now_time}","date":"{date_str}","sections":[{{"heading":"חדשות אחרונות","content":"* sub-heading: fact\\n* sub-heading: fact..."}}]}}"""

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
# POST-PROCESSING — STRUCTURE ENFORCEMENT (NEW)
# ══════════════════════════════════════════════════════════════

_BULLET_CHARS = r'[•■●▪▫◦‣⁃–—]'

def normalize_bullets(text):
    """Convert mixed bullet styles (•, ■, -, etc.) to `* ` so the HTML renderer picks them up.
    This is the fix for the 'wall of text instead of bullets' bug."""
    if not isinstance(text, str) or not text.strip():
        return text

    lines = text.split("\n")
    result = []
    bullet_count = 0
    non_bullet_lines = []

    # First pass: classify
    for line in lines:
        stripped = line.strip()
        if not stripped:
            result.append("")
            continue

        # Unicode bullets → * 
        converted = re.sub(rf'^{_BULLET_CHARS}\s+', '* ', stripped)
        # Dash bullets → * 
        converted = re.sub(r'^-\s+', '* ', converted)
        # Keep existing * bullets
        if converted.startswith('* '):
            bullet_count += 1
            result.append(converted)
        else:
            # Detect implicit bullets: "$TICKER: ..." or "Topic label: ..." with short label
            if re.match(r'^\$[A-Z]{1,5}\s*:', stripped):
                result.append('* ' + stripped)
                bullet_count += 1
            elif re.match(r'^[^\n]{2,35}:\s+\S', stripped) and len(lines) >= 3:
                # Looks like a sub-heading prefix followed by content, and we have multiple lines
                result.append('* ' + stripped)
                bullet_count += 1
            else:
                non_bullet_lines.append(stripped)
                result.append(stripped)

    # If we found bullets, return as-is (bullets interleaved with potential intro paragraph)
    # If we found NO bullets at all, we have a paragraph — leave it alone (HTML will render as <p>)
    return "\n".join(l for l in result if l.strip() or not l)

def debullet(text):
    """Remove bullet markers from paragraph text (the bottom-line section should be prose)."""
    if not isinstance(text, str) or not text.strip():
        return text
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        stripped = re.sub(rf'^(\*|\-|{_BULLET_CHARS})\s+', '', stripped)
        cleaned.append(stripped)
    # Single paragraph: join with spaces. Multiple paragraphs: join with newline.
    if len(cleaned) <= 1:
        return cleaned[0] if cleaned else ""
    return " ".join(cleaned)

def enforce_structure(result, review_type, expected_title):
    """Force the Gemini output to match the structure expected by the HTML renderer.
    Fixes: wrong titles, 3-section outputs, wrong headings, mixed bullet styles,
    and the critical 'שורה תחתונה' heading that enables the gold-highlighted box styling."""

    if not isinstance(result, dict):
        print("  ⚠️ enforce_structure: result is not a dict — returning unchanged")
        return result

    # Events uses a different structure — no enforcement needed here
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
        result["sections"] = [
            {"heading": first_heading, "content": ""},
            {"heading": "שורה תחתונה", "content": ""},
        ]
        return result

    # 3. If more than 2 sections, consolidate
    if len(sections) > 2:
        # Pick the section that looks most like a bottom-line paragraph:
        # prefer sections whose heading mentions "שורה תחתונה"/"סיכום"/"מה זה אומר"/"מבט קדימה",
        # otherwise prefer the section with the fewest bullets (most prose-like),
        # otherwise fall back to the last section.
        def bottom_line_score(s):
            heading = s.get("heading", "")
            content = s.get("content", "")
            if isinstance(content, list):
                content = "\n".join(content)
            score = 0
            for keyword in ["שורה תחתונה", "סיכום", "מבט קדימה", "מה זה אומר", "השלכות", "משמעות"]:
                if keyword in heading:
                    score += 100
            # Prose-like = few bullets, long sentences
            lines = [l.strip() for l in content.split("\n") if l.strip()]
            bullet_lines = sum(1 for l in lines if re.match(rf'^(\*|-|\$[A-Z]+:|{_BULLET_CHARS})', l))
            if bullet_lines <= 1 and len(content) > 100:
                score += 50
            # Prefer later sections as tiebreaker
            return score

        best_idx = max(range(len(sections)), key=lambda i: (bottom_line_score(sections[i]), i))
        bottom = sections[best_idx]
        rest = [s for i, s in enumerate(sections) if i != best_idx]

        print(f"  ✅ Consolidating {len(sections)} sections → 2")
        print(f"     Selected section {best_idx} ('{bottom.get('heading', '')}') as bottom line")

        merged_parts = []
        for s in rest:
            c = s.get("content", "")
            if isinstance(c, list):
                c = "\n".join(c)
            if c and c.strip():
                merged_parts.append(c.strip())
        merged = "\n".join(merged_parts)
        sections = [
            {"heading": first_heading, "content": merged},
            bottom,
        ]
    elif len(sections) == 1:
        print("  ⚠️ Only 1 section returned — appending empty bottom line")
        sections.append({"heading": "שורה תחתונה", "content": ""})

    # 4. Force headings
    original_h0 = sections[0].get("heading", "")
    original_h1 = sections[1].get("heading", "")
    sections[0]["heading"] = first_heading
    sections[1]["heading"] = "שורה תחתונה"
    if original_h0 != first_heading:
        print(f"  ✅ Section 1 heading overridden: '{original_h0}' → '{first_heading}'")
    if original_h1 != "שורה תחתונה":
        print(f"  ✅ Section 2 heading overridden: '{original_h1}' → 'שורה תחתונה' (enables gold box styling)")

    # 5. Normalize content — bullets in section 1, prose in section 2
    c0 = sections[0].get("content", "")
    if isinstance(c0, list):
        c0 = "\n".join(c0)
    sections[0]["content"] = normalize_bullets(c0)

    c1 = sections[1].get("content", "")
    if isinstance(c1, list):
        c1 = " ".join(c1)
    sections[1]["content"] = debullet(c1)

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
    r'(?:נסדק|נאסד"ק|Nasdaq)\s*100\s*[\-–:]\s*([\d,\.]+)': (18000, 30000, 'Nasdaq 100'),
    r'(?:נסדק|נאסד"ק|Nasdaq)\s*(?:קומפוזיט|Composite)\s*[\-–:]\s*([\d,\.]+)': (15000, 28000, 'Nasdaq Composite'),
    r'(?:דאו\s*ג\'?ונס|Dow\s*Jones?|DJIA)\s*[\-–:]\s*([\d,\.]+)': (30000, 55000, 'Dow Jones'),
    r'(?:ראסל|Russell)\s*2000\s*[\-–:]\s*([\d,\.]+)': (1500, 3500, 'Russell 2000'),
}

PCT_MAX_DAILY = 8.0
PCT_MAX_WEEKLY = 15.0

# ── Pre-market tense guards for daily_prep ──
# These apply ONLY when:
#   1. review_type == 'daily_prep'
#   2. AND current Israel time is BEFORE US market open (16:30 IL time) or briefing is for a future day
# Fix past-tense descriptions of market activity that hasn't happened yet.
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
    if now.weekday() >= 5:  # Weekend
        return False
    minutes = now.hour * 60 + now.minute
    return minutes < (16 * 60 + 30)

def apply_pre_market_tense_guard(result, review_type):
    """For daily_prep runs that finish BEFORE US market open, fix any accidental
    past-tense descriptions of market activity. The cash market hasn't opened yet."""
    if review_type != "daily_prep":
        return result

    now = datetime.now(ISR_TZ)
    if not is_before_us_market_open(now):
        return result  # Market already open — these phrases could legitimately be true

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
    """Flash-based fact check. Runs AFTER enforce_structure so the structure it sees is already correct."""
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
- DO NOT change the writing style, structure, section count, or section headings.
- DO NOT remove content — only fix errors or remove clearly-hallucinated numbers.
- DO NOT change the "title" field or section headings — those are already enforced.
- If everything is correct, return the review unchanged.

COMMON ERRORS:
- Donald Trump is the CURRENT US President (since Jan 2025). NOT a former president.
- Claude is by Anthropic, ChatGPT is by OpenAI, Gemini is by Google.
- IPO ≠ ETF.
- Contradictions: If bullets say market rose sharply, the bottom line must not say "mixed trading".

OUTPUT: Return the corrected review as valid JSON in EXACTLY the same structure (same title, same two section headings, same number of sections). No backticks, no explanations — pure JSON only."""

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

    # Finnhub market data
    is_weekly = REVIEW_TYPE in ("weekly_summary", "weekly_prep")
    market_data = fetch_market_data(weekly=is_weekly)

    # Economic calendar
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

    prompt = get_prompt(
        tweets, REVIEW_TYPE, date_str, day_name,
        title_date_str=title_date_str,
        title_day_name=title_day_name,
        week_range=week_range,
        is_trading=target_is_trading if REVIEW_TYPE == "daily_prep" else today_is_trading,
        market_data=market_data,
        prior_context=prior_context,
        expected_title=expected_title,
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

    # Layer 2: Structure enforcement (NEW) — forces title, section count, heading names, bullet format
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
