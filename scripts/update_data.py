"""
Wall Street IL — Hebrew financial market review pipeline.
Architecture: Gather → Ledger → Generate → Verify → Fix → Save.

Replaces the previous 6-layer ad-hoc validation with a single fact ledger.
Every claim in the final review must trace to a fact in the ledger, and
every numerical claim is verified against Finnhub.

Environment:
    GEMINI_API_KEY, TWITTER_API_KEY, FINNHUB_API_KEY, REVIEW_TYPE
"""

# ─── 1. Imports & Constants ─────────────────────────────────────────────

import os, sys, json, re, time
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests

GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY",  "")
TWITTER_API_KEY = os.environ.get("TWITTER_API_KEY", "")
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
REVIEW_TYPE     = os.environ.get("REVIEW_TYPE", "daily_prep")

ISR_TZ = ZoneInfo("Asia/Jerusalem")
UTC    = timezone.utc

ACCOUNTS = ["AIStockSavvy", "wallstengine", "DeIaone",
            "StockMKTNewz", "zerohedge",   "financialjuice"]

PY_TO_HEB_DAY = {0: "שני", 1: "שלישי", 2: "רביעי",
                 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}

US_HOLIDAYS_2026 = ["2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
                    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
                    "2026-11-26", "2026-12-25"]

# ─── Editable: live geopolitical context ────────────────────────────────
# Update this string when reality changes. Set to "" to disable.
GEO_CONTEXT = """ACTIVE GEOPOLITICAL CONTEXT (override softer framing in tweets):
- The United States is in an ACTIVE WAR with Iran since February 28, 2026.
- US Navy is running Operation "Project Freedom" in the Strait of Hormuz.
- The Strait is intermittently closed; oil shipments are disrupted.
- This is NOT "tensions" or "escalation" — it is a war with active operations.
- USE: מלחמה, מבצע צבאי, תקיפה, עימות מזוין.
- DO NOT USE: מתיחות, הסלמה דיפלומטית, חששות גיאופוליטיים."""

# ─── Editable: index level sanity ranges ────────────────────────────────
# If review claims "<index> at <level>", level must fall in range. Update
# every few weeks as markets move. Catches the "Nasdaq 100 = 25,838" error.
INDEX_RANGES = {
    "S&P 500":          (6800, 7800),
    "Nasdaq 100":       (27000, 30000),
    "Nasdaq Composite": (24000, 27000),
    "Dow Jones":        (47000, 51000),
    "Russell 2000":     (2700, 3000),
}

# Hebrew names → canonical English key (for index lookup)
INDEX_NAME_MAP = {
    "S&P 500":      "S&P 500",
    "S&P":          "S&P 500",
    "סנופי":        "S&P 500",
    "אס אנד פי":    "S&P 500",
    "נאסד\"ק 100":  "Nasdaq 100",
    "Nasdaq 100":   "Nasdaq 100",
    "NDX":          "Nasdaq 100",
    "נאסד\"ק קומפוזיט": "Nasdaq Composite",
    "Nasdaq Composite": "Nasdaq Composite",
    "הנאסד\"ק":     "Nasdaq Composite",  # ambiguous, default to composite
    "דאו":          "Dow Jones",
    "דאו ג'ונס":    "Dow Jones",
    "Dow":          "Dow Jones",
    "Dow Jones":    "Dow Jones",
    "ראסל 2000":    "Russell 2000",
    "Russell 2000": "Russell 2000",
}

# Section headings per review type (Hebrew)
SECTION_HEADINGS = {
    "daily_prep":     ["מה קרה אתמול / בלילה", "מה יזיז את השוק היום"],
    "daily_summary":  ["סיכום המסחר", "מה זה אומר ליום הבא"],
    "weekly_prep":    ["הנושא המרכזי של השבוע", "יומן השבוע"],
    "weekly_summary": ["סיכום השבוע", "מבט לשבוע הבא"],
    "live_news":      ["חדשות אחרונות"],
}

# JSON top-level key per review type (matches existing data.json schema)
JSON_KEY = {
    "daily_prep":     "dailyPrep",
    "daily_summary":  "dailySummary",
    "weekly_prep":    "weeklyPrep",
    "weekly_summary": "weeklySummary",
    "events":         "events",
    "live_news":      "liveNews",
}

# Tickers to skip during direction verification (already covered as ETFs,
# or generic acronyms picked up by $TICKER regex)
TICKER_EXCLUDE = {
    "SPY","QQQ","DIA","IWM","USO","BNO","GLD","SLV","IBIT","TLT","UUP","VIXY",
    "XLE","XLK","XLF","XLY","XLV","XLI","XLP","XLU",
    "USD","EUR","GBP","JPY","CHF","CNY","INR","USA","EU","UK","ET","ETF","IPO",
    "AI","ML","LLM","EPS","EBITDA","FY","VIX","DXY","SPX","NDX","DJI","RUT",
    "CEO","CFO","CTO","COO","FED","ECB","BOJ","GDP","CPI","PPI","PMI","ISM",
    "NFP","FOMC","JOLTS","ADP",
}

# ETFs to fetch from Finnhub for the market snapshot
SNAPSHOT_ETFS = {
    "SPY": "S&P 500",      "QQQ": "Nasdaq 100",   "DIA": "Dow Jones",
    "IWM": "Russell 2000", "USO": "WTI oil",      "BNO": "Brent oil",
    "GLD": "Gold",         "SLV": "Silver",       "IBIT": "Bitcoin",
    "TLT": "20Y Treasury", "UUP": "Dollar (DXY)", "VIXY": "VIX",
    "XLE": "Energy",       "XLK": "Technology",   "XLF": "Financials",
    "XLY": "Cons. Disc.",  "XLV": "Healthcare",   "XLI": "Industrials",
}


# ─── 2. Time, Sessions, Metadata ─────────────────────────────────────────

def now_il():
    return datetime.now(ISR_TZ)

def is_us_trading_day(date_il):
    if date_il.weekday() in (4, 5):  # Fri (after IL pivot), Sat
        return False
    if date_il.strftime("%Y-%m-%d") in US_HOLIDAYS_2026:
        return False
    return True

def get_us_market_session(now=None):
    """Returns 'closed' / 'premarket' / 'open' / 'afterhours' (IL time perspective)."""
    n = now or now_il()
    wd = n.weekday()
    if wd == 5 or (wd == 6 and n.hour < 16):  # Sat all day, Sun morning before pre-market
        return "closed"
    if n.strftime("%Y-%m-%d") in US_HOLIDAYS_2026:
        return "closed"
    mins = n.hour * 60 + n.minute
    if mins < 16*60 + 30:  return "premarket"
    if mins < 23*60:       return "open"
    return "afterhours"

def get_review_metadata(review_type, now=None):
    """Build {title, date_str, day_name_he, week_range, target_date} for a review."""
    n = now or now_il()
    today = n.date()
    out = {"now": n, "today": today, "today_str": today.isoformat(),
           "day_he": PY_TO_HEB_DAY[n.weekday()]}

    if review_type == "daily_prep":
        # Briefing for the next trading day
        target = today
        # If we're past pre-market, briefing is for tomorrow's session
        if get_us_market_session(n) in ("open", "afterhours"):
            target = today + timedelta(days=1)
        # Skip weekends/holidays
        for _ in range(7):
            if is_us_trading_day(datetime.combine(target, datetime.min.time(), tzinfo=ISR_TZ)):
                break
            target += timedelta(days=1)
        out["target"] = target
        out["target_str"] = target.isoformat()
        out["target_day_he"] = PY_TO_HEB_DAY[target.weekday()]
        out["title"] = f"הכנה ליום מסחר בוול סטריט 🇺🇸 – יום {out['target_day_he']} {target.isoformat()}"

    elif review_type == "daily_summary":
        # Summary of the most recent trading day
        target = today
        for _ in range(7):
            if is_us_trading_day(datetime.combine(target, datetime.min.time(), tzinfo=ISR_TZ)):
                break
            target -= timedelta(days=1)
        out["target"] = target
        out["target_str"] = target.isoformat()
        out["target_day_he"] = PY_TO_HEB_DAY[target.weekday()]
        out["title"] = f"סיכום יום המסחר בוול סטריט 🇺🇸 – יום {out['target_day_he']} {target.isoformat()}"

    elif review_type == "weekly_prep":
        # Sun afternoon: prep for the upcoming week
        mon = today + timedelta(days=(7 - today.weekday()) % 7 or 1)
        if today.weekday() == 6:  # Sunday → next day is Monday
            mon = today + timedelta(days=1)
        fri = mon + timedelta(days=4)
        out["week_range"] = (mon, fri)
        out["title"] = f"הכנה לשבוע מסחר – {mon.strftime('%d/%m')}–{fri.strftime('%d/%m/%Y')}"

    elif review_type == "weekly_summary":
        # Sat 01:00 IL: summary of week that just ended
        # Find the most recent Friday
        days_back = (today.weekday() - 4) % 7 or 7
        fri = today - timedelta(days=days_back)
        mon = fri - timedelta(days=4)
        out["week_range"] = (mon, fri)
        out["title"] = f"סיכום שבועי – {mon.strftime('%d/%m')}–{fri.strftime('%d/%m/%Y')}"

    elif review_type == "events":
        out["title"] = f"לוח אירועים כלכליים – {today.isoformat()}"

    elif review_type == "live_news":
        out["title"] = f"מה קורה עכשיו – {n.strftime('%H:%M')} (זמן ישראל) {today.isoformat()}"

    return out


# ─── 3. Data Gathering ──────────────────────────────────────────────────

def fetch_tweets(hours_back=24):
    """Pull tweets from configured accounts via twitterapi.io. Returns text bundle."""
    if not TWITTER_API_KEY:
        return ""
    cutoff = datetime.now(UTC) - timedelta(hours=hours_back)
    bundles = []
    for handle in ACCOUNTS:
        try:
            r = requests.get(
                "https://api.twitterapi.io/twitter/user/last_tweets",
                params={"userName": handle},
                headers={"X-API-Key": TWITTER_API_KEY},
                timeout=15,
            )
            if not r.ok:
                continue
            data = r.json()
            tweets = data.get("data", {}).get("tweets", []) or data.get("tweets", []) or []
            for t in tweets[:30]:
                created = t.get("createdAt") or t.get("created_at") or ""
                try:
                    ts = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
                except Exception:
                    try:
                        ts = datetime.fromisoformat(created.replace("Z","+00:00"))
                    except Exception:
                        ts = None
                if ts and ts < cutoff:
                    continue
                txt = (t.get("text") or "").strip()
                if not txt:
                    continue
                bundles.append(f"@{handle} [{created}]: {txt}")
        except Exception as e:
            print(f"  Tweet fetch error for {handle}: {e}")
    print(f"  Fetched {len(bundles)} tweets from {len(ACCOUNTS)} accounts")
    return "\n\n".join(bundles)


def fetch_finnhub_quote(symbol):
    """Returns {price, pct, prev_close} or None."""
    if not FINNHUB_API_KEY:
        return None
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}",
            timeout=8,
        )
        if not r.ok:
            return None
        d = r.json()
        c, dp, pc = d.get("c", 0) or 0, d.get("dp", 0) or 0, d.get("pc", 0) or 0
        if c <= 0 or pc <= 0:
            return None
        return {"price": float(c), "pct": float(dp), "prev_close": float(pc)}
    except Exception:
        return None


def fetch_market_snapshot():
    """Pull current prices for all tracked ETFs. Returns dict for prompt + verifier."""
    snapshot = {}
    for sym, label in SNAPSHOT_ETFS.items():
        q = fetch_finnhub_quote(sym)
        if q:
            snapshot[sym] = {**q, "label": label}
    print(f"  Market snapshot: {len(snapshot)}/{len(SNAPSHOT_ETFS)} ETFs fetched")
    return snapshot


def extract_potential_tickers(text):
    """Find $TICKER mentions, exclude known false positives."""
    out = set()
    for m in re.finditer(r'\$([A-Z]{1,5})\b', text or ""):
        sym = m.group(1)
        if sym not in TICKER_EXCLUDE:
            out.add(sym)
    return out


def fetch_ticker_quotes(tickers):
    """For a set of tickers (e.g. extracted from tweets), fetch each quote."""
    out = {}
    for t in tickers:
        q = fetch_finnhub_quote(t)
        if q:
            out[t] = q
    print(f"  Ticker quotes: {len(out)}/{len(tickers)} fetched")
    return out


# ─── 4. Gemini API (Flash + Pro) ─────────────────────────────────────────

def call_gemini(prompt, model="gemini-2.5-pro", temperature=0.2,
                max_tokens=4096, retries=2):
    """Call Gemini and return raw text. Retries on transient failures."""
    if not GEMINI_API_KEY:
        return ""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
    }
    for attempt in range(retries + 1):
        try:
            r = requests.post(url, headers={"Content-Type": "application/json"},
                              json=body, timeout=90)
            if not r.ok:
                if attempt < retries:
                    time.sleep(3 * (attempt + 1))
                    continue
                print(f"  Gemini {model} error {r.status_code}: {r.text[:300]}")
                return ""
            data = r.json()
            cand = (data.get("candidates") or [{}])[0]
            parts = cand.get("content", {}).get("parts", [])
            text = "".join(p.get("text", "") for p in parts)
            if not text and attempt < retries:
                time.sleep(3)
                continue
            return text.strip()
        except Exception as e:
            if attempt < retries:
                time.sleep(3 * (attempt + 1))
                continue
            print(f"  Gemini call exception: {e}")
            return ""
    return ""


def extract_json(text):
    """Pull the first complete JSON object/array from text. Returns dict/list or None."""
    if not text:
        return None
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```\s*$", "", s)
    # Find first { or [
    start = -1
    for i, ch in enumerate(s):
        if ch in "{[":
            start = i
            break
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch == '"' and not esc:
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch in "{[":
            depth += 1
        elif ch in "}]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(s[start:i+1])
                except json.JSONDecodeError:
                    return None
    return None


# ─── 5. Fact Ledger (the core innovation) ────────────────────────────────

def build_fact_ledger(tweets, market_snapshot, ticker_quotes, review_type):
    """
    Single Flash call that produces:
      - facts: atomic claims with source + tickers + numbers + direction
      - stories: top 5-7 stories with cannot_miss angles

    The ledger is the AUTHORITATIVE source for the writer. Anything not in
    the ledger should not appear in the review.
    """
    market_lines = []
    for sym, d in market_snapshot.items():
        market_lines.append(f"[{sym} ({d['label']})] price={d['price']:.2f} change={d['pct']:+.2f}%")
    quote_lines = []
    for t, q in ticker_quotes.items():
        quote_lines.append(f"[${t}] price={q['price']:.2f} change={q['pct']:+.2f}%")

    market_block = "\n".join(market_lines) if market_lines else "(none)"
    quote_block = "\n".join(quote_lines) if quote_lines else "(none)"
    geo_block = GEO_CONTEXT if GEO_CONTEXT else ""

    prompt = f"""You are a financial-fact extractor. Your output is a STRUCTURED LEDGER that another writer will use to compose a Hebrew market review. Your job is editorial discipline — extract every relevant atomic fact with its source, then rank the top stories.

{geo_block}

INPUT 1 — TWEETS (numbered for source-tracing):
{tweets[:14000] if tweets else "(none)"}

INPUT 2 — VERIFIED MARKET DATA from Finnhub (these are GROUND TRUTH; any contradiction in tweets is the tweet's error):
{market_block}

INPUT 3 — VERIFIED TICKER QUOTES from Finnhub:
{quote_block}

YOUR TASK — produce JSON with two arrays: "facts" and "stories".

FACTS — atomic, single-claim, source-traced. Each fact must be a single concrete event/number/quote, not a synthesis. Format:
{{
  "id": "F1",
  "claim": "Brief description of the fact, in English ok",
  "source": "tweet:N" or "finnhub" or "market_data",
  "tickers": ["AAPL", ...],
  "key_numbers": ["$55.5B", "+1.46%", "147.83"],
  "direction": "up" / "down" / "neutral" / null  (for ticker price moves only),
  "category": "earnings" / "macro" / "geopolitics" / "M&A" / "analyst" / "central_bank" / "company_news" / "other"
}}

STORIES — the top 5-7 most important, ranked. Each story may aggregate multiple facts. Format:
{{
  "rank": 1,
  "headline": "Brief description (English ok)",
  "fact_ids": ["F1", "F3"],
  "tickers": ["GME", "EBAY"],
  "cannot_miss": "The SPECIFIC angle/number/connection the review must include. Examples: 'Burry sold GME because of the eBay deal — link them.' / 'PLTR after-hours was VOLATILE: initial pop, then sold off to -2% — don't only say jumped.' / 'HSBC miss was driven primarily by $400M UK fraud, NOT Middle East ($300M).' Must be specific, not generic."
}}

CRITICAL RULES:
- A fact's "direction" for a ticker MUST match the Finnhub quote, NOT a tweet's claim.
- If tweets and Finnhub disagree on a number, Finnhub wins.
- If a story is purely speculation/rumor, exclude it.
- "cannot_miss" must catch counter-narrative angles — the part the writer would otherwise skip to make the story sound cleaner.
- Skip stories already covered exhaustively in past reviews; focus on what's NEW.

Return ONLY valid JSON, no preamble, no backticks:
{{"facts": [...], "stories": [...]}}"""

    print("  Calling Gemini Flash for fact ledger...")
    raw = call_gemini(prompt, model="gemini-2.5-flash", temperature=0.1, max_tokens=8192)
    parsed = extract_json(raw)
    if not parsed or not isinstance(parsed, dict):
        print("  Ledger parse failed — returning empty ledger")
        return {"facts": [], "stories": []}
    facts = parsed.get("facts") or []
    stories = parsed.get("stories") or []
    print(f"  Ledger: {len(facts)} facts, {len(stories)} stories")
    for s in stories[:3]:
        print(f"    #{s.get('rank','?')}: {s.get('headline','')[:90]}")
    return {"facts": facts, "stories": stories}


# ─── 6. Review Generation ───────────────────────────────────────────────

def format_ledger_for_prompt(ledger):
    lines = ["LEDGER OF VERIFIED FACTS — these are the only facts you may use:", ""]
    for f in ledger.get("facts", []):
        tickers = " ".join(f"${t}" for t in f.get("tickers", []) or []) or "—"
        nums = ", ".join(f.get("key_numbers", []) or []) or "—"
        direction = f.get("direction") or "—"
        lines.append(f"[{f.get('id')}] ({f.get('category', '?')}) {f.get('claim', '')}")
        lines.append(f"   source={f.get('source','?')}  tickers={tickers}  numbers={nums}  direction={direction}")
        lines.append("")
    lines.append("")
    lines.append("TOP STORIES (must be covered in priority order — top 3 mandatory):")
    for s in ledger.get("stories", []):
        lines.append(f"#{s.get('rank','?')}: {s.get('headline','')}")
        lines.append(f"   facts: {s.get('fact_ids', [])}")
        lines.append(f"   CANNOT-MISS ANGLE: {s.get('cannot_miss','(none)')}")
        lines.append("")
    return "\n".join(lines)


def build_review_prompt(ledger, metadata, review_type):
    headings = SECTION_HEADINGS.get(review_type, ["נקודות מרכזיות"])
    n_sections = len(headings)
    expected_title = metadata.get("title", "")

    style_note = """STYLE — financial journalism, Hebrew. Each section: 8-10 dense bullets, each opens with a bold lead phrase, then the substance. Concrete numbers and tickers. No filler, no hedging. Treat readers as professionals."""

    structure_note = f"""STRUCTURE — exactly {n_sections} section(s) with these headings, in this order:
{chr(10).join(f'  {i+1}. "{h}"' for i, h in enumerate(headings))}

Output JSON ONLY:
{{
  "title": "{expected_title}",
  "date": "{metadata.get('today_str', metadata.get('target_str',''))}",
  "sections": [
    {{"heading": "{headings[0]}", "content": "* bullet 1\\n* bullet 2\\n* ..."}}
"""
    if n_sections > 1:
        structure_note += f',\n    {{"heading": "{headings[1]}", "content": "..."}}'
    structure_note += "\n  ]\n}"

    geo = GEO_CONTEXT if GEO_CONTEXT else ""

    ledger_block = format_ledger_for_prompt(ledger)

    prompt = f"""You are writing a Hebrew financial market review for Israeli investors.

{geo}

{ledger_block}

{style_note}

{structure_note}

CRITICAL DISCIPLINE:
- Use ONLY facts from the ledger above. If it's not in the ledger, do not write it.
- For top-3 stories: cover each one. If you cover a story, you MUST include its CANNOT-MISS ANGLE — that's the editorial line that prevents half-truth coverage.
- For ticker price moves: the direction in your bullet MUST match the direction in the fact (which was verified against Finnhub). Never write that a stock is up if the ledger says down.
- For index levels: only state a level if the ledger contains it. Never invent or estimate.
- Hebrew. Tickers in $ form (e.g. $AAPL). Numbers with comma separators where appropriate.

Return the JSON object now, no backticks, no preamble."""
    return prompt


def generate_review(ledger, metadata, review_type):
    if review_type == "events":
        return generate_events(ledger, metadata)
    prompt = build_review_prompt(ledger, metadata, review_type)
    print(f"  Calling Gemini Pro to generate {review_type}...")
    raw = call_gemini(prompt, model="gemini-2.5-pro", temperature=0.25, max_tokens=8192)
    review = extract_json(raw)
    if not review:
        print("  Generation parse failed — retrying once with stricter prompt...")
        raw = call_gemini(prompt + "\n\nReturn ONLY valid JSON. No prose, no backticks.",
                          model="gemini-2.5-pro", temperature=0.1, max_tokens=8192)
        review = extract_json(raw)
    if not review:
        return None
    # Force the title to match expected (Gemini sometimes rewrites it)
    review["title"] = metadata.get("title", review.get("title",""))
    review["date"] = metadata.get("today_str", metadata.get("target_str", ""))
    return review


def generate_events(ledger, metadata):
    """Events page is a calendar of upcoming items, not a narrative review."""
    today = metadata["today"]
    facts = ledger.get("facts", [])
    macro_facts = [f for f in facts if f.get("category") in ("macro", "central_bank", "earnings", "geopolitics")]
    facts_summary = "\n".join(f"- {f.get('claim','')} (cat={f.get('category','?')})" for f in macro_facts[:30])

    prompt = f"""Generate a calendar of UPCOMING economic and market-moving events for the next 7 days starting {today.isoformat()}.

Use these facts and known event categories from the ledger:
{facts_summary}

Also include the standard well-known scheduled events (CPI, FOMC, NFP, ISM, JOLTS, ADP, big earnings) for the next 7 days based on your training data and the current week.

Output JSON:
{{
  "items": [
    {{"time": "ISO 8601 with timezone, e.g. 2026-05-07T17:00:00+03:00",
      "title": "Hebrew title of event",
      "impact": "high" / "medium" / "low",
      "description": "Brief Hebrew explanation, 1-2 sentences"}}
  ]
}}

Sort by time ascending. 8-15 items. Return JSON only."""
    raw = call_gemini(prompt, model="gemini-2.5-pro", temperature=0.2, max_tokens=4096)
    parsed = extract_json(raw)
    if not parsed:
        return {"items": []}
    return {"items": parsed.get("items", [])}


# ─── 7. Verification ────────────────────────────────────────────────────

DIR_UP = ["עולה","עולים","עולות","עלתה","עלה","עלו","עלייה","עליות","בעלייה",
          "מטפס","מטפסת","מטפסים","טיפס","טיפסה","טיפסו",
          "מזנק","מזנקת","מזנקים","זינק","זינקה","זינקו",
          "קופץ","קופצת","קופצים","קפץ","קפצה","קפצו",
          "מתחזק","מתחזקת","התחזק","התחזקה",
          "ירוק","בירוק","מוסיפה","מוסיף","הוסיפה","הוסיף"]
DIR_DOWN = ["יורד","יורדת","יורדים","ירד","ירדה","ירדו","ירידה","ירידות","בירידה",
            "נופל","נופלת","נופלים","נפל","נפלה","נפלו",
            "צונח","צונחת","צונחים","צנח","צנחה","צנחו","צניחה",
            "נחלש","נחלשת","נחלשים","נחלשה",
            "אדום","באדום","מאבד","מאבדת","מאבדים","איבד","איבדה","איבדו"]

def detect_direction(text):
    has_up = any(re.search(rf'(?<!\w){re.escape(t)}(?!\w)', text) for t in DIR_UP)
    has_down = any(re.search(rf'(?<!\w){re.escape(t)}(?!\w)', text) for t in DIR_DOWN)
    if has_up and not has_down: return "up"
    if has_down and not has_up: return "down"
    return None


def iter_bullets(review):
    for sec in review.get("sections", []) or []:
        c = sec.get("content","")
        if isinstance(c, list): c = "\n".join(c)
        for line in (c or "").split("\n"):
            line = line.strip()
            if line.startswith(("*","-","•")):
                yield line.lstrip("*-• ").strip(), sec.get("heading","")


def verify_review(review, ledger, ticker_quotes, market_snapshot):
    """Single verification pass. Returns list of issues for the fixer."""
    issues = []
    if not isinstance(review, dict):
        return [{"type": "fatal", "msg": "Review is not a dict"}]

    # 1. Per-ticker direction (sign-flip) check
    for bullet, heading in iter_bullets(review):
        for m in re.finditer(r'\$([A-Z]{1,5})\b', bullet):
            t = m.group(1)
            if t in TICKER_EXCLUDE or t not in ticker_quotes:
                continue
            claimed = detect_direction(bullet)
            if not claimed:
                continue
            pct = ticker_quotes[t]["pct"]
            actual = "up" if pct > 0.3 else "down" if pct < -0.3 else "flat"
            if actual != "flat" and claimed != actual:
                issues.append({"type": "sign_flip", "ticker": t,
                               "claimed": claimed, "actual": f"{pct:+.2f}%",
                               "bullet": bullet, "section": heading})

    # 2. Index level guard (the new check, catches Nasdaq 100 = 25,838)
    # Build raw text without JSON escaping that would munge the Hebrew quotes
    raw_chunks = []
    if isinstance(review.get("title"), str):
        raw_chunks.append(review["title"])
    for sec in review.get("sections", []) or []:
        if isinstance(sec.get("heading"), str):
            raw_chunks.append(sec["heading"])
        c = sec.get("content", "")
        if isinstance(c, list): c = "\n".join(c)
        if isinstance(c, str):
            raw_chunks.append(c)
    full_text = "\n".join(raw_chunks)
    # Pattern: "<index name> ... <number with optional , and .> ... נקודות"
    seen = set()
    for he_name, canon in INDEX_NAME_MAP.items():
        for m in re.finditer(re.escape(he_name) + r'.{0,80}?([\d][\d,.]{3,})[^\n]{0,15}?נקודות', full_text):
            num_str = m.group(1).replace(",", "")
            try:
                level = int(float(num_str))
            except ValueError:
                continue
            key = (canon, level)
            if key in seen:
                continue
            seen.add(key)
            if canon in INDEX_RANGES:
                lo, hi = INDEX_RANGES[canon]
                if not (lo <= level <= hi):
                    issues.append({"type": "index_level",
                                   "index": canon, "claimed_level": level,
                                   "expected_range": f"{lo}-{hi}",
                                   "context": m.group(0)[:300]})

    # 3. Top-3 stories must be covered
    stories = ledger.get("stories", [])
    full_text_lower = full_text.lower()
    for s in stories[:3]:
        tickers = s.get("tickers", []) or []
        if tickers and any(t.lower() in full_text_lower for t in tickers):
            continue
        headline_words = [w for w in re.findall(r"[A-Za-z]{4,}", s.get("headline",""))]
        if any(w.lower() in full_text_lower for w in headline_words):
            continue
        issues.append({"type": "missing_story", "rank": s.get("rank"),
                       "headline": s.get("headline",""),
                       "cannot_miss": s.get("cannot_miss",""),
                       "tickers": tickers})

    # 4. Cannot-miss angles must appear when story is covered
    for s in stories:
        tickers = s.get("tickers", []) or []
        if not tickers: continue
        story_in_review = any(t.lower() in full_text_lower for t in tickers)
        if not story_in_review: continue
        cm = s.get("cannot_miss","").strip()
        if not cm or len(cm) < 15: continue
        keywords = re.findall(r"[A-Za-zא-ת]{4,}", cm)
        content_kws = [k for k in keywords if k.lower() not in
                       {"the","this","that","with","from","were","been","will","when",
                        "have","they","than","more","most","into","because","direct",
                        "their","other","what","which","while","about","could","should"}]
        if not content_kws: continue
        hits = sum(1 for k in content_kws if k.lower() in full_text_lower)
        if hits < max(2, len(content_kws) // 4):
            issues.append({"type": "missing_cannot_miss", "story": s.get("headline",""),
                           "cannot_miss": cm, "tickers": tickers})

    return issues


def fix_issues_with_flash(review, issues, ledger, ticker_quotes):
    """Single Flash call that fixes all detected issues at once."""
    if not issues:
        return review
    issue_lines = ["ISSUES TO FIX (each requires a specific edit):"]
    for i, iss in enumerate(issues[:20], 1):
        t = iss["type"]
        if t == "sign_flip":
            issue_lines.append(f"{i}. SIGN-FLIP: ${iss['ticker']} bullet claims '{iss['claimed']}' but Finnhub shows {iss['actual']}.")
            issue_lines.append(f"   Bullet: {iss['bullet'][:300]}")
            issue_lines.append(f"   FIX: Either rewrite with correct direction+number, OR remove the bullet if it was only about price.")
        elif t == "index_level":
            issue_lines.append(f"{i}. INDEX-LEVEL MISMATCH: review says {iss['index']} at {iss['claimed_level']}, but realistic range is {iss['expected_range']}.")
            issue_lines.append(f"   Context: {iss['context'][:200]}")
            issue_lines.append(f"   FIX: Either correct the level, fix the index name (Nasdaq Composite vs Nasdaq 100 are different!), or remove.")
        elif t == "missing_story":
            issue_lines.append(f"{i}. MISSING TOP STORY (rank #{iss['rank']}): {iss['headline']}")
            issue_lines.append(f"   Tickers: {iss['tickers']}")
            issue_lines.append(f"   CANNOT-MISS: {iss['cannot_miss']}")
            issue_lines.append(f"   FIX: Add a bullet covering this story with the cannot-miss angle.")
        elif t == "missing_cannot_miss":
            issue_lines.append(f"{i}. CANNOT-MISS ANGLE MISSING: story '{iss['story']}' is covered but its key angle is not.")
            issue_lines.append(f"   ANGLE: {iss['cannot_miss']}")
            issue_lines.append(f"   FIX: Edit the bullet about this story to include the angle.")
    issue_block = "\n".join(issue_lines)

    quote_block = "\n".join(f"${t} {q['pct']:+.2f}%" for t,q in ticker_quotes.items())

    prompt = f"""You are fixing specific issues in a Hebrew market review. Make ONLY the fixes listed. Do not change anything else — keep all working bullets, structure, headings, title, date.

VERIFIED TICKER QUOTES (ground truth):
{quote_block}

REVIEW JSON:
{json.dumps(review, ensure_ascii=False, indent=2)}

{issue_block}

Return the corrected review as the same JSON structure (same title, same headings, same number of sections). No backticks. No preamble. Just JSON."""
    print(f"  Fixing {len(issues)} issues with Flash...")
    raw = call_gemini(prompt, model="gemini-2.5-flash", temperature=0.1, max_tokens=8192)
    fixed = extract_json(raw)
    if not fixed or not isinstance(fixed, dict):
        print("  Fix parse failed — returning original review")
        return review
    # Preserve title/date/heading
    fixed["title"] = review.get("title", fixed.get("title",""))
    fixed["date"] = review.get("date", fixed.get("date",""))
    return fixed


# ─── 8. Output ──────────────────────────────────────────────────────────

def load_data_json():
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"marketStatus": {"usHolidays2026": US_HOLIDAYS_2026}}


def save_data_json(data):
    data["lastUpdated"] = now_il().isoformat()
    if "marketStatus" not in data:
        data["marketStatus"] = {"usHolidays2026": US_HOLIDAYS_2026}
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved data.json ({sum(len(json.dumps(v)) for v in data.values())} bytes)")


def commit_review(review, review_type, metadata):
    data = load_data_json()
    key = JSON_KEY[review_type]

    if review_type == "events":
        data[key] = {"lastUpdated": now_il().isoformat(),
                     "items": review.get("items", [])}
    else:
        data[key] = {
            "title": review.get("title", metadata.get("title","")),
            "date":  review.get("date",  metadata.get("today_str","")),
            "sections": review.get("sections", []),
        }
    save_data_json(data)


# ─── 9. Main Pipeline ───────────────────────────────────────────────────

def main():
    print(f"\n=== Wall Street IL: {REVIEW_TYPE} @ {now_il().isoformat()} ===\n")

    if not GEMINI_API_KEY:
        print("ERROR: GEMINI_API_KEY missing")
        sys.exit(1)
    if REVIEW_TYPE not in JSON_KEY:
        print(f"ERROR: unknown REVIEW_TYPE={REVIEW_TYPE}")
        sys.exit(1)

    metadata = get_review_metadata(REVIEW_TYPE)
    print(f"  Title: {metadata.get('title','(no title)')}")

    # ── Phase 1: GATHER
    print("\n[1/5] Gather data")
    hours_back = 72 if REVIEW_TYPE in ("weekly_prep","weekly_summary") else 24
    tweets = fetch_tweets(hours_back=hours_back)
    snapshot = fetch_market_snapshot()
    candidate_tickers = extract_potential_tickers(tweets)
    quotes = fetch_ticker_quotes(candidate_tickers)

    # ── Phase 2: LEDGER
    print("\n[2/5] Build fact ledger")
    ledger = build_fact_ledger(tweets, snapshot, quotes, REVIEW_TYPE)

    # ── Phase 3: GENERATE
    print("\n[3/5] Generate review")
    review = generate_review(ledger, metadata, REVIEW_TYPE)
    if not review:
        print("ERROR: generation failed")
        sys.exit(1)

    # ── Phase 4: VERIFY
    print("\n[4/5] Verify")
    if REVIEW_TYPE != "events":
        # Re-fetch quotes for tickers actually used in the review (may be new ones)
        used_tickers = extract_potential_tickers(json.dumps(review, ensure_ascii=False))
        new_tickers = used_tickers - set(quotes.keys())
        if new_tickers:
            print(f"  Fetching {len(new_tickers)} additional ticker quotes from review")
            quotes.update(fetch_ticker_quotes(new_tickers))
        issues = verify_review(review, ledger, quotes, snapshot)
        if issues:
            print(f"  Found {len(issues)} issues:")
            for iss in issues[:10]:
                print(f"    [{iss['type']}] {str(iss)[:200]}")
            review = fix_issues_with_flash(review, issues, ledger, quotes)
            # One more verification pass
            issues2 = verify_review(review, ledger, quotes, snapshot)
            print(f"  After fix: {len(issues2)} remaining issues")
        else:
            print("  ✓ No issues detected")

    # ── Phase 5: SAVE
    print("\n[5/5] Save")
    commit_review(review, REVIEW_TYPE, metadata)
    print(f"\n=== Done: {REVIEW_TYPE} ===\n")


if __name__ == "__main__":
    main()
