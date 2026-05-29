import json, os, re, requests, sys
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

# Use real Israel timezone, including daylight-saving changes.
ISR_TZ = ZoneInfo("Asia/Jerusalem") if ZoneInfo else timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

PY_TO_HEB = {0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}

# ══════════════════════════════════════════════════════════════
# ACTIVE GEOPOLITICAL CONTEXT — optional, externally supplied only.
# Reliability rule: do NOT hard-code geopolitical facts in the script.
# If a real, current geopolitical context must be injected, set it in the
# GitHub Action / environment as ACTIVE_GEOPOLITICAL_CONTEXT.
# Otherwise leave empty so the review relies only on current sources.
# ══════════════════════════════════════════════════════════════
ACTIVE_GEOPOLITICAL_CONTEXT = os.environ.get("ACTIVE_GEOPOLITICAL_CONTEXT", "").strip()

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


# Deterministic market-direction layer.
# X remains the news/narrative source. Price direction must come from verified market data.
_DIRECTION_ASSETS = {
    "oil": {
        "symbols": ["USO", "BNO"],
        "label": "נפט (WTI/Brent proxies)",
        "terms": ["נפט", "WTI", "Brent", "ברנט", "crude", "oil"],
    },
    "gold": {
        "symbols": ["GLD"],
        "label": "זהב",
        "terms": ["זהב", "gold"],
    },
    "bitcoin": {
        "symbols": ["IBIT"],
        "label": "ביטקוין",
        "terms": ["ביטקוין", "bitcoin", "BTC", "IBIT"],
    },
    "dollar": {
        "symbols": ["UUP"],
        "label": "דולר",
        "terms": ["דולר", "DXY", "UUP"],
    },
    "vix": {
        "symbols": ["VIXY"],
        "label": "תנודתיות / VIX",
        "terms": ["VIX", "תנודתיות", "VIXY"],
    },
    "long_bonds": {
        "symbols": ["TLT"],
        "label": "אג\"ח ארוכות / TLT",
        "terms": ["TLT", "אג\"ח", "אגח", "Treasury", "תשואות"],
    },
}

_UP_WORDS = [
    "עולה", "עולים", "עלו", "עלה", "עלייה", "בעלייה", "מטפס", "מטפסים", "טיפס", "טיפסו",
    "מזנק", "מזנקים", "זינק", "זינקו", "קופץ", "קופצים", "התחזק", "התחזקו", "מתחזק", "מתחזקים",
    "מתאושש", "מתאוששת", "מתאוששים", "התאושש", "התאוששה", "התאוששו", "התאוששות", "ריבאונד"
]
_DOWN_WORDS = [
    "יורד", "יורדים", "ירד", "ירדו", "ירידה", "בירידה", "נופל", "נופלים", "נפל", "נפלו",
    "צונח", "צונחים", "צנח", "צנחו", "נחלש", "נחלשו", "נחלשת", "נחלשים", "מאבד", "מאבדים", "איבד", "איבדו"
]
_NEUTRAL_WORDS = ["יציב", "יציבים", "ללא שינוי", "מדשדש", "מדשדשים"]

_DIRECTION_REPLACEMENTS_UP = {
    "צונחים": "עולים", "צונח": "עולה", "צנחו": "עלו", "צנח": "עלה",
    "יורדים": "עולים", "יורד": "עולה", "ירדו": "עלו", "ירד": "עלה", "ירידה": "עלייה", "בירידה": "בעלייה",
    "נופלים": "עולים", "נופל": "עולה", "נפלו": "עלו", "נפל": "עלה",
    "נחלשים": "מתחזקים", "נחלש": "התחזק", "נחלשו": "התחזקו", "נחלשת": "מתחזקת",
    "מאבדים": "מוסיפים", "מאבד": "מוסיף", "איבדו": "הוסיפו", "איבד": "הוסיף",
}
_DIRECTION_REPLACEMENTS_DOWN = {
    "מזנקים": "יורדים", "מזנק": "יורד", "זינקו": "ירדו", "זינק": "ירד",
    "עולים": "יורדים", "עולה": "יורד", "עלו": "ירדו", "עלה": "ירד", "עלייה": "ירידה", "בעלייה": "בירידה",
    "מטפסים": "יורדים", "מטפס": "יורד", "טיפסו": "ירדו", "טיפס": "ירד",
    "קופצים": "יורדים", "קופץ": "יורד", "התחזקו": "נחלשו", "התחזק": "נחלש", "מתחזקים": "נחלשים", "מתחזק": "נחלש",
}

def _direction_from_pct(pct, threshold=0.15):
    """Return up/down/flat based on percentage change. Threshold avoids noisy micro-moves."""
    try:
        pct = float(pct)
    except Exception:
        return None
    if pct >= threshold:
        return "up"
    if pct <= -threshold:
        return "down"
    return "flat"

def get_verified_asset_directions():
    """Map asset groups to verified direction using the last Finnhub pull.
    If proxies disagree, return mixed to force neutral wording."""
    pcts = _LAST_MARKET_DATA.get("pcts", {}) or {}
    out = {}
    for key, meta in _DIRECTION_ASSETS.items():
        dirs = []
        vals = []
        for sym in meta["symbols"]:
            if sym in pcts:
                vals.append((sym, pcts[sym]))
                d = _direction_from_pct(pcts[sym])
                if d:
                    dirs.append(d)
        if not dirs:
            continue
        nonflat = [d for d in dirs if d != "flat"]
        if not nonflat:
            direction = "flat"
        elif all(d == nonflat[0] for d in nonflat):
            direction = nonflat[0]
        else:
            direction = "mixed"
        out[key] = {"direction": direction, "values": vals, "meta": meta}
    return out

def build_direction_rules(etf_pcts):
    """Text block injected into the LLM prompt so direction words cannot drift away from data."""
    if not etf_pcts:
        return []
    # Temporarily use the supplied pct map rather than the global, which is set later in fetch_market_data.
    old = dict(_LAST_MARKET_DATA.get("pcts", {}))
    try:
        _LAST_MARKET_DATA["pcts"] = etf_pcts
        directions = get_verified_asset_directions()
    finally:
        _LAST_MARKET_DATA["pcts"] = old
    if not directions:
        return []
    he = {"up": "עולה", "down": "יורד", "flat": "יציב/כמעט ללא שינוי", "mixed": "מעורב - להשתמש בניסוח ניטרלי בלבד"}
    lines = ["DIRECTIONAL FACTS — use these for words like עולה/יורד/צונח/מזנק:"]
    for key, info in directions.items():
        vals = ", ".join(f"{sym}: {pct:+.2f}%" for sym, pct in info["values"])
        lines.append(f"  {info['meta']['label']}: {he.get(info['direction'], info['direction'])} ({vals})")
    lines.extend([
        "RULE: Directional Hebrew words MUST match the direction above.",
        "If oil direction is up, NEVER write צונח/יורד/נחלש for oil. If oil direction is down, NEVER write מזנק/עולה/מטפס for oil.",
        "If direction is flat or mixed, use neutral wording such as 'נע בתנודתיות' or omit the direction."
    ])
    return lines

def _contains_any(text, words):
    return any(w in text for w in words)

def _sentence_has_asset(sentence, terms):
    s = sentence.lower()
    return any(t.lower() in s for t in terms)

def _replace_direction_words(sentence, direction):
    repl = _DIRECTION_REPLACEMENTS_UP if direction == "up" else _DIRECTION_REPLACEMENTS_DOWN
    for src, dst in sorted(repl.items(), key=lambda x: -len(x[0])):
        sentence = re.sub(rf'(?<!\w){re.escape(src)}(?!\w)', dst, sentence)
    return sentence

def apply_market_direction_guard(result, review_type):
    """Deterministic post-check: fixes or neutralizes direction words that contradict verified market data.
    This catches errors like 'מחירי הנפט צונחים' when USO/BNO show oil proxies rising."""
    if not isinstance(result, dict):
        return result
    directions = get_verified_asset_directions()
    if not directions:
        return result

    def fix_text(text):
        if not isinstance(text, str):
            return text

        def fix_sentence(sent):
            changed_local = False
            for info in directions.values():
                direction = info["direction"]
                terms = info["meta"]["terms"]
                if not _sentence_has_asset(sent, terms):
                    continue
                has_up = _contains_any(sent, _UP_WORDS)
                has_down = _contains_any(sent, _DOWN_WORDS)
                # If both up and down words appear, the sentence may contain context such as
                # "עלה לאחר שירד אתמול". Do not auto-rewrite mixed internal context.
                if direction == "up" and has_down and not has_up:
                    sent = _replace_direction_words(sent, "up")
                    changed_local = True
                    print(f"  ✅ Direction guard fixed contradiction: {info['meta']['label']} should be UP")
                elif direction == "down" and has_up and not has_down:
                    sent = _replace_direction_words(sent, "down")
                    changed_local = True
                    print(f"  ✅ Direction guard fixed contradiction: {info['meta']['label']} should be DOWN")
                elif direction in ("flat", "mixed") and (has_up or has_down):
                    # Avoid false precision when verified proxies are flat or conflicting.
                    sent = re.sub(r'(מזנקים|מזנק|זינקו|זינק|מטפסים|מטפס|טיפסו|טיפס|עולים|עולה|עלו|עלה|בעלייה|יורדים|יורד|ירדו|ירד|בירידה|צונחים|צונח|צנחו|צנח|נופלים|נופל|נפלו|נפל|נחלשים|נחלש|נחלשו|נחלשת)', 'נעים בתנודתיות', sent)
                    changed_local = True
                    print(f"  ✅ Direction guard neutralized mixed/flat direction: {info['meta']['label']}")
            return sent, changed_local

        changed = False
        out_lines = []
        for line in text.split('\n'):
            parts = re.split(r'(?<=[\.\!\?])\s+', line)
            fixed_parts = []
            for sent in parts:
                fixed, did_change = fix_sentence(sent)
                changed = changed or did_change
                fixed_parts.append(fixed)
            out_lines.append(' '.join(fixed_parts))
        return '\n'.join(out_lines) if changed else text

    for section in result.get("sections", []):
        content = section.get("content")
        if isinstance(content, str):
            section["content"] = fix_text(content)
        elif isinstance(content, list):
            section["content"] = [fix_text(x) if isinstance(x, str) else x for x in content]
    for item in result.get("items", []):
        if isinstance(item.get("description"), str):
            item["description"] = fix_text(item["description"])
        if isinstance(item.get("title"), str):
            item["title"] = fix_text(item["title"])
    return result

# ══════════════════════════════════════════════════════════════
# PER-TICKER DIRECTION GUARD (NEW — closes the PLTR-style errors)
# Verifies every $TICKER mentioned in the review against a live Finnhub quote.
# Catches sign-flip errors (review says "PLTR up" while Finnhub shows down).
# ══════════════════════════════════════════════════════════════

# Hebrew direction tokens used to claim a stock is moving up/down.
_TICKER_UP_TOKENS = [
    "עולה", "עולים", "עלתה", "עלה", "עלו", "עלייה", "עליות", "בעלייה",
    "מטפס", "מטפסת", "מטפסים", "טיפס", "טיפסה", "טיפסו",
    "מזנק", "מזנקת", "מזנקים", "זינק", "זינקה", "זינקו",
    "קופץ", "קופצת", "קופצים", "קפץ", "קפצה", "קפצו",
    "מתחזק", "מתחזקת", "התחזק", "התחזקה",
    "מתאושש", "מתאוששת", "מתאוששים", "התאושש", "התאוששה", "התאוששו", "התאוששות", "ריבאונד",
    "ירוק", "בירוק", "מוסיפה", "מוסיף", "הוסיפה", "הוסיף",
]
_TICKER_DOWN_TOKENS = [
    "יורד", "יורדת", "יורדים", "ירד", "ירדה", "ירדו", "ירידה", "ירידות", "בירידה",
    "נופל", "נופלת", "נופלים", "נפל", "נפלה", "נפלו",
    "צונח", "צונחת", "צונחים", "צנח", "צנחה", "צנחו", "צניחה",
    "נחלש", "נחלשת", "נחלשים", "נחלשה",
    "אדום", "באדום", "מאבד", "מאבדת", "מאבדים", "איבד", "איבדה", "איבדו",
]

# Tickers excluded from per-ticker verification:
# - Indices/ETFs already covered by the main direction guard
# - Generic acronyms that are not real tickers (USD, ET, AM, IPO, etc.)
_TICKER_EXCLUDE = {
    "SPY", "QQQ", "DIA", "IWM", "USO", "BNO", "GLD", "SLV", "IBIT", "TLT", "UUP", "VIXY",
    "XLE", "XLK", "XLF", "XLY", "XLV", "XLI", "XLP", "XLU",
    "USD", "EUR", "GBP", "JPY", "CHF", "CNY", "INR", "USA", "EU", "UK",
    "AM", "PM", "ET", "ETF", "ETFS", "IPO", "API", "AI", "ML", "LLM",
    "EPS", "EBITDA", "PER", "P", "Q", "H", "FY",
    "VIX", "DXY", "SPX", "NDX", "DJI", "RUT",
    "NYC", "LA", "SF", "NY", "DC", "TX", "CA",
    "CEO", "CFO", "CTO", "COO",
    "FED", "ECB", "BOJ", "BOE", "RBA", "PBOC",
    "GDP", "CPI", "PPI", "PMI", "ISM", "NFP", "FOMC", "JOLTS",
}


def extract_ticker_mentions(result):
    """Return set of unique tickers mentioned with $ prefix."""
    if not isinstance(result, dict):
        return set()
    texts = []
    if isinstance(result.get("title"), str):
        texts.append(result["title"])
    for section in result.get("sections", []):
        c = section.get("content", "")
        if isinstance(c, list):
            c = "\n".join(str(x) for x in c)
        if isinstance(c, str):
            texts.append(c)
    for item in result.get("items", []):
        d = item.get("description", "")
        if isinstance(d, str):
            texts.append(d)
    full_text = "\n".join(texts)
    tickers = set()
    for m in re.finditer(r'\$([A-Z]{1,5})\b', full_text):
        sym = m.group(1)
        if sym not in _TICKER_EXCLUDE:
            tickers.add(sym)
    return tickers


def fetch_ticker_quotes(tickers):
    """Pull current Finnhub quote for each ticker.
    Returns dict: ticker -> {'price', 'pct', 'prev_close'}.
    During pre-market, `c` reflects the latest pre-market trade if any.
    Returns empty dict if FINNHUB_API_KEY missing or all calls fail."""
    if not FINNHUB_API_KEY or not tickers:
        return {}
    out = {}
    for t in tickers:
        try:
            r = requests.get(
                f"https://finnhub.io/api/v1/quote?symbol={t}&token={FINNHUB_API_KEY}",
                timeout=5
            )
            if not r.ok:
                continue
            d = r.json()
            price = d.get("c", 0) or 0
            pct = d.get("dp", 0) or 0
            prev = d.get("pc", 0) or 0
            # Skip empty quotes (Finnhub returns zeros for unknown symbols)
            if price <= 0 or prev <= 0:
                continue
            out[t] = {"price": float(price), "pct": float(pct), "prev_close": float(prev)}
            print(f"  Ticker quote {t}: ${price:.2f} ({pct:+.2f}%)")
        except Exception as e:
            print(f"  Ticker quote error for {t}: {e}")
    return out


def _split_into_bullets(text):
    """Split content into bullets by line. Empty lines skipped."""
    if not isinstance(text, str):
        return []
    return [line for line in text.split("\n") if line.strip()]


def _bullet_contains_ticker(bullet, ticker):
    return re.search(rf'\${re.escape(ticker)}\b', bullet) is not None


def _bullet_claims_direction(bullet):
    """Return 'up'|'down'|None based on Hebrew direction tokens.
    Returns None when both up and down tokens appear (ambiguous: 'עלה לאחר שירד')."""
    has_up = any(re.search(rf'(?<!\w){re.escape(t)}(?!\w)', bullet) for t in _TICKER_UP_TOKENS)
    has_down = any(re.search(rf'(?<!\w){re.escape(t)}(?!\w)', bullet) for t in _TICKER_DOWN_TOKENS)
    if has_up and not has_down:
        return "up"
    if has_down and not has_up:
        return "down"
    return None


def apply_ticker_direction_guard(result, ticker_quotes, threshold=0.3):
    """Per-bullet check: for each $TICKER mention with a directional claim,
    verify against Finnhub. Threshold is the dead zone (in %) where the daily
    move is treated as flat (default 0.3%).

    Logs contradictions and stores them in result['_ticker_warnings'] so the
    fact-checker (Layer 4) can decide whether to remove or correct the bullet."""
    if not isinstance(result, dict) or not ticker_quotes:
        return result

    warnings = []

    def scan_bullets(content_text, label):
        if not isinstance(content_text, str):
            return
        for bullet in _split_into_bullets(content_text):
            for ticker, q in ticker_quotes.items():
                if not _bullet_contains_ticker(bullet, ticker):
                    continue
                claimed = _bullet_claims_direction(bullet)
                if claimed is None:
                    continue
                pct = q["pct"]
                if abs(pct) < threshold:
                    actual = "flat"
                else:
                    actual = "up" if pct > 0 else "down"
                if claimed == actual:
                    continue
                # Bullet claims movement but Finnhub shows ~0 — likely the
                # claim references pre-market move not yet visible in /quote.
                severity = "low" if actual == "flat" else "high"
                warnings.append({
                    "ticker": ticker,
                    "claimed": claimed,
                    "actual": f"{pct:+.2f}%",
                    "actual_dir": actual,
                    "severity": severity,
                    "bullet": bullet.strip(),
                    "label": label,
                })

    for i, section in enumerate(result.get("sections", [])):
        c = section.get("content", "")
        if isinstance(c, list):
            c = "\n".join(str(x) for x in c)
        scan_bullets(c, f"section[{section.get('heading', i)}]")
    for i, item in enumerate(result.get("items", [])):
        d = item.get("description", "")
        scan_bullets(d, f"event[{i}]")

    high_severity = [w for w in warnings if w["severity"] == "high"]
    if high_severity:
        print(f"\n  ⚠️  Ticker direction guard: {len(high_severity)} sign-flip contradictions")
        for w in high_severity[:10]:
            print(f"     ${w['ticker']}: bullet claims {w['claimed']}, Finnhub shows {w['actual']}")
            print(f"       bullet: {w['bullet'][:200]}")
    elif warnings:
        print(f"  ⚠️  Ticker direction guard: {len(warnings)} low-severity warnings (likely pre/after-market only)")
    else:
        print("  ✅ Ticker direction guard: no contradictions for verified tickers")

    if warnings:
        result["_ticker_warnings"] = warnings
    return result


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

    direction_rules = build_direction_rules(etf_pcts)
    if direction_rules:
        result_lines.append("")
        result_lines.extend(direction_rules)

    result_lines.extend([
        "",
        "The % changes above are ACCURATE — use them for direction and magnitude.",
        "AUTOMATED-PUBLISHING RULE: Do NOT include absolute commodity prices (oil $/barrel, gold $/oz, Bitcoin $ level, VIX point level) unless they are provided by deterministic verified data. Use direction/% only from the ETF proxy data above.",
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
                    text = t.get('text', '')
                    # Include timestamp if available — critical for live_news 2-hour window
                    ts = t.get('createdAt') or t.get('created_at') or t.get('date') or ''
                    if ts:
                        all_t.append(f"@{acc} [{ts}]: {text}")
                    else:
                        all_t.append(f"@{acc}: {text}")
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

SHARED_RULES = (ACTIVE_GEOPOLITICAL_CONTEXT + "\n" if ACTIVE_GEOPOLITICAL_CONTEXT else "") + """Rules:
- Write ONLY in Hebrew. Use English only for tickers ($AAPL), index names (S&P 500), and well-known financial terms in parentheses on first use only.
- Be specific: every claim must include a number, percentage, or ticker. Never write vague statements like "the market had an interesting week".
- Do NOT repeat the same information across sections. Each section must contain NEW content.
- Do NOT mention the same ticker or company in multiple separate bullets. If a company has multiple news items, combine them into ONE bullet.
- No buy/sell recommendations.
- Start each section directly with the key fact. No generic opening sentences.
- Output pure JSON only, no backticks, no explanations.

CRITICAL — KEY MARKET DATA (MANDATORY VERIFICATION):
- If VERIFIED MARKET DATA from Finnhub API is provided above the tweets, you MUST use those numbers for index performance (% change). Do NOT override them with numbers from tweets or from memory.
- Use the verified % changes as-is. For exact index point levels, use Google Search only if necessary.
- AUTOMATED-PUBLISHING RULE: Do NOT write absolute commodity prices such as Brent $/barrel, WTI $/barrel, gold $/oz, Bitcoin $ level, or VIX point level unless they appear in the deterministic VERIFIED MARKET DATA block.
- Because this project publishes automatically, commodity direction is allowed, but unverified absolute commodity prices are forbidden.
- NEVER trust a single tweet for major price data. If a commodity price is not in deterministic verified data, omit the price.
- Directional words are factual claims. Words like "צונח", "יורד", "נחלש", "מזנק", "עולה", "מטפס" MUST match the verified market-data direction block. If verified data says oil is up, do not write oil is falling, even if a tweet's wording suggests pressure.
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
- For commodity absolute prices (oil $/barrel, gold $/oz): do NOT include them in automated reviews unless they are present in deterministic verified data.
- If a number from a tweet contradicts the Finnhub verified data, the Finnhub data is correct — the tweet is wrong.
- Getting a number wrong destroys credibility. When in doubt, omit.

CRITICAL — CONSISTENCY:
- Every bullet must be internally consistent with the verified market data above.
- Do NOT add a separate "שורה תחתונה" section, closing paragraph, or summary section.

CRITICAL — CAUSAL MARKET LOGIC / NARRATIVE CONSISTENCY:
- Do NOT write a causal explanation unless the causal chain is clear and market-consistent.
- Forbidden contradiction: "עלייה בתיאבון לסיכון" together with "מעבר לאפיקים בטוחים".
- Forbidden contradiction: "risk-on" together with "flight to safety" unless explicitly framed as a rare divergence and verified.
- Forbidden contradiction: crypto falling BECAUSE risk appetite increased. If unsure, write "במקביל ל" or "בתוך סביבת מסחר תנודתית" instead of "על רקע", "בשל", "כתוצאה מכך" or "משקף".
- Forbidden contradiction: Treasury yields rising because investors fled into Treasuries. Bond demand normally pushes yields lower.
- Forbidden contradiction: oil falling because supply disruption fears increased, unless a stronger offsetting factor is explicitly verified.
- When the causal link is uncertain, keep the verified fact and remove the explanation.

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
    """Standard, rigid output-format spec. All review types use one section only.
    The dedicated 'שורה תחתונה' section has been removed by design."""
    return f"""
CRITICAL — OUTPUT FORMAT (MANDATORY, NOT NEGOTIABLE):
- Output EXACTLY 1 section in the "sections" array. Not 2, not 3, not 4. EXACTLY 1.
- The only section heading MUST be EXACTLY "{first_heading}" (no variations, no emojis, no added words).
- The "title" field MUST be EXACTLY: "{expected_title}"
- The only section "content": a list of bullet points, each on its own line, each starting with "* " (asterisk + space).
- Each bullet: "* Short topic label: one concise analytical sentence with numbers."
- Do NOT add a "שורה תחתונה", "סיכום", "מסקנה", or any closing paragraph as a separate section.
- Do NOT use <b>, <strong>, **, ■, 📍, or any HTML/markdown formatting inside content.
- Do NOT add extra sections. If you are tempted to add another section, MERGE that content into the only section as more bullets.
"""

def get_prompt(tweets, review_type, date_str, day_name, title_date_str=None, title_day_name=None,
               week_range=None, is_trading=True, market_data="", prior_context="", expected_title="",
               editorial_block=""):
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
    # Editorial pre-flight goes ABOVE everything else — it sets the agenda.
    if editorial_block:
        tweets_block = editorial_block + "\n" + tweets_block

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
- Do NOT add a separate 'שורה תחתונה' section. Keep all content in the only section, using bullets."""
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

Include 7-12 bullets in the only section. Topics:
1. If the briefing is for a future date, first bullet states when trading resumes.
2. Pre-market/futures sentiment ONLY if the briefing is for today.
3. Scheduled economic data for the target day (Israel times + consensus forecast).
4. Expected earnings reports today.
5. NEW overnight geopolitical developments.
6. NEW overnight company news, analyst upgrades/downgrades.
7. Commodity/currency moves that signal market direction.

No Section 2. Do NOT add a "שורה תחתונה" section. Put any concluding insight as a regular bullet inside the only section.

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

Include 7-12 bullets in the only section, ordered by market impact:
1. Index performance (S&P 500, Nasdaq, Dow with %, point levels, context).
2. Macro data released today with FULL numbers (actual vs forecast vs previous) and market reaction.
3. Key market-moving events: geopolitics, Fed comments, trade news — with cause-and-effect.
4. Commodities and currencies: oil, gold, Bitcoin, VIX — with % and explanation.
5. Notable stock moves with WHY ($TICKER +/- %, what caused it).
6. Sector rotation (using ONLY Finnhub-provided sector ETF data) or institutional activity if relevant.

No Section 2. Do NOT add a "שורה תחתונה" section. Put any concluding insight as a regular bullet inside the only section.

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

Include 8-14 bullets in the only section, ALL forward-looking:
1. Key events coming THIS week: Fed decisions, economic data (NFP, CPI, PMI, GDP, PPI), earnings reports, trade/tariff deadlines, geopolitical developments.
2. For each event: specific day and Israel time when known.
3. Geopolitical risks and what to watch for.
4. Notable companies expected to report earnings this week.
Do NOT include any bullets about last week's performance. Zero backward-looking data.

No Section 2. Do NOT add a "שורה תחתונה" section. Put any concluding insight as a regular bullet inside the only section.

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

Include 8-14 bullets in the only section:
1. Index performance: S&P 500, Nasdaq, Dow, Russell 2000 — weekly % changes, context, leading/lagging sectors.
2. Macro data published this week with FULL numbers (CPI headline AND core, NFP, claims, sentiment — actuals, forecasts, market reaction).
3. Key events that moved markets: geopolitics, Fed comments, trade/tariff news — transmission mechanism.
4. Commodities with context: oil (weekly change + why), gold, Bitcoin.
5. Notable company news, earnings, M&A — combined where related.
6. Earnings season outlook or institutional positioning.

No Section 2. Do NOT add a "שורה תחתונה" section. Put any concluding insight as a regular bullet inside the only section.

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
        now_dt = datetime.now(ISR_TZ)
        now_time = now_dt.strftime('%H:%M')
        two_hours_ago = (now_dt - timedelta(hours=2)).strftime('%H:%M')
        return f"""אתה עורך חדשות בוול סטריט. תן למשקיע ישראלי את החדשות מ-2 השעות האחרונות, בנקודות.

זמן עכשיו: {date_str} בשעה {now_time} (שעון ישראל).
חלון זמן: רק חדשות שפורסמו בין {two_hours_ago} ל-{now_time} היום.

פורמט:
- סעיף אחד בלבד, כותרת "חדשות אחרונות".
- 4–7 בולטים. כל שורה מתחילה ב-"* ".
- כל בולט = ידיעה אחת. משפט אחד עד שניים. תמציתי, נעים לקריאה.
- אין שורה תחתונה. אין סיכום.
- אם אין חדשות דרמטיות ב-2 שעות האחרונות, החזר בולט אחד: "* שקט יחסי בוול סטריט — אין חדשות דרמטיות בשעתיים האחרונות."

מה נכלל:
חדשות אמיתיות — הודעות חברות, מהלכים גיאופוליטיים, תנועות חדות של מניות (מעל 3%) או סחורות (מעל 2%), פרסום נתוני מאקרו משמעותיים (אינפלציה, תעסוקה, מדדי מנהלי רכש, החלטות פד), דיבור של בכירי פד, עסקאות M&A גדולות, החלטות רגולטוריות.

מה לא נכלל:
- מה שהיה מוקדם יותר היום או אתמול.
- נתוני מאקרו משניים כמו Redbook, מלאי עסקים, מכרזי אג"ח קצרות, מכירות קמעונאיות/GDP/סנטימנט צרכנים (אלה שייכים לסיכום היומי, לא לעדכון חי).
- פירוט טכני של מדדים: בלי "Core", "בנטרול רכב", "קבוצת בקרה", "MoM/YoY" ביחד. רק הכותרת.
- אנליסטים שמעלים/מורידים המלצה (אלא אם יעד המחיר זז מעל 20%).
- ADP שבועי — ADP הוא חודשי. אם ראית "ADP שבועי" בציוץ, זו טעות. התעלם.
- מספרים שאתה לא יכול לאתר במקור.

כלל זהב: כל בולט = ידיעה אחת, משפט קצר, בלי הרחבות. אם הבולט שלך מעל 25 מילים, קצר אותו.

{SHARED_RULES}

{tweets_block}

החזר אך ורק JSON בפורמט הזה, בלי backticks, בלי הסברים:
{{"title":"מה קורה עכשיו בוול סטריט 🇺🇸 – יום {day_name}, {date_str} | {now_time}","date":"{date_str}","sections":[{{"heading":"חדשות אחרונות","content":"* בולט 1\\n* בולט 2\\n* בולט 3"}}]}}"""

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
    """Remove bullet markers from paragraph text."""
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
    All review pages now use one section only. A dedicated 'שורה תחתונה' section is dropped."""

    if not isinstance(result, dict):
        print("  ⚠️ enforce_structure: result is not a dict — returning unchanged")
        return result

    # Events still uses a completely different structure (items, not sections)
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
        print("  ⚠️ enforce_structure: no sections — creating one empty section")
        result["sections"] = [{"heading": first_heading, "content": ""}]
        return result

    # 3. Drop explicit bottom-line sections and merge everything else into one section
    merged_parts = []
    dropped_bottom_lines = 0
    for s in sections:
        heading = str(s.get("heading", ""))
        c = s.get("content", "")
        if isinstance(c, list):
            c = "\n".join(str(x) for x in c)

        if "שורה תחתונה" in heading or heading.lower().strip() in {"bottom line", "the bottom line"}:
            dropped_bottom_lines += 1
            continue

        if c and str(c).strip():
            merged_parts.append(str(c).strip())

    if not merged_parts:
        # Safety: if Gemini returned only a bottom-line section, keep its content but under the main heading
        for s in sections:
            c = s.get("content", "")
            if isinstance(c, list):
                c = "\n".join(str(x) for x in c)
            if c and str(c).strip():
                merged_parts.append(str(c).strip())

    merged = "\n".join(merged_parts)
    normalized = normalize_bullets(merged)

    if len(sections) != 1 or dropped_bottom_lines:
        print(f"  ✅ Sections normalized: {len(sections)} → 1; dropped bottom-line sections: {dropped_bottom_lines}")

    result["sections"] = [{
        "heading": first_heading,
        "content": normalized,
    }]
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
# NUMBER PROVENANCE CHECK — every number in the output must trace to a source
# ══════════════════════════════════════════════════════════════

# Numbers always safe to ignore (years, common round bases, tiny values)
_PROVENANCE_IGNORE_EXACT = {
    '100', '1000', '10000',
    '2020', '2021', '2022', '2023', '2024', '2025', '2026', '2027', '2028',
}
# Below this threshold numbers are usually trivial (bullet counts, small list sizes)
_PROVENANCE_IGNORE_MAX = 2.0
# Above this threshold almost certainly market cap / dollar figures — keep checking
_PROVENANCE_ABS_MAX = 1e13

# Number-token regex: 1,234 | 1234.56 | 54.75 | 0.9
_NUM_TOKEN = re.compile(r'(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)')

# Hebrew context words that indicate the number is a time/date/period, not a data point
_TEMPORAL_CTX = re.compile(
    r'\b(שעה|שעות|בשעה|יום|ימים|חודש|חודשים|שנה|שנים|שנתיים|ברבעון|רבעון|'
    r'רבעונים|Q[1-4]|H[12]|שבוע|שבועות|דקה|דקות|ETA|בתוך)\b'
)

def _norm_num(s):
    """Normalize a number token: strip commas, strip trailing zeros after decimal."""
    s = s.replace(',', '')
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
        # Strip trailing zeros: "54.70" -> "54.7"
        return f"{f:g}"
    except ValueError:
        return s

def build_source_bundle(market_data, tweets, prior_context):
    """Flatten all source material into a searchable structure for provenance checks.
    Returns a dict with 'all_text' (concatenated) and 'numbers' (set of normalized numeric tokens)."""
    all_text = "\n".join([
        market_data or "",
        tweets or "",
        prior_context or "",
    ])
    numbers = set()
    for m in _NUM_TOKEN.finditer(all_text):
        raw = m.group(1)
        numbers.add(raw)
        numbers.add(_norm_num(raw))
    return {"all_text": all_text, "numbers": numbers}

def number_provenance_check(result, source_bundle, review_type):
    """Scan the generated review for numeric claims that don't trace to any source.
    Phase 1: informational warnings only — logs suspicious numbers to stdout, returns
    result unchanged. Drops are handled by fact_check_with_gemini downstream.

    This catches hallucinations like 'ADP weekly 54.75K' that pass earlier layers because
    they're internally consistent but absent from Finnhub/econ_calendar/tweets."""

    if not isinstance(result, dict):
        return result

    all_text = source_bundle.get("all_text", "")
    src_numbers = source_bundle.get("numbers", set())
    warnings = []

    def _is_in_sources(raw_token, normalized):
        # Direct token match
        if raw_token in src_numbers or normalized in src_numbers:
            return True
        # Raw substring in any source (catches cases where source has '54,750' and
        # output has '54.75 אלף')
        if raw_token in all_text or normalized in all_text:
            return True
        # Fuzzy match: within 0.5% relative tolerance (rounding differences are OK)
        try:
            v = float(normalized)
            if v <= 0:
                return False
            for sn in src_numbers:
                try:
                    sv = float(sn)
                    if sv > 0 and abs(v - sv) / sv < 0.005:
                        return True
                except ValueError:
                    continue
        except ValueError:
            pass
        return False

    def _scan(text, label):
        if not isinstance(text, str) or not text.strip():
            return
        for m in _NUM_TOKEN.finditer(text):
            raw = m.group(1)
            normalized = _norm_num(raw)
            # Skip well-known / year numbers
            if normalized in _PROVENANCE_IGNORE_EXACT:
                continue
            try:
                fv = float(normalized)
                if fv >= _PROVENANCE_ABS_MAX:
                    continue
            except ValueError:
                continue
            # Immediate character after the number — '%' or '$' flags it as financial
            after_char = text[m.end():m.end()+1] if m.end() < len(text) else ""
            before_char = text[m.start()-1:m.start()] if m.start() > 0 else ""
            after_window = text[m.end():min(len(text), m.end() + 15)]
            # Financial unit markers attached to the number
            is_financial = (
                after_char == "%"
                or after_char == "$"
                or before_char == "$"
                or after_window.lstrip().startswith(("אלף", "מיליון", "מיליארד", "טריליון", "נקודות", "נק'", "נק׳", "יורו", "₪", "דולר"))
            )
            # Bare numbers (no financial unit) that are tiny are almost always trivial
            # (list numbering, "3 reasons", etc.) — skip. But financial numbers like 0.4%
            # are significant macro data and must be checked no matter how small.
            if not is_financial and fv <= _PROVENANCE_IGNORE_MAX:
                continue
            # Wide context window for temporal check
            ctx_start = max(0, m.start() - 25)
            ctx_end = min(len(text), m.end() + 25)
            ctx = text[ctx_start:ctx_end]
            # Temporal skip — BUT only if the number is NOT explicitly marked as financial.
            # Otherwise "3 חודשים: 3.61%" would drop the 3.61 (a yield) as if it were "3 months".
            if not is_financial and _TEMPORAL_CTX.search(ctx):
                continue
            # Now check provenance
            if _is_in_sources(raw, normalized):
                continue
            # Not found — record warning
            warnings.append({
                "label": label,
                "number": raw,
                "context": ctx.strip(),
            })

    # Scan title + every section content + events items
    title = result.get("title", "")
    if isinstance(title, str):
        _scan(title, "title")
    for i, section in enumerate(result.get("sections", [])):
        content = section.get("content", "")
        if isinstance(content, list):
            content = "\n".join(str(x) for x in content)
        heading = section.get("heading", f"section_{i}")
        _scan(content, f"section[{heading}]")
    for i, item in enumerate(result.get("items", [])):
        _scan(item.get("description", ""), f"event[{i}]")

    if warnings:
        print(f"\n  ⚠️  Number provenance: {len(warnings)} numbers not found in sources")
        for w in warnings[:20]:
            print(f"     [{w['label']}] '{w['number']}' → ...{w['context']}...")
        if len(warnings) > 20:
            print(f"     ... and {len(warnings) - 20} more")
        # Attach warnings to the result for potential downstream use
        result["_provenance_warnings"] = warnings
    else:
        print("  ✅ Number provenance: every number traces to a source")

    return result



# ══════════════════════════════════════════════════════════════
# NARRATIVE CONSISTENCY GUARD — causal logic / market-regime sanity check
# ══════════════════════════════════════════════════════════════

_CAUSAL_CONNECTORS = [
    "על רקע", "בשל", "עקב", "בעקבות", "כתוצאה מכך", "כתוצאה מ", "הוביל ל", "הובילה ל",
    "משקף", "משקפת", "מבטא", "מבטאת", "נבע", "נבעה", "נובעת", "נובע"
]

_RISK_ON_TERMS = [
    "עלייה בתיאבון לסיכון", "עליה בתיאבון לסיכון", "תיאבון לסיכון גובר", "תיאבון סיכון גובר",
    "תיאבון לסיכון", "risk-on", "Risk-on", "ריסק און", "סנטימנט חיובי", "הקלה בשווקים"
]

_RISK_OFF_TERMS = [
    "ירידה בתיאבון לסיכון", "תיאבון סיכון נמוך", "risk-off", "Risk-off", "ריסק אוף",
    "בריחה מסיכון", "שנאת סיכון", "סנטימנט שלילי"
]

_SAFE_HAVEN_TERMS = [
    "אפיקים בטוחים", "נכסים בטוחים", "נכסי מקלט", "מקלט בטוח", "flight to safety",
    "דולר", "זהב", "אג\"ח ממשלתיות", "אגרות חוב ממשלתיות", "סקטורים דפנסיביים"
]

_RISK_ASSET_TERMS = [
    "ביטקוין", "קריפטו", "BTC", "$BTC", "מניות צמיחה", "טכנולוגיה", "נכסי סיכון",
    "מניות ספקולטיביות", "high beta", "היי בטא"
]

_YIELD_TERMS = ["תשואה", "תשואות", "Treasury yield", "yields", "אג\"ח ל-10", "אגח ל-10"]
_BOND_DEMAND_TERMS = ["בריחה לאג\"ח", "מעבר לאג\"ח", "ביקוש לאג\"ח", "רכישת אג\"ח", "קניית אג\"ח", "fled into Treasuries", "flight to Treasuries"]
_OIL_TERMS = ["נפט", "ברנט", "WTI", "Brent", "crude oil"]
_SUPPLY_DISRUPTION_TERMS = ["שיבושי אספקה", "הפרעה לאספקה", "סיכון לאספקה", "supply disruption", "מצרי הורמוז", "Hormuz"]
_GEOPOLITICAL_RELIEF_TERMS = ["הקלה גיאופוליטית", "הרגעה גיאופוליטית", "דחיית תקיפה", "ceasefire", "הפסקת אש", "de-escalation"]


def _has_any_ci(text, terms):
    """Case-insensitive containment for Hebrew/English phrase lists."""
    if not isinstance(text, str):
        return False
    low = text.lower()
    return any(t.lower() in low for t in terms)


def _has_causal_connector(text):
    return _has_any_ci(text, _CAUSAL_CONNECTORS)


def _strip_or_soften_causality(sentence):
    """Remove the causal tail from a market sentence when the causal chain is contradictory.

    We keep the factual part, and replace the explanation with a neutral, professional phrase.
    This prevents embarrassing claims such as: 'BTC fell because risk appetite increased'.
    """
    if not isinstance(sentence, str) or not sentence.strip():
        return sentence

    # If the sentence itself is only an explanation using 'משקף/מבטא', replace it entirely.
    if re.search(r'(משקף|משקפת|מבטא|מבטאת)\s+את', sentence):
        return "בשלב זה לא ניתן לייחס את התנועה לגורם יחיד."

    # Cut the sentence at the first causal connector and keep the factual opening.
    pattern = r'\s+(על רקע|בשל|עקב|בעקבות|כתוצאה מכך|כתוצאה מ|נבע(?:ה)? מ|נובע מ|נובעת מ|הוביל(?:ה)? ל)\s+'
    m = re.search(pattern, sentence)
    if m:
        factual = sentence[:m.start()].strip().rstrip(".،,;:")
        if factual:
            return factual + " בתוך סביבת מסחר תנודתית."

    # Fallback: soften explicit causal verbs without deleting the whole sentence.
    replacements = {
        "על רקע": "במקביל ל",
        "בשל": "במקביל ל",
        "עקב": "במקביל ל",
        "בעקבות": "לאחר",
        "כתוצאה מכך": "לצד זאת",
        "הוביל ל": "לווה ב",
        "הובילה ל": "לוותה ב",
        "נבעה": "נרשמה במקביל",
        "נבע": "נרשם במקביל",
        "נובעת": "נרשמת במקביל",
        "נובע": "נרשם במקביל",
    }
    out = sentence
    for src, dst in replacements.items():
        out = out.replace(src, dst)
    return out


def _split_sentences_keep_lines(text):
    """Split text by line and sentence boundaries while preserving bullet lines."""
    lines = text.split("\n")
    for line in lines:
        if not line.strip():
            yield line, []
            continue
        parts = re.split(r'(?<=[\.\!\?])\s+', line)
        yield line, parts


def apply_narrative_consistency_guard(result, review_type):
    """Detect and neutralize market-logic contradictions in causal explanations.

    This guard is intentionally conservative: it does NOT try to be a macro strategist.
    It only catches common embarrassing contradictions:
    - risk-on + flight-to-safety in the same causal claim
    - crypto/risk assets falling because risk appetite increased
    - yields rising because investors fled into Treasuries
    - oil falling because supply disruption fears increased
    - geopolitical relief together with a surge in safe-haven demand

    Returns (result, warnings). Warnings with severity='high' should block publication
    if they remain after the fact-checking layers.
    """
    warnings = []

    if not isinstance(result, dict):
        return result, warnings

    def check_sentence(sentence, label):
        original = sentence
        if not isinstance(sentence, str) or not sentence.strip():
            return sentence

        has_up = _contains_any(sentence, _UP_WORDS)
        has_down = _contains_any(sentence, _DOWN_WORDS)
        has_causal = _has_causal_connector(sentence)

        rules = []

        # 1) Risk-on + flight-to-safety contradiction.
        if _has_any_ci(sentence, _RISK_ON_TERMS) and _has_any_ci(sentence, _SAFE_HAVEN_TERMS):
            rules.append("risk_on_vs_safe_haven")

        # 2) Risk asset down because risk appetite increased.
        if has_causal and _has_any_ci(sentence, _RISK_ASSET_TERMS) and _has_any_ci(sentence, _RISK_ON_TERMS) and has_down:
            rules.append("risk_asset_down_due_to_risk_on")

        # 3) Risk asset up because risk appetite fell / risk-off, unless explicitly framed as defensive crypto flow.
        if has_causal and _has_any_ci(sentence, _RISK_ASSET_TERMS) and _has_any_ci(sentence, _RISK_OFF_TERMS) and has_up:
            rules.append("risk_asset_up_due_to_risk_off")

        # 4) Yields rising because investors buy/flee into bonds.
        if has_causal and _has_any_ci(sentence, _YIELD_TERMS) and has_up and _has_any_ci(sentence, _BOND_DEMAND_TERMS):
            rules.append("yields_up_due_to_bond_demand")

        # 5) Oil down because supply disruption fears increased.
        if has_causal and _has_any_ci(sentence, _OIL_TERMS) and has_down and _has_any_ci(sentence, _SUPPLY_DISRUPTION_TERMS):
            rules.append("oil_down_due_to_supply_disruption")

        # 6) Geopolitical relief + safe-haven demand surge.
        if _has_any_ci(sentence, _GEOPOLITICAL_RELIEF_TERMS) and _has_any_ci(sentence, _SAFE_HAVEN_TERMS) and has_up:
            rules.append("relief_vs_safe_haven_demand")

        if not rules:
            return sentence

        fixed = _strip_or_soften_causality(sentence)
        warnings.append({
            "label": label,
            "severity": "high" if fixed == original else "medium",
            "rules": rules,
            "original": original.strip(),
            "fixed": fixed.strip(),
        })
        if fixed != original:
            print(f"  ✅ Narrative guard neutralized causal contradiction ({', '.join(rules)})")
            print(f"     before: {original.strip()[:220]}")
            print(f"     after : {fixed.strip()[:220]}")
        else:
            print(f"  ⚠️  Narrative guard detected unresolved contradiction ({', '.join(rules)})")
            print(f"     sentence: {original.strip()[:220]}")
        return fixed

    def fix_text(text, label):
        if not isinstance(text, str):
            return text
        new_lines = []
        for line, parts in _split_sentences_keep_lines(text):
            if not parts:
                new_lines.append(line)
                continue

            # Cross-sentence contradiction inside the same bullet/line:
            # Example: sentence 1 says BTC fell because risk appetite increased;
            # sentence 2 says the fall reflects flight to safety. Sentence-level
            # scanning may miss the second sentence, so the whole line context matters.
            line_has_down = _contains_any(line, _DOWN_WORDS)
            line_has_up = _contains_any(line, _UP_WORDS)
            cross_sentence_conflict = (
                (_has_any_ci(line, _RISK_ON_TERMS) and _has_any_ci(line, _SAFE_HAVEN_TERMS))
                or (_has_any_ci(line, _RISK_ASSET_TERMS) and _has_any_ci(line, _RISK_ON_TERMS) and line_has_down)
                or (_has_any_ci(line, _YIELD_TERMS) and line_has_up and _has_any_ci(line, _BOND_DEMAND_TERMS))
                or (_has_any_ci(line, _OIL_TERMS) and line_has_down and _has_any_ci(line, _SUPPLY_DISRUPTION_TERMS))
            )

            fixed_parts = []
            for part in parts:
                fixed = check_sentence(part, label)
                if cross_sentence_conflict and fixed == part and (_has_causal_connector(part) or re.search(r'(משקף|משקפת|מבטא|מבטאת)\s+את', part)):
                    fixed2 = _strip_or_soften_causality(part)
                    warnings.append({
                        "label": label,
                        "severity": "medium" if fixed2 != part else "high",
                        "rules": ["cross_sentence_market_regime_contradiction"],
                        "original": part.strip(),
                        "fixed": fixed2.strip(),
                    })
                    fixed = fixed2
                    print("  ✅ Narrative guard neutralized cross-sentence causal contradiction")
                    print(f"     before: {part.strip()[:220]}")
                    print(f"     after : {fixed.strip()[:220]}")
                fixed_parts.append(fixed)
            new_lines.append(" ".join(fixed_parts))
        return "\n".join(new_lines)

    for i, section in enumerate(result.get("sections", [])):
        heading = section.get("heading", f"section_{i}")
        content = section.get("content")
        label = f"section[{heading}]"
        if isinstance(content, str):
            section["content"] = fix_text(content, label)
        elif isinstance(content, list):
            section["content"] = [fix_text(x, label) if isinstance(x, str) else x for x in content]

    for i, item in enumerate(result.get("items", [])):
        if isinstance(item.get("description"), str):
            item["description"] = fix_text(item["description"], f"event[{i}]")
        if isinstance(item.get("title"), str):
            item["title"] = fix_text(item["title"], f"event_title[{i}]")

    if warnings:
        result["_narrative_warnings"] = warnings
        high = [w for w in warnings if w.get("severity") == "high"]
        print(f"\n  ⚠️  Narrative consistency: {len(warnings)} warning(s), {len(high)} unresolved/high")
    else:
        print("  ✅ Narrative consistency: no causal contradictions detected")

    return result, warnings


def save_failed_review(result, blocking_errors, review_type):
    """Persist a draft for manual review when publication is blocked.

    The GitHub Action should not commit data.json when sys.exit(2) is raised, but this file
    is useful in local runs and logs. It intentionally does not mutate data.json.
    """
    try:
        payload = {
            "review_type": review_type,
            "blocked_at": datetime.now(ISR_TZ).isoformat(),
            "blocking_errors": blocking_errors,
            "draft": result,
        }
        filename = f"failed_review_{review_type}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"  Draft saved for manual review: {filename}")
    except Exception as e:
        print(f"  Could not save failed-review draft: {e}")



# ══════════════════════════════════════════════════════════════
# ALWAYS-PUBLISH SAFE MODE
# ══════════════════════════════════════════════════════════════
# Doron preference: the site must always publish a review. Therefore the final
# behavior is: generate -> repair -> validate -> if still unsafe, publish a
# conservative safe-mode review instead of failing the GitHub Action.

ALWAYS_PUBLISH_SAFE_MODE = True

_SAFE_DIRECTION_HE = {
    "up": "חיובי",
    "down": "שלילי",
    "flat": "יציב יחסית",
    "mixed": "מעורב",
    None: "לא חד משמעי",
}

_SAFE_SYMBOL_LABELS = {
    "SPY": "מדד S&P 500, באמצעות SPY",
    "QQQ": "מדד Nasdaq 100, באמצעות QQQ",
    "DIA": "מדד Dow Jones, באמצעות DIA",
    "IWM": "מדד Russell 2000, באמצעות IWM",
    "XLE": "סקטור האנרגיה, באמצעות XLE",
    "XLK": "סקטור הטכנולוגיה, באמצעות XLK",
    "XLF": "סקטור הפיננסים, באמצעות XLF",
    "XLY": "סקטור הצריכה המחזורית, באמצעות XLY",
    "XLV": "סקטור הבריאות, באמצעות XLV",
    "USO": "נפט WTI, באמצעות USO",
    "BNO": "נפט Brent, באמצעות BNO",
    "GLD": "זהב, באמצעות GLD",
    "TLT": "אגח ארוכות בארהב, באמצעות TLT",
    "UUP": "דולר ארהב, באמצעות UUP",
    "IBIT": "ביטקוין, באמצעות IBIT",
    "VIXY": "תנודתיות, באמצעות VIXY",
}

_SAFE_REVIEW_FALLBACKS = {
    "daily_prep": [
        "* פתיחת המסחר: הסקירה פורסמה בגרסה שמרנית, ללא רמות מחיר שלא אומתו במערכת.",
        "* מאקרו: המשקיעים יעקבו אחר נתוני המאקרו והודעות החברות שצפויים להשפיע על סנטימנט המסחר.",
        "* מניות במוקד: יש להתמקד בדוחות, עדכוני אנליסטים וחדשות חברה שפורסמו במקורות המעקב.",
        "* סחורות ואגח: תנועות בנפט, בזהב ובתשואות יוזכרו רק כאשר הכיוון אומת במקור נתונים דטרמיניסטי.",
    ],
    "daily_summary": [
        "* סיכום המסחר: הסקירה פורסמה בגרסה שמרנית, ללא רמות מחיר שלא אומתו במערכת.",
        "* מדדים: כיווני השוק נבדקו מול נתוני ETF מאומתים, אך לא פורסמו רמות מדד שלא עברו אימות.",
        "* מניות במוקד: תנועות חריגות הושארו רק כאשר לא נמצאה סתירה מול נתוני שוק דטרמיניסטיים.",
        "* סחורות ואגח: מחירי חבית, אונקיה ותשואות לא יופיעו ללא מקור מאומת ברור.",
    ],
    "weekly_prep": [
        "* הכנה לשבוע: הסקירה פורסמה בגרסה שמרנית, ללא נתונים מספריים שלא אומתו במערכת.",
        "* מאקרו: השבוע יתמקד בנתוני מאקרו, דוחות כספיים ואירועים גיאופוליטיים שעשויים להשפיע על סנטימנט המשקיעים.",
        "* דוחות: חברות מרכזיות צפויות לרכז עניין בהתאם ללוח הדוחות ולציפיות השוק.",
        "* ניהול סיכונים: רמות מחיר ותשואות יוצגו רק כאשר הן מגיעות ממקור נתונים מאומת.",
    ],
    "weekly_summary": [
        "* סיכום השבוע: הסקירה פורסמה בגרסה שמרנית, ללא נתונים מספריים שלא אומתו במערכת.",
        "* מדדים: כיווני השוק השבועיים נבדקו מול נתוני ETF מאומתים, ללא פרסום רמות מדד לא מאומתות.",
        "* סקטורים: הדגש הוא על כיוון יחסי ומוקדי עניין, לא על מספרים שלא עברו בדיקת מקור.",
        "* מבט קדימה: המשך השבוע יושפע מנתוני מאקרו, דוחות והודעות מדיניות.",
    ],
    "live_news": [
        "* עדכון חי: הסקירה פורסמה בגרסה שמרנית, ללא מספרים שלא אומתו במערכת.",
        "* שוק: המערכת מציגה רק כיוונים שנבדקו מול מקור נתונים דטרמיניסטי.",
        "* מניות: אזכורי מניות נשמרים רק כאשר לא נמצאה סתירה מול נתוני מחיר מאומתים.",
        "* סחורות ואגח: רמות מחיר מוחלטות לא יוצגו ללא מקור אימות ברור.",
    ],
}

def _safe_direction_from_pct_for_publish(pct):
    try:
        pct = float(pct)
    except Exception:
        return None
    if pct >= 0.15:
        return "up"
    if pct <= -0.15:
        return "down"
    return "flat"

def _normalize_review_bullet(line):
    if not isinstance(line, str):
        return ""
    line = re.sub(r'^\s*[\*•\-]+\s*', '', line.strip())
    line = re.sub(r'<[^>]+>', '', line)
    line = re.sub(r'\s+', ' ', line).strip()
    if not line:
        return ""
    return "* " + line

def _line_matches_blocking_error(line, blocking_errors):
    if not line or not blocking_errors:
        return False
    for err in blocking_errors:
        for key in ("line", "bullet", "context", "text", "original"):
            val = err.get(key) if isinstance(err, dict) else None
            if isinstance(val, str) and val:
                a = re.sub(r'\s+', ' ', val.strip())
                b = re.sub(r'\s+', ' ', line.strip())
                if a and (a in b or b in a):
                    return True
    return False

def _is_safe_to_keep_in_safe_mode(line, blocking_errors=None):
    """Keep only low-risk qualitative bullets from the generated draft.
    In safe mode we aggressively remove numbers and market-direction claims.
    """
    if not isinstance(line, str) or len(line.strip()) < 24:
        return False
    if _line_matches_blocking_error(line, blocking_errors):
        return False
    # Any digit or market price sign is removed in the last-resort public fallback.
    # This prevents unverified index levels, yields, oil prices and market caps.
    if re.search(r'[0-9$%]', line):
        return False
    # Avoid hard direction words in safe mode. We use neutral words from verified data below.
    if _contains_any(line, _UP_WORDS) or _contains_any(line, _DOWN_WORDS):
        return False
    # Avoid commodities/yields if the line is not deterministic.
    if _has_any_ci(line, ["נפט", "ברנט", "WTI", "זהב", "אונקיה", "תשואה", "אג\"ח", "אגח", "VIX", "ביטקוין", "BTC"]):
        return False
    return True

def _collect_safe_qualitative_bullets(result, blocking_errors=None, max_items=4):
    bullets = []
    seen = set()
    if not isinstance(result, dict):
        return bullets
    for _label, line in _iter_review_lines(result):
        bullet = _normalize_review_bullet(line)
        if not bullet or not _is_safe_to_keep_in_safe_mode(bullet, blocking_errors):
            continue
        norm = re.sub(r'\s+', ' ', bullet).strip()
        if norm in seen:
            continue
        seen.add(norm)
        bullets.append(bullet)
        if len(bullets) >= max_items:
            break
    return bullets

def _build_verified_direction_bullets(max_items=5):
    pcts = (_LAST_MARKET_DATA.get("pcts") or {}) if isinstance(_LAST_MARKET_DATA, dict) else {}
    bullets = []
    priority = ["SPY", "QQQ", "DIA", "IWM", "XLK", "XLE", "XLF", "XLY", "XLV", "USO", "BNO", "GLD", "TLT", "UUP", "IBIT", "VIXY"]
    for sym in priority:
        if sym not in pcts:
            continue
        direction = _SAFE_DIRECTION_HE.get(_safe_direction_from_pct_for_publish(pcts.get(sym)), "לא חד משמעי")
        label = _SAFE_SYMBOL_LABELS.get(sym, sym)
        # No numbers here by design, so fallback cannot fail on unverified numeric provenance.
        bullets.append(f"* {label}: הכיוון המאומת במערכת הוא {direction}, ללא פרסום רמת מחיר מוחלטת.")
        if len(bullets) >= max_items:
            break
    return bullets

def build_safe_publish_review(result, review_type, expected_title, review_date, blocking_errors=None):
    """Last-resort public review. It is intentionally conservative and numeric-free.
    This guarantees the site publishes something useful without leaking bad numbers.
    """
    first_heading = EXPECTED_FIRST_HEADING.get(review_type, "נקודות מרכזיות")
    bullets = []

    # Prefer verified direction context, without numeric levels or percentages.
    bullets.extend(_build_verified_direction_bullets(max_items=5))

    # Preserve a few qualitative, low-risk bullets from the generated draft.
    bullets.extend(_collect_safe_qualitative_bullets(result, blocking_errors=blocking_errors, max_items=3))

    # Ensure minimum useful content by review type.
    fallback = _SAFE_REVIEW_FALLBACKS.get(review_type, _SAFE_REVIEW_FALLBACKS["daily_summary"])
    for item in fallback:
        if item not in bullets:
            bullets.append(item)
        if len(bullets) >= 7:
            break

    # Defensive de-duplication and final sanitization.
    clean = []
    seen = set()
    for b in bullets:
        b = _normalize_review_bullet(b)
        # Strip any accidental numbers in fallback lines. We prefer imprecision to wrong data.
        b = re.sub(r'\$?\d+(?:[,.]\d+)*\s?%?', '', b)
        b = re.sub(r'\s+', ' ', b).replace(" :", ":").strip()
        if not b.startswith("* "):
            b = "* " + b
        if len(b) < 12:
            continue
        if b in seen:
            continue
        seen.add(b)
        clean.append(b)

    return {
        "title": expected_title,
        "date": review_date,
        "sections": [{"heading": first_heading, "content": clean[:8]}],
        "safeMode": True,
        "safeModeReason": "auto_repair_failed_or_unverified_data",
    }


# ══════════════════════════════════════════════════════════════
# FINAL HARD QUALITY GATE — blocks publication rather than publishing bad data
# ══════════════════════════════════════════════════════════════

_FORBIDDEN_ABSOLUTE_PRICE_CONTEXT = [
    "נפט", "ברנט", "WTI", "Brent", "crude", "חבית",
    "זהב", "gold", "אונקיה", "oz",
    "ביטקוין", "Bitcoin", "BTC", "VIX"
]

_FORBIDDEN_PRICE_PATTERN = re.compile(
    r'(?:\$\s?\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d{1,3}(?:,\d{3})*(?:\.\d+)?\s?\$|\d{1,3}(?:,\d{3})*(?:\.\d+)?\s?(?:דולר|נקודות))'
)

_HARD_PROVENANCE_TERMS = [
    "נפט", "ברנט", "WTI", "חבית", "זהב", "אונקיה", "VIX", "ביטקוין", "BTC",
    "תשואה", "תשואת", "אג\"ח", "אגח", "Treasury", "S&P", "נאסד", "נאסד\"ק", "דאו", "Russell",
    "%", "$", "דולר", "מיליארד", "מיליון"
]


def _iter_review_lines(result):
    if not isinstance(result, dict):
        return
    if isinstance(result.get("title"), str):
        yield "title", result["title"]
    for section in result.get("sections", []):
        heading = section.get("heading", "section")
        content = section.get("content", "")
        if isinstance(content, list):
            content = "\n".join(str(x) for x in content)
        if isinstance(content, str):
            for line in content.split("\n"):
                if line.strip():
                    yield f"section[{heading}]", line.strip()
    for i, item in enumerate(result.get("items", [])):
        for field in ("title", "description"):
            val = item.get(field, "")
            if isinstance(val, str) and val.strip():
                yield f"event[{i}].{field}", val.strip()


def detect_forbidden_absolute_prices(result, source_bundle=None):
    """Block absolute commodity prices in auto-published reviews unless they are deterministic.
    The current verified block uses ETF proxy prices, not real Brent/WTI/gold spot/futures.
    So a bullet like 'Brent סביב 96.7 דולר לחבית' is not safe for automatic publishing."""
    warnings = []
    for label, line in _iter_review_lines(result):
        if not _has_any_ci(line, _FORBIDDEN_ABSOLUTE_PRICE_CONTEXT):
            continue
        if not _FORBIDDEN_PRICE_PATTERN.search(line):
            continue
        # Percent changes are allowed; this pattern catches dollar/point levels.
        warnings.append({
            "type": "forbidden_absolute_price",
            "severity": "high",
            "label": label,
            "line": line[:500],
        })
    return warnings


def detect_internal_direction_conflicts(result):
    """Catch headline/body contradictions in a single bullet.
    Example: 'הנפט מתאושש בחדות: ... מחירי הנפט רושמים ירידה חדה'."""
    warnings = []
    for label, line in _iter_review_lines(result):
        if not _has_any_ci(line, ["נפט", "ברנט", "WTI", "זהב", "ביטקוין", "VIX", "חוזים", "מדדים"]):
            continue
        has_up = _contains_any(line, _UP_WORDS)
        has_down = _contains_any(line, _DOWN_WORDS)
        if not (has_up and has_down):
            continue
        # Legitimate contrast can be allowed when clearly separated by time.
        # But if the topic label before ':' claims one direction and the body claims the opposite, block.
        if ":" in line:
            topic, body = line.split(":", 1)
            topic_up, topic_down = _contains_any(topic, _UP_WORDS), _contains_any(topic, _DOWN_WORDS)
            body_up, body_down = _contains_any(body, _UP_WORDS), _contains_any(body, _DOWN_WORDS)
            if (topic_up and body_down) or (topic_down and body_up):
                warnings.append({
                    "type": "internal_direction_conflict",
                    "severity": "high",
                    "label": label,
                    "line": line[:500],
                })
                continue
        # Also block common contradictory same-line formulation.
        if re.search(r'(מתאושש|התאוששות|ריבאונד).{0,120}(ירידה|יורד|נחלש|צונח|נופל)', line) or \
           re.search(r'(ירידה|יורד|נחלש|צונח|נופל).{0,120}(מתאושש|התאוששות|ריבאונד)', line):
            warnings.append({
                "type": "internal_direction_conflict",
                "severity": "high",
                "label": label,
                "line": line[:500],
            })
    return warnings


def hard_provenance_warnings(provenance_warnings):
    """Only promote provenance warnings that are dangerous for credibility.
    We do not block harmless list numbers, but we do block market numbers."""
    hard = []
    for w in provenance_warnings or []:
        ctx = (w.get("context") or "") + " " + (w.get("label") or "")
        if _has_any_ci(ctx, _HARD_PROVENANCE_TERMS):
            ww = dict(w)
            ww["type"] = "unverified_market_number"
            ww["severity"] = "high"
            hard.append(ww)
    return hard


def final_hard_quality_gate(result, source_bundle, review_type):
    """Run after all fixes. Anything returned here must block publication."""
    blocking = []
    blocking.extend(detect_forbidden_absolute_prices(result, source_bundle))
    blocking.extend(detect_internal_direction_conflicts(result))

    # Re-run number provenance on the final result. The function mutates metadata only.
    checked = number_provenance_check(result, source_bundle, review_type)
    final_provenance = checked.pop("_provenance_warnings", None)
    blocking.extend(hard_provenance_warnings(final_provenance))
    return blocking

def should_block_publication(validation_warnings=None, provenance_warnings=None, ticker_warnings=None, narrative_warnings=None):
    """Return a list of blocking errors.

    Reliability posture for this project: when an automated review contains a hard market-data
    conflict, do not publish. A stale but clean old review is better than a fresh wrong one.
    """
    blocking = []

    high_ticker = [w for w in (ticker_warnings or []) if w.get("severity") == "high"]
    if high_ticker:
        blocking.append({
            "type": "ticker_direction_contradiction",
            "count": len(high_ticker),
            "examples": high_ticker[:5],
        })

    hard_prov = hard_provenance_warnings(provenance_warnings)
    if hard_prov:
        blocking.append({
            "type": "unverified_market_numbers",
            "count": len(hard_prov),
            "examples": hard_prov[:8],
        })

    high_narrative = [w for w in (narrative_warnings or []) if w.get("severity") == "high"]
    if high_narrative:
        blocking.append({
            "type": "unresolved_narrative_contradiction",
            "count": len(high_narrative),
            "examples": high_narrative[:5],
        })

    return blocking


# ══════════════════════════════════════════════════════════════
# AUTO-REPAIR PASS — fixes known failure locations before blocking publication
# ══════════════════════════════════════════════════════════════

def _flatten_quality_errors(errors):
    """Normalize direct gate errors and grouped publication-gate summaries.
    The repair layer is deliberately conservative: it removes or neutralizes only
    the exact line/bullet that a guard identified as unsafe."""
    flat = []
    for e in errors or []:
        if not isinstance(e, dict):
            continue
        parent_type = e.get("type")
        if parent_type and ("examples" in e) and isinstance(e.get("examples"), list):
            for ex in e.get("examples") or []:
                if isinstance(ex, dict):
                    xx = dict(ex)
                    xx.setdefault("type", parent_type)
                    flat.append(xx)
            continue
        flat.append(e)
    return flat


def _normalize_line_for_match(s):
    s = str(s or "").strip()
    s = re.sub(r'^[\s•\-–—]+', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s


def _loose_line_match(line, bad_line):
    a = _normalize_line_for_match(line)
    b = _normalize_line_for_match(bad_line)
    if not a or not b:
        return False
    if a == b:
        return True
    if len(b) >= 35 and (b in a or a in b):
        return True
    if len(b) >= 60:
        frag = b[:120]
        return frag in a or a[:120] in b
    return False


def _neutral_market_replacement(line):
    """Return safe wording for a line with unverified market levels."""
    l = line or ""
    if _has_any_ci(l, ["תשואה", "תשואת", "אג\"ח", "אגח", "Treasury"]):
        return "שוק האג\"ח: תשואות אג\"ח ממשלת ארה\"ב במוקד, אך רמת התשואה המדויקת לא אומתה במערכת."
    if _has_any_ci(l, ["נפט", "ברנט", "WTI", "חבית"]):
        return "שוק הנפט: מחירי הנפט במוקד, אך מחיר החבית המדויק לא אומת במערכת."
    if _has_any_ci(l, ["זהב", "אונקיה", "gold"]):
        return "שוק הזהב: מחיר הזהב במוקד, אך המחיר המדויק לא אומת במערכת."
    if _has_any_ci(l, ["ביטקוין", "Bitcoin", "BTC"]):
        return "שוק הקריפטו: הביטקוין במוקד, אך המחיר המדויק לא אומת במערכת."
    if _has_any_ci(l, ["VIX", "תנודתיות"]):
        return "מדד התנודתיות במוקד, אך רמת ה-VIX המדויקת לא אומתה במערכת."
    if _has_any_ci(l, ["S&P", "נאסד", "נאסד\"ק", "Nasdaq", "דאו", "Dow", "Russell", "מדד", "מדדים"]):
        return "המדדים המרכזיים במוקד, אך רמות המדדים המספריות לא אומתו במערכת."
    return None


def _strip_unverified_market_number(line, number):
    """Last-resort cleanup when a line contains one unsafe number but no clear replacement."""
    if not number:
        return line
    n = re.escape(str(number))
    patterns = [
        rf'(?:סביב|ברמה של|לרמה של|לרמה|של|כ[\-]?)\s*{n}\s*(?:%|דולר|נקודות|נק[\'׳]?|מיליון|מיליארד|אלף)?',
        rf'{n}\s*(?:%|דולר|נקודות|נק[\'׳]?|מיליון|מיליארד|אלף)?',
    ]
    out = line
    for pat in patterns:
        out = re.sub(pat, ' ', out)
    out = re.sub(r'\s{2,}', ' ', out).strip()
    out = re.sub(r'\s+([,.;:])', r'\1', out)
    return out


def auto_repair_blocking_issues(result, blocking_errors, review_type):
    """Repair deterministic quality-gate findings before deciding to block."""
    if not isinstance(result, dict):
        return result, 0
    flat = _flatten_quality_errors(blocking_errors)
    if not flat:
        return result, 0

    repairs = 0
    seen_replacements = set()

    def repair_line(line):
        nonlocal repairs
        original = line
        if not isinstance(line, str) or not line.strip():
            return line
        for e in flat:
            et = e.get("type")
            bad_line = e.get("line") or e.get("bullet") or e.get("original")
            if et in ("internal_direction_conflict", "unresolved_narrative_contradiction") and bad_line and _loose_line_match(line, bad_line):
                repairs += 1
                print(f"  ✅ Auto-repair removed unsafe line ({et})")
                return None
            if et == "ticker_direction_contradiction":
                ticker = e.get("ticker")
                claimed = e.get("claimed")
                if ticker and _bullet_contains_ticker(line, ticker):
                    if not claimed or _bullet_claims_direction(line) == claimed or (bad_line and _loose_line_match(line, bad_line)):
                        repairs += 1
                        print(f"  ✅ Auto-repair removed ticker sign-flip line (${ticker})")
                        return None
            if et == "forbidden_absolute_price" and bad_line and _loose_line_match(line, bad_line):
                repl = _neutral_market_replacement(line)
                repairs += 1
                if repl and repl not in seen_replacements:
                    seen_replacements.add(repl)
                    print("  ✅ Auto-repair neutralized unverified absolute price")
                    return repl
                print("  ✅ Auto-repair removed duplicate unverified absolute price line")
                return None
            if et in ("unverified_market_number", "unverified_market_numbers"):
                number = e.get("number")
                ctx = e.get("context") or ""
                has_number = bool(number and str(number) in line)
                has_context = bool(ctx and len(ctx) >= 25 and ctx in line)
                if (has_number or has_context) and _has_any_ci(line, _HARD_PROVENANCE_TERMS):
                    repl = _neutral_market_replacement(line)
                    repairs += 1
                    if repl and repl not in seen_replacements:
                        seen_replacements.add(repl)
                        print("  ✅ Auto-repair neutralized unverified market number")
                        return repl
                    if number:
                        fixed = _strip_unverified_market_number(line, number)
                        if fixed and fixed != original:
                            print("  ✅ Auto-repair stripped unverified market number")
                            return fixed
                    print("  ✅ Auto-repair removed unresolved unverified-number line")
                    return None
        return line

    def repair_text(text):
        if not isinstance(text, str):
            return text
        out = []
        for line in text.split("\n"):
            fixed = repair_line(line)
            if fixed is None:
                continue
            if str(fixed).strip():
                out.append(fixed)
        return "\n".join(out)

    for section in result.get("sections", []):
        content = section.get("content")
        if isinstance(content, str):
            section["content"] = repair_text(content)
        elif isinstance(content, list):
            new_items = []
            for item in content:
                if isinstance(item, str):
                    fixed = repair_line(item)
                    if fixed is not None and fixed.strip():
                        new_items.append(fixed)
                else:
                    new_items.append(item)
            section["content"] = new_items
    for item in result.get("items", []):
        if isinstance(item.get("title"), str):
            fixed = repair_line(item["title"])
            item["title"] = fixed or item["title"]
        if isinstance(item.get("description"), str):
            item["description"] = repair_text(item["description"])
    return result, repairs


def final_ticker_quality_gate(result):
    """Final pass after all repairs/fact-checking. Returns high-severity ticker contradictions still present."""
    tickers = extract_ticker_mentions(result)
    if not tickers:
        return result, []
    print(f"  Final ticker scan: {sorted(tickers)}")
    quotes = fetch_ticker_quotes(tickers)
    if not quotes:
        print("  ⚠️  Final ticker scan skipped: no verified quotes returned")
        return result, []
    result = apply_ticker_direction_guard(result, quotes)
    warnings = result.pop("_ticker_warnings", None) or []
    high = [dict(w, type="ticker_direction_contradiction") for w in warnings if w.get("severity") == "high"]
    return result, high

# ══════════════════════════════════════════════════════════════
# EDITORIAL PRE-FLIGHT (NEW — closes the "missing big story" gap)
# Runs BEFORE the main review prompt. Asks Gemini Flash to identify the top
# 5-7 stories from the tweet pool, with the cannot-miss aspect of each.
# The result is injected into the main prompt as a MUST-INCLUDE checklist.
#
# This is the fix for: GME-eBay $56B deal missing from the review, HSBC's UK
# fraud being more important than the Middle East provisions, PLTR after-hours
# being volatile and ending negative not just "the jump".
# ══════════════════════════════════════════════════════════════

def editorial_preflight(tweets, review_type):
    """Identify top 5-7 stories from the tweet pool with cannot-miss aspects.
    Returns formatted text block to inject into the main prompt, or "" on failure.

    Skipped for review_types where it doesn't help (events calendar, live_news
    which is already a 2-hour window).
    """
    if review_type in ("events", "live_news"):
        return ""
    if not tweets or not tweets.strip():
        return ""

    prompt = f"""You are an editorial pre-flight assistant for a Hebrew financial market review.

Below is a pool of tweets from financial news accounts on X (Twitter). Your job is to identify the top 5-7 most important market-moving stories — NOT to write a review, just to identify the stories that the main review writer must not miss.

CRITERIA — what makes a story "top tier":
- Concrete events with named companies or tickers (M&A, earnings, regulatory action, geopolitics, macro data).
- Events with hard numbers (dollar amounts, percentages, share counts).
- Stories that link multiple tweets — if 3+ tweets reference the same event, it's important.
- Major macro releases or central bank actions.
- Sign-flip or counter-narrative stories: a stock that beat earnings BUT fell, a bank that grew BUT missed estimates, a CEO who sold positions.

DO NOT include:
- Pure analyst commentary without a concrete trigger.
- Speculation, rumors, generic market color.
- Stories already covered exhaustively in earlier reviews — focus on what's NEW.

OUTPUT FORMAT — pure JSON, no backticks, no preamble:
{{
  "stories": [
    {{
      "rank": 1,
      "headline": "Brief one-line description of the story (English ok, will be translated)",
      "tickers": ["GME", "EBAY"],
      "cannot_miss": "The single most important fact or angle that the review must include when covering this story. Example: 'Burry sold his entire GME stake AS A DIRECT RESPONSE to this acquisition offer — the two are linked, not separate stories.'"
    }}
  ]
}}

Include 5-7 stories. Rank #1 = most important. Each "cannot_miss" must be a SPECIFIC fact or angle, not a generic instruction.

TWEETS:
{tweets[:15000]}

Return ONLY the JSON object."""

    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048}
            },
            timeout=60
        )
        if not r.ok:
            print(f"  Pre-flight Gemini status {r.status_code}, skipping")
            return ""

        resp_data = r.json()
        candidate = resp_data.get("candidates", [{}])[0]
        parts = candidate.get("content", {}).get("parts", [])
        text = ""
        for part in parts:
            if "text" in part:
                text = part["text"]

        if not text:
            print("  Pre-flight returned no text, skipping")
            return ""

        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        # Extract the first complete JSON object
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

        parsed = json.loads(text)
        stories = parsed.get("stories", [])

        if not stories or not isinstance(stories, list):
            print("  Pre-flight returned no usable stories, skipping")
            return ""

        # Trim to 7 max, build the prompt block
        stories = stories[:7]
        lines = [
            "",
            "══ EDITORIAL PRE-FLIGHT — TOP STORIES IDENTIFIED FROM TWEET POOL ══",
            "These are the most important stories in today's tweets. The review MUST cover stories #1-#3 at minimum.",
            "When covering ANY of these stories, the review MUST include the 'cannot_miss' angle — that is the editorial line that prevents the embarrassing 'half-truth' coverage.",
            "Do NOT just mention the story by name. Include the specific number, link, or angle marked as cannot_miss.",
            ""
        ]
        for s in stories:
            rank = s.get("rank", "?")
            headline = s.get("headline", "")
            tickers = s.get("tickers", []) or []
            cannot_miss = s.get("cannot_miss", "")
            tickers_str = ", ".join(f"${t}" for t in tickers if t) if tickers else "(no tickers)"
            lines.append(f"#{rank}: {headline}")
            lines.append(f"  Tickers: {tickers_str}")
            lines.append(f"  CANNOT-MISS ANGLE: {cannot_miss}")
            lines.append("")
        lines.append("══════════════════════════════════════════════════════════════════════════════════════════")
        lines.append("")

        block = "\n".join(lines)
        print(f"  ✅ Pre-flight identified {len(stories)} top stories")
        for s in stories[:3]:
            print(f"     #{s.get('rank', '?')}: {s.get('headline', '')[:80]}")
        return block

    except json.JSONDecodeError as e:
        print(f"  Pre-flight JSON parse error: {e}, skipping")
        return ""
    except Exception as e:
        print(f"  Pre-flight failed: {e}, skipping")
        return ""


# ══════════════════════════════════════════════════════════════
# FACT-CHECKER
# ══════════════════════════════════════════════════════════════

def fact_check_with_gemini(result, market_data, review_type, provenance_warnings=None, ticker_warnings=None):
    """Flash-based fact check. Runs AFTER enforce_structure so the structure it sees is already correct.
    If provenance_warnings is provided, the fact-checker is instructed to remove bullets whose
    numbers cannot be verified against sources.
    If ticker_warnings is provided, the fact-checker is instructed to fix or remove bullets
    whose directional claim about a ticker contradicts the Finnhub quote."""
    # Strip internal metadata before serializing for the model
    clean_result = {k: v for k, v in result.items() if not k.startswith("_")}
    review_json = json.dumps(clean_result, ensure_ascii=False, indent=2)

    # Build the provenance block for the fact-checker prompt
    provenance_block = ""
    if provenance_warnings:
        lines = ["\nPROVENANCE WARNINGS — these numbers from the review were NOT found in any source:"]
        for w in provenance_warnings[:15]:
            lines.append(f"- In {w['label']}: number '{w['number']}' (context: ...{w['context']}...)")
        lines.append("\nFor each warning above: either (a) the number is correct and you can verify it via your own knowledge — keep the bullet; or (b) the number is a hallucination — REMOVE the entire bullet containing that number from the content. Do NOT just silently fix the number to something else — if it can't be verified, remove the claim.")
        provenance_block = "\n".join(lines)

    # Build the ticker direction block — these are sign-flip errors caught by Finnhub
    ticker_block = ""
    if ticker_warnings:
        high_sev = [w for w in ticker_warnings if w.get("severity") == "high"]
        if high_sev:
            lines = ["\nTICKER DIRECTION CONTRADICTIONS — these bullets claim a price direction that CONTRADICTS the verified Finnhub quote:"]
            for w in high_sev[:10]:
                lines.append(
                    f"- ${w['ticker']}: bullet claims '{w['claimed']}' but Finnhub shows {w['actual']} ({w['actual_dir']})."
                )
                lines.append(f"  Offending bullet: {w['bullet'][:300]}")
            lines.append("")
            lines.append("FIX RULE for each contradiction above:")
            lines.append("- If the bullet's claim about the stock direction can be re-stated correctly using the Finnhub %, REWRITE it with the correct direction and percentage. Keep the surrounding context (earnings result, news event, etc.) but flip the directional words and numbers.")
            lines.append("- If the bullet was written ONLY about the price move (no real news content), REMOVE the entire bullet.")
            lines.append("- NEVER leave a bullet that says a stock is going up when Finnhub shows it down, or vice versa. This is the most embarrassing kind of error.")
            ticker_block = "\n".join(lines)

    prompt = f"""You are a FACT-CHECKER for a Hebrew financial market review. Your ONLY job is to find and fix factual errors.

VERIFIED MARKET DATA (100% correct, sourced from Finnhub API):
{market_data if market_data else "(No Finnhub data available for this run)"}
{provenance_block}
{ticker_block}

THE REVIEW TO CHECK:
{review_json}

YOUR TASK:
- Compare EVERY number, percentage, and factual claim in the review against the verified data and your own knowledge.
- Fix any number that contradicts the verified data.
- Fix any factual error (wrong company attribution, wrong political titles, wrong dates, wrong terminology).
- For sector ETF percentages (XLE/XLK/XLF/XLY/XLV/XLI): if a specific sector number appears in the review that does NOT match the Finnhub data, REMOVE that claim or replace it with a number from the Finnhub data.
- For 10-year Treasury yield, commodity absolute prices ($/barrel, $/oz), Bitcoin $ level, and VIX point level: these are NOT in the verified deterministic data. REMOVE those absolute-price claims from automated reviews unless explicitly present in the verified block.
- DO NOT change the writing style, structure, section count, or section headings.
- DO NOT remove content — only fix errors or remove clearly-hallucinated numbers.
- EXCEPTION: if PROVENANCE WARNINGS above flag a number you cannot verify, remove the entire bullet containing it (see provenance instructions above).
- EXCEPTION: if TICKER DIRECTION CONTRADICTIONS above are listed, follow the FIX RULE for each — flipping direction words and percentages to match Finnhub, or removing the bullet entirely.
- DO NOT change the "title" field or section headings — those are already enforced.
- If everything is correct, return the review unchanged.

CROSS-LINK RELATED EVENTS:
- Scan the bullets for events that are causally linked but appear as separate, disconnected items.
- Common pattern: one bullet says "Investor X sold all of stock Y" and another bullet says "Y made acquisition offer for Z". These are linked — the sale was driven by the acquisition news.
- Another pattern: one bullet describes a stock falling, another describes the news that caused the fall.
- When you detect such links, MERGE them into a single bullet that explains the causal connection. Do NOT leave linked events as disconnected facts.
- Only merge when the link is clear from the content. Do not invent connections.

COMMON ERRORS TO CATCH:
- Donald Trump is the CURRENT US President (since Jan 2025). NOT a former president.
- Claude is by Anthropic, ChatGPT is by OpenAI, Gemini is by Google.
- IPO ≠ ETF.
- ADP Employment Report is MONTHLY, not weekly. Any "weekly ADP" number is a hallucination — remove it.
- Contradictions: if one bullet says the market rose sharply, another bullet must not describe mixed or weak trading without explaining the distinction.
- Self-contradicting phrases like "נותרו יציבות עם עלייה של X%" — resolve to one or the other.
- Directional wording must match the verified market data. If oil proxies are positive, phrases like "מחירי הנפט צונחים" are factual errors. If oil proxies are negative, phrases like "מחירי הנפט מזנקים" are factual errors.
- Geopolitical softening: if the review describes the US-Iran war as "tensions" or "escalation" or "diplomatic crisis", REWRITE using accurate terms (מלחמה, מבצע צבאי, תקיפה).

OUTPUT: Return the corrected review as valid JSON in EXACTLY the same structure (same title, same section headings, same number of sections — usually exactly 1 section under the current output-format rules). No backticks, no explanations — pure JSON only."""

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

    # Compute the canonical "review date" that will be forced onto result["date"].
    # Gemini hallucinates dates (e.g. weekly_summary date showing 5/9/2026 in the future).
    # We override deterministically based on the review type.
    if REVIEW_TYPE in ("daily_prep", "daily_summary"):
        review_date = title_date_str  # the trading day this review is about
    elif REVIEW_TYPE == "weekly_prep":
        # Monday of the upcoming/current trading week
        weekday = now.weekday()
        if weekday <= 4:
            wp_monday = now - timedelta(days=weekday)
        else:
            wp_monday = now + timedelta(days=(7 - weekday))
        review_date = wp_monday.strftime("%Y-%m-%d")
    elif REVIEW_TYPE == "weekly_summary":
        # Friday of the trading week that just ended
        weekday = now.weekday()  # Mon=0 ... Sun=6
        days_since_friday = (weekday - 4) % 7
        last_friday = now - timedelta(days=days_since_friday)
        review_date = last_friday.strftime("%Y-%m-%d")
    else:  # live_news
        review_date = date_str
    print(f"  Review date (will be forced onto output): {review_date}")

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

    # Hard reliability gate converted to SAFE-PUBLISH mode.
    # Doron preference: never leave the site without a new review.
    # If deterministic market data is missing, continue generation but the final gate may
    # replace the draft with a conservative numeric-free safe-mode review.
    if REVIEW_TYPE in ("daily_prep", "daily_summary", "weekly_prep", "weekly_summary", "live_news") and not _LAST_MARKET_DATA.get("pcts"):
        print("⚠️ No deterministic Finnhub market data available — continuing in ALWAYS-PUBLISH safe mode")

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

    # Editorial pre-flight: identify top stories from the tweet pool BEFORE writing.
    # This block becomes a MUST-INCLUDE checklist in the main prompt, preventing
    # major stories (e.g. M&A deals) from being missed and preventing half-truth
    # framing of stories where there's a counter-narrative angle.
    print("\n── Editorial pre-flight ──")
    editorial_block = editorial_preflight(tweets, REVIEW_TYPE)
    if editorial_block:
        print(f"  Editorial block: {len(editorial_block)} chars injected into main prompt")
    else:
        print(f"  No editorial block (review_type={REVIEW_TYPE} or pre-flight skipped)")

    prompt = get_prompt(
        tweets, REVIEW_TYPE, date_str, day_name,
        title_date_str=title_date_str,
        title_day_name=title_day_name,
        week_range=week_range,
        is_trading=target_is_trading if REVIEW_TYPE == "daily_prep" else today_is_trading,
        market_data=market_data,
        prior_context=prior_context,
        expected_title=expected_title,
        editorial_block=editorial_block,
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

    # Layer 2: Structure enforcement — forces title, section count, heading names, bullet format
    print("\n── Layer 2: Structure enforcement ──")
    result = enforce_structure(result, REVIEW_TYPE, expected_title)

    # Layer 3: Number provenance check — flags numbers absent from source bundle
    print("\n── Layer 3: Number provenance ──")
    source_bundle = build_source_bundle(market_data, tweets, prior_context)
    result = number_provenance_check(result, source_bundle, REVIEW_TYPE)
    provenance_warnings = result.pop("_provenance_warnings", None)

    # Layer 3b: Per-ticker direction guard — pulls live Finnhub quotes for every
    # $TICKER mentioned in the review and flags sign-flip contradictions.
    # This is the fix for the PLTR-style "stock up 2.6%" when actually down -2.6%.
    print("\n── Layer 3b: Per-ticker direction guard ──")
    mentioned_tickers = extract_ticker_mentions(result)
    if mentioned_tickers:
        print(f"  Tickers mentioned in review: {sorted(mentioned_tickers)}")
        ticker_quotes = fetch_ticker_quotes(mentioned_tickers)
        result = apply_ticker_direction_guard(result, ticker_quotes)
    else:
        print("  No tickers mentioned (or all excluded) — skipping per-ticker check")
    ticker_warnings = result.pop("_ticker_warnings", None)

    # Layer 4: Gemini Flash fact-checker — uses provenance + ticker warnings to fix or drop bullets
    print("\n── Layer 4: Gemini fact-checker ──")
    result = fact_check_with_gemini(result, market_data, REVIEW_TYPE,
                                     provenance_warnings=provenance_warnings,
                                     ticker_warnings=ticker_warnings)

    # Layer 4b: Deterministic market-direction guard — fixes words like צונח/מזנק if they contradict Finnhub data
    print("\n── Layer 4b: Market direction guard ──")
    result = apply_market_direction_guard(result, REVIEW_TYPE)

    # Layer 4c: Narrative consistency guard — catches causal contradictions such as
    # "BTC fell because risk appetite increased" or "yields rose because investors fled into bonds".
    print("\n── Layer 4c: Narrative consistency guard ──")
    result, narrative_warnings = apply_narrative_consistency_guard(result, REVIEW_TYPE)
    narrative_warnings = result.pop("_narrative_warnings", narrative_warnings or None)

    # Layer 4d-pre: final deterministic hard quality gate after all fixes.
    # Historical warnings from earlier layers are repair signals, not final blockers.
    # The final decision is based only on what is STILL present after fact-checking,
    # deterministic guards, and the auto-repair pass below.
    print("\n── Layer 4d-pre: Final hard quality gate ──")
    result, final_ticker_blockers = final_ticker_quality_gate(result)
    final_blocking_errors = final_hard_quality_gate(result, source_bundle, REVIEW_TYPE)
    blocking_errors = list(final_ticker_blockers) + list(final_blocking_errors)

    if blocking_errors:
        print(f"  ⚠️ Final hard gate found {len(blocking_errors)} issue(s). Attempting auto-repair...")
        for err in blocking_errors[:8]:
            print(f"     - {err.get('type')}: {err.get('line', err.get('bullet', err.get('context', '')))[:220]}")
        result, repair_count = auto_repair_blocking_issues(result, blocking_errors, REVIEW_TYPE)
        if repair_count:
            print(f"  ✅ Auto-repair applied {repair_count} fix(es). Re-running final gates...")
            result = enforce_structure(result, REVIEW_TYPE, expected_title)
            result = apply_market_direction_guard(result, REVIEW_TYPE)
            result, final_ticker_blockers = final_ticker_quality_gate(result)
            final_blocking_errors = final_hard_quality_gate(result, source_bundle, REVIEW_TYPE)
            blocking_errors = list(final_ticker_blockers) + list(final_blocking_errors)
        else:
            print("  ⚠️ Auto-repair had no safe deterministic fix")

    if blocking_errors:
        print(f"  ⚠️ Final hard gate still has {len(blocking_errors)} blocking issue(s)")
        for err in blocking_errors[:8]:
            print(f"     - {err.get('type')}: {err.get('line', err.get('bullet', err.get('context', '')))[:220]}")
    else:
        print("  ✅ Final hard gate passed")

    # Layer 4d: Publication gate — ALWAYS-PUBLISH mode.
    # If a contradiction remains after auto-repair, publish a conservative safe-mode
    # review instead of failing the GitHub Action and leaving the site stale.
    print("\n── Layer 4d: Publication gate ──")
    if blocking_errors:
        print("⚠️ Auto-repair could not clean all issues. Publishing SAFE-MODE review instead of blocking.")
        for err in blocking_errors:
            print(f"  - {err.get('type')}: {err.get('count', 1)}")
        save_failed_review(result, blocking_errors, REVIEW_TYPE)
        result = build_safe_publish_review(result, REVIEW_TYPE, expected_title, review_date, blocking_errors)
        result = enforce_structure(result, REVIEW_TYPE, expected_title)
        print("  ✅ Safe-mode review built. Publication will continue.")
    else:
        print("  ✅ Publication gate passed")

    # Layer 5: Re-enforce structure (defensive — fact-checker sometimes alters section headings)
    print("\n── Layer 5: Final structure enforcement ──")
    result = enforce_structure(result, REVIEW_TYPE, expected_title)

    # Layer 6: Pre-market tense guard (daily_prep only, only if run before US market open)
    print("\n── Layer 6: Pre-market tense guard ──")
    result = apply_pre_market_tense_guard(result, REVIEW_TYPE)
    print("── Validation complete ──\n")

    # Defensive: strip any internal metadata before persisting
    result = {k: v for k, v in result.items() if not k.startswith("_")}

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
        # Force the canonical date — Gemini sometimes writes nonsense dates
        # (e.g. weekly_summary returning 2026-09-05 when the week ended 2026-05-08).
        original_date = result.get("date", "")
        if original_date != review_date:
            print(f"  ✅ Date overridden: '{original_date}' → '{review_date}'")
        result["date"] = review_date
        data[key_map[REVIEW_TYPE]] = result

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Updated {REVIEW_TYPE} successfully.")

if __name__ == "__main__":
    main()
