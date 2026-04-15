import json, os, re, requests
from datetime import datetime, timezone, timedelta

# הגדרות בסיסיות
ISR_TZ = timezone(timedelta(hours=3))
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
REVIEW_TYPE = os.environ.get("REVIEW_TYPE", "daily_prep")

ACCOUNTS = ["AIStockSavvy","wallstengine","DeIaone","StockMKTNewz","zerohedge","financialjuice"]

def get_israel_time():
    return datetime.now(ISR_TZ)

def fetch_market_data(weekly=False):
    """משיכת נתונים מדויקים מ-Finnhub למניעת הזיות של המודל"""
    if not FINNHUB_API_KEY: return "No API Key"
    
    symbols = {"SPY": "S&P 500", "QQQ": "Nasdaq 100", "DIA": "Dow Jones", "GLD": "Gold", "USO": "WTI Oil"}
    verified_data = []
    
    for sym, label in symbols.items():
        try:
            r = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_API_KEY}", timeout=5)
            if r.ok:
                d = r.json()
                # חישוב רמת המדד (מקורב לפי ה-ETF)
                mult = 10 if sym == "SPY" else (40 if sym == "QQQ" else 100)
                level = d['c'] * mult if sym in ["SPY", "QQQ", "DIA"] else d['c']
                verified_data.append(f"{label}: {level:,.2f} ({d['dp']}% today)")
        except: continue
        
    return "\n".join(verified_data)

def get_system_prompt(review_type):
    """פרומפטים קצרים וממוקדים לכל סוג סקירה"""
    prompts = {
        "daily_summary": """אתה אנליסט בכיר בוול סטריט. תפקידך לסכם את יום המסחר בעברית מקצועית.
כללים:
- השתמש אך ורק במספרים ובאחוזים המופיעים בבלוק 'VERIFIED DATA'.
- אל תבצע חישובים בעצמך.
- טראמפ הוא נשיא ארה"ב המכהן (2026).
- Claude מפותח ע"י Anthropic. ChatGPT ע"י OpenAI. Gemini ע"י Google.
- פורמט: JSON בלבד עם שדות title, date, sections.""",
        
        "daily_prep": """אתה מכין תדריך בוקר לפני פתיחת המסחר.
כללים:
- התמקד באירועים צפויים (יומן כלכלי, דוחות) ובחדשות לילה.
- אל תסכם את מה שקרה אתמול, אלא מה יזיז את השוק היום.
- פורמט: JSON בלבד."""
    }
    return prompts.get(review_type, prompts["daily_summary"])

def call_gemini(prompt, system_instruction):
    """קריאה ל-Gemini עם הפרדה בין הוראות מערכת לתוכן"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "system_instruction": {"parts": [{"text": system_instruction}]},
        "generationConfig": {
            "temperature": 0.2, # טמפרטורה נמוכה לדיוק מקסימלי
            "response_mime_type": "application/json"
        }
    }
    
    r = requests.post(url, json=payload)
    if not r.ok: 
        print(f"Error: {r.text}")
        return None
        
    # ניקוי הטקסט והפיכה ל-JSON
    result = r.json()['candidates'][0]['content']['parts'][0]['text']
    return json.loads(result)

def main():
    now = get_israel_time()
    market_info = fetch_market_data(weekly=("weekly" in REVIEW_TYPE))
    tweets = "TWEETS DATA HERE" # פונקציית הטוויטים שלך
    
    full_prompt = f"""
    DATE: {now.strftime('%Y-%m-%d')}
    VERIFIED DATA (USE THESE NUMBERS ONLY):
    {market_info}
    
    NEWS FEED:
    {tweets}
    """
    
    system_instr = get_system_prompt(REVIEW_TYPE)
    result = call_gemini(full_prompt, system_instr)
    
    if result:
        # שמירה ל-data.json
        with open("data.json", "r+", encoding="utf-8") as f:
            data = json.load(f)
            # עדכון השדה המתאים (dailySummary, dailyPrep וכו')
            # ... לוגיקת השמירה שלך ...
            f.seek(0)
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.truncate()

if __name__ == "__main__":
    main()
