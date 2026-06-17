# Wall Street IL, fixed file structure

החבילה הזו מסדרת את הקבצים לפי המבנה שהאתר וה-GitHub Actions מצפים לו.

## מה להעלות לריפו

העלה את כל התוכן של התיקייה הזו לשורש הריפו, תוך שמירה על הנתיבים:

```text
index.html
daily-prep.html
daily-summary.html
weekly-prep.html
weekly-summary.html
events.html
live-news.html
data.json
scripts/update_data.py
.github/workflows/update.yml
```

## מה היה שבור

- קובץ ה-Workflow הופיע בשם daily-prep.html.
- סקריפט הפייתון הופיע בשם data.json.
- קובץ הנתונים האמיתי הופיע בשם live-news.html.
- דפי weekly-prep.html ו-live-news.html לא היו זמינים כעמודי HTML תקינים בחבילה שהועלתה.

## אחרי העלאה

1. להיכנס ל-GitHub Actions.
2. לפתוח את Update Wall Street IL.
3. ללחוץ Run workflow.
4. לבחור daily_prep.
5. לבדוק שהריצה יוצרת commit שמעדכן את data.json.

## Secrets נדרשים

ה-Workflow הנוכחי מעביר לסקריפט את:

- GEMINI_API_KEY
- TWITTER_API_KEY
- FINNHUB_API_KEY

אם אחד מהם חסר, הריצה תיכשל.
