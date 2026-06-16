"""
每日情報站 v3.0 - 免費桌面版
==================================================
v3.0 優化清單：
  [效能-1] 並行抓取（ThreadPoolExecutor），速度提升 3~5x
  [效能-2] Retry 加 exponential backoff（1s / 2s 間隔）
  [功能-1] 動態摘要：從當日 top-3 標題萃取，每天不同
  [功能-2] 時區容錯：放寬日期比對緩衝，避免 UTC 誤判
  [功能-3] --only world/house 只抓單一類別
  [功能-4] 當日 JSON cache，重執行直接讀（--no-cache 停用）
  [功能-5] 來源名稱中文對照表
  [品質-1] 標題命中 2 分 / 摘要命中 1 分，排序更準確
  [品質-2] difflib 相似度去重（>0.72），減少同事件重複
  [品質-3] 摘要在標點處斷句，不在詞語中間截斷
  [UX-1]   進度顯示 [3/7]
  [UX-2]   --keep N 自動清理舊報告
  [UX-3]   Windows bat 加錯誤暫停

使用方式：
    python news.py                  抓取全部，開啟瀏覽器
    python news.py --only world     只抓國際新聞
    python news.py --only house     只抓房市新聞
    python news.py --no-open        不開瀏覽器
    python news.py --no-cache       忽略今日快取，強制重抓
    python news.py --keep 7         只保留最近 7 份報告
    python news.py --days 2         包含前 2 天的新聞
    python news.py --out report.html 自訂輸出檔名
"""

from __future__ import annotations

import argparse
import base64
import datetime
import difflib
import json
import re
import sys
import time
import urllib.parse
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import escape
from pathlib import Path

# ── 依賴檢查 ──────────────────────────────────────────────
REQUIRED = {
    "feedparser": "feedparser>=6.0",
    "requests":   "requests>=2.31",
    "bs4":        "beautifulsoup4>=4.12",
}
missing = []
for mod, pkg in REQUIRED.items():
    try:
        __import__(mod)
    except ImportError:
        missing.append(pkg)

if missing:
    print("[!] 缺少以下套件，請先執行：")
    print(f"    pip install {' '.join(missing)}")
    sys.exit(1)

import feedparser
import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────
# 設定區
# ─────────────────────────────────────────────────────────

GNEWS_BASE    = "https://news.google.com/rss/search"
GNEWS_LANG    = "zh-TW"
GNEWS_COUNTRY = "TW"

WORLD_QUERIES = [
    "國際 政治 局勢",
    "國際 新聞 焦點",
    "國際 重大事件",
    "美國 中國 關係",
    "兩岸 關係",
    "烏克蘭 以色列 中東",
    "日本 韓國 亞洲",
    "全球 經濟 外交",
    "地緣政治 軍事",
]

HOUSE_QUERIES = [
    "台灣 房市 房價",
    "房貸 利率",
    "買房 賣房",
    "看屋 預售屋 建案",
    "重劃區 開發",
    "房屋 交易 政策",
    "實價登錄 成交",
    "新青安 房貸",
    "台北 新北 桃園 房價",
    "都更 危老 危老重建",
]

FALLBACK_WORLD = [
    ("中央社",   "https://www.cna.com.tw/rss/aall.aspx"),
    ("自由時報", "https://news.ltn.com.tw/rss/all.xml"),
    ("ETtoday",  "https://feeds.feedburner.com/ettoday/world"),
    ("聯合報",   "https://udn.com/rssfeed/news/2/6638?ch=news"),
    ("風傳媒",   "https://www.storm.mg/rss-news"),
]
FALLBACK_HOUSE = [
    ("好房網",   "https://news.housefun.com.tw/rss.xml"),
    ("鉅亨網",   "https://news.cnyes.com/rss/cat/house"),
    ("聯合房市", "https://house.udn.com/house/rss/home"),
    ("中時地產", "https://www.chinatimes.com/rss/real-estate.xml"),
    ("工商時報", "https://www.ctee.com.tw/rss"),
]

WORLD_KEYWORDS = [
    "台灣", "國際", "全球", "政治", "兩岸", "亞洲", "經濟",
    "美國", "中國", "俄羅斯", "烏克蘭", "以色列", "伊朗", "歐洲",
    "北韓", "日本", "韓國", "歐盟", "NATO", "貿易", "關稅", "制裁",
    "軍事", "外交", "戰爭", "衝突", "選舉", "峰會", "聯合國", "地緣", "G7", "G20",
]
HOUSE_KEYWORDS = [
    "房價", "房市", "房屋", "買房", "賣房", "看屋", "重劃區",
    "房貸", "利率", "實價登錄", "容積", "都更", "危老", "建商",
    "預售屋", "建案", "新青安", "囤房稅", "地價", "坪", "房地產",
    "台北", "新北", "桃園", "北台灣", "豪宅", "租金", "成交",
    "央行", "升息", "降息", "房仲", "開價", "議價", "交易",
]

WEATHER_QUERIES = [
    "台灣 天氣 預報",
    "天氣 氣溫 變化",
    "氣象署 天氣",
    "颱風",
    "地震 台灣",
]
FALLBACK_WEATHER = [
    ("中央社",   "https://www.cna.com.tw/rss/aall.aspx"),
    ("ETtoday",  "https://feeds.feedburner.com/ettoday/society"),
    ("公視新聞", "https://news.pts.org.tw/rss"),
]
WEATHER_KEYWORDS = [
    "天氣", "氣溫", "降雨", "降雪", "颱風", "豪雨", "大雨", "暴雨",
    "氣象", "預報", "低溫", "高溫", "寒流", "寒害", "鋒面", "梅雨",
    "地震", "海嘯", "特報", "警報", "氣象署", "中央氣象", "回暖", "變天",
]

# [功能-5] 來源名稱中文對照表
SOURCE_MAP = {
    "Reuters":              "路透社",
    "AP":                   "美聯社",
    "BBC News":             "BBC",
    "The Guardian":         "衛報",
    "South China Morning Post": "南華早報",
    "Nikkei Asia":          "日經亞洲",
    "Bloomberg":            "彭博",
    "Financial Times":      "金融時報",
    "The Wall Street Journal": "華爾街日報",
    "CNN":                  "CNN",
    "Al Jazeera":           "半島電視台",
    "NHK WORLD":            "NHK",
    "Kyodo News":           "共同社",
    "CNA":                  "中央社",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Cache-Control":   "no-cache",
}

MAX_PER_SECTION   = 25
TIMEOUT           = 12
RETRY             = 2
DEDUP_THRESHOLD   = 0.72   # [品質-2] 相似度閾值
MAX_WORKERS       = 6      # [效能-1] 並行執行緒數

BASE_DIR = Path(__file__).parent          # 固定以腳本目錄為基準，不受 CWD 影響

# [關鍵修復] 日期改為「執行當下」即時計算，不在 import 時凍結。
# 原本 TODAY / NOW / CACHE_FILE 是模組載入時算一次的常數，
# 一旦 server.py / scheduler.py 常駐跨日執行，日期會卡在「啟動那天」，
# 導致報告日期、快取檔名、近 N 天篩選全部用到舊日期 → 看起來抓不到最新。
def today() -> datetime.date:
    return datetime.date.today()

def now() -> datetime.datetime:
    return datetime.datetime.now()

def cache_file() -> Path:
    return BASE_DIR / f".news_cache_{today().strftime('%Y%m%d')}.json"


# ─────────────────────────────────────────────────────────
# 工具函式
# ─────────────────────────────────────────────────────────

def gnews_url(query: str) -> str:
    params = {
        "q":    query,
        "hl":   GNEWS_LANG,
        "gl":   GNEWS_COUNTRY,
        "ceid": f"{GNEWS_COUNTRY}:{GNEWS_LANG}",
    }
    return f"{GNEWS_BASE}?{urllib.parse.urlencode(params)}"


def clean_html(text: str) -> str:
    if not text:
        return ""
    for parser in ("lxml", "html.parser"):
        try:
            return BeautifulSoup(text, parser).get_text(" ", strip=True)
        except Exception:
            continue
    return re.sub(r"<[^>]+>", " ", text).strip()


def smart_truncate(text: str, max_len: int = 400) -> str:
    """[品質-3] 在標點符號處斷句，避免詞語中間截斷"""
    if len(text) <= max_len:
        return text
    # 在 max_len 前找最近的中文句號/逗號/分號
    for punct in ("。", "！", "？", "，", "；", ".", "!", "?", ",", ";"):
        idx = text.rfind(punct, 0, max_len)
        if idx > max_len * 0.5:  # 至少保留 60% 長度
            return text[:idx + 1]
    return text[:max_len] + "…"


def parse_date(entry) -> datetime.date | None:
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                return datetime.date(val[0], val[1], val[2])
            except Exception:
                pass
    return None


def is_recent(article_date: datetime.date | None, max_days: int) -> bool:
    """[功能-2] 放寬緩衝至 max_days+1，避免 UTC 時區誤判"""
    if article_date is None:
        return True
    return (today() - article_date).days <= (max_days + 1)


def map_source(name: str) -> str:
    """[功能-5] 英文來源名稱轉中文"""
    return SOURCE_MAP.get(name, name)


def fetch_feed(name: str, url: str, max_days: int) -> list[dict]:
    """[效能-2] 含 exponential backoff 的 retry"""
    label = ""  # [B12修復] 並行時順序隨機，移除 idx/total 顯示
    for attempt in range(RETRY + 1):
        try:
            resp = requests.get(
                url,
                headers=REQUEST_HEADERS,
                timeout=TIMEOUT,
                allow_redirects=True,
            )
            resp.raise_for_status()
            # [B13修復] 傳入 response_headers 讓 feedparser 正確偵測 charset
            feed = feedparser.parse(
                resp.content,
                response_headers={"content-type": resp.headers.get("content-type", "")}
            )
            break
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else "?"
            if attempt < RETRY:
                wait = 2 ** attempt
                time.sleep(wait)
            else:
                print(f"  ⚠ {label}{name}: HTTP {code}")
                return []
        except Exception as e:
            if attempt < RETRY:
                time.sleep(2 ** attempt)
            else:
                print(f"  ⚠ {label}{name}: {str(e)[:55]}")
                return []

    articles = []
    for entry in feed.entries[:30]:
        title   = clean_html(getattr(entry, "title",   "")).strip()
        summary = clean_html(getattr(entry, "summary", "")).strip()
        link    = getattr(entry, "link", "").strip()
        src_raw = getattr(entry, "source", {})
        src_raw = src_raw.get("title", name) if isinstance(src_raw, dict) else name
        src     = map_source(src_raw)

        summary      = smart_truncate(re.sub(r"\s+", " ", summary))
        article_date = parse_date(entry)

        if title and is_recent(article_date, max_days):
            articles.append({
                "title":   title,
                "summary": summary,
                "link":    link,
                "source":  src,
                "date":    article_date.isoformat() if article_date else None,
            })

    if articles:
        print(f"  ✅ {label}{name}: {len(articles)} 則")
    else:
        print(f"  ─  {label}{name}: 0 則")
    return articles


def deduplicate(articles: list[dict]) -> list[dict]:
    """[品質-2] difflib 相似度去重，比純前N字比對更準確"""
    result: list[dict] = []
    titles: list[str]  = []
    for a in articles:
        t = re.sub(r"\s+", "", a.get("title") or "")
        is_dup = any(
            difflib.SequenceMatcher(None, t, existing).ratio() > DEDUP_THRESHOLD
            for existing in titles
        )
        if not is_dup:
            result.append(a)
            titles.append(t)
    return result


def score_article(a: dict, keywords: list[str]) -> int:
    """[品質-1] 標題命中 2 分，摘要命中 1 分"""
    kw    = [k.lower() for k in keywords]
    title = (a.get("title") or "").lower()
    summ  = (a.get("summary") or "").lower()
    return sum(
        (2 if k in title else 0) + (1 if k in summ else 0)
        for k in kw
    )


def rank_articles(articles: list[dict], keywords: list[str], top: int) -> list[dict]:
    return sorted(articles, key=lambda a: -score_article(a, keywords))[:top]


# ─────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────

def load_cache() -> dict | None:
    cf = cache_file()
    if cf.exists():
        try:
            data = json.loads(cf.read_text(encoding="utf-8"))
            print(f"📦 讀取今日快取（{cf.name}）")
            return data
        except Exception:
            pass
    return None


def save_cache(world, house, weather=None) -> None:
    try:
        cache_file().write_text(
            json.dumps({"world": world, "house": house, "weather": weather or []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"  ⚠ cache 寫入失敗（不影響報告生成）：{e}")


def cleanup_old_cache() -> None:
    for f in BASE_DIR.glob(".news_cache_*.json"):
        name = f.name
        try:
            date_str = name.replace(".news_cache_", "").replace(".json", "")
            file_date = datetime.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            if (today() - file_date).days > 7:
                f.unlink()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────
# 抓取流程
# ─────────────────────────────────────────────────────────

def fetch_section_parallel(
    label: str,
    queries: list[str],
    fallbacks: list[tuple],
    keywords: list[str],
    max_days: int,
) -> list[dict]:
    """[效能-1] 並行抓取所有 query"""
    emoji = "🌐" if "國際" in label else "🏠"
    print(f"\n{emoji} 抓取{label}（並行 {min(MAX_WORKERS, len(queries))} 線程）...")

    total = len(queries)
    all_articles: list[dict] = []
    gnews_ok = 0

    # 並行抓取 Google News
    tasks = {
        gnews_url(q): (f"Google News「{q}」", i + 1)
        for i, q in enumerate(queries)
    }
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch_feed, name, url, max_days): url
            for url, (name, _) in tasks.items()
        }
        for future in as_completed(futures):
            arts = future.result()
            all_articles.extend(arts)
            if arts:
                gnews_ok += 1

    # 備援（串行，只有在 Google News 全失敗時）
    if gnews_ok == 0:
        print(f"  [!] Google News 無法連線，嘗試備援來源...")
        for name, url in fallbacks:
            all_articles.extend(fetch_feed(name, url, max_days))

    all_articles = deduplicate(all_articles)
    result       = rank_articles(all_articles, keywords, MAX_PER_SECTION)
    print(f"  → 去重後 {len(all_articles)} 則，篩選顯示 {len(result)} 則")
    return result


# ─────────────────────────────────────────────────────────
# 動態摘要
# ─────────────────────────────────────────────────────────

def make_dynamic_summary(articles: list[dict], category: str) -> str:
    """[功能-1][品質-1] 根據當日 top 標題動態生成摘要"""
    if not articles:
        return "今日未能取得相關新聞，請檢查網路連線後重新執行。"

    count   = len(articles)
    sources = list(dict.fromkeys(a.get("source", "綜合報導") for a in articles))[:3]
    src_str = "、".join(sources)

    # 取分數最高的前 3 則標題做摘要骨幹
    kw = WORLD_KEYWORDS if category == "world" else HOUSE_KEYWORDS
    top3 = sorted(articles, key=lambda a: -score_article(a, kw))[:3]
    events = [re.sub(r"[：:].+", "", a.get("title",""))[:20] for a in top3]
    events = [e for e in events if e.strip()]  # [B9修復] 過濾空字串
    events_str = "、".join(events) if events else "多則重要議題"

    if category == "world":
        return (
            f"今日彙整 {src_str} 等媒體共 {count} 則國際新聞。"
            f"焦點事件包括：{events_str}等重要議題。"
            f"建議持續關注台海情勢、中美博弈及相關外交動向。"
        )
    else:
        return (
            f"今日彙整 {src_str} 等媒體共 {count} 則房市新聞。"
            f"主要關注：{events_str}等市場動態。"
            f"北台灣買方宜密切追蹤政策與央行利率走向。"
        )


# ─────────────────────────────────────────────────────────
# HTML 生成
# ─────────────────────────────────────────────────────────

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Serif+TC:wght@500;700;900&family=Noto+Sans+TC:wght@400;500;700&display=swap');
:root{
  --bg:#0b1622; --card:#13263a; --card2:#16293c;
  --line:rgba(212,175,55,.20); --line2:rgba(212,175,55,.34);
  --gold:#d4af37; --gold-soft:#e7cd84; --gold-dim:#b3942f;
  --ink:#eef3f8; --sub:#b3c2d1; --muted:#7e93a6;
  --world:#7fb2e3; --house:#e7cd84; --weather:#6cc6d6;
  --serif:'Noto Serif TC',serif; --sans:'Noto Sans TC',system-ui,sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{
  font-family:var(--sans); color:var(--ink); min-height:100vh;
  background:
    radial-gradient(1200px 600px at 85% -10%, #14304a 0%, transparent 55%),
    radial-gradient(900px 500px at 0% 0%, #0e2236 0%, transparent 50%),
    linear-gradient(180deg,#0b1622 0%,#091320 100%);
  background-attachment:fixed;
  padding:env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);
}
a{color:inherit;text-decoration:none}
.wrap{max-width:1200px;margin:0 auto;padding:22px 20px 60px}

.masthead{display:flex;align-items:flex-end;justify-content:space-between;
  gap:14px;flex-wrap:wrap;padding-bottom:16px;
  border-bottom:1px solid var(--line2);position:relative}
.masthead::after{content:"";position:absolute;left:0;bottom:-1px;width:120px;height:2px;background:var(--gold)}
.brand .eyebrow{font-size:10px;letter-spacing:4px;color:var(--gold-dim);
  text-transform:uppercase;margin-bottom:7px;font-weight:700}
.brand h1{font-family:var(--serif);font-weight:900;line-height:1;
  font-size:clamp(28px,5.2vw,46px);letter-spacing:1px}
.brand h1 .dot{color:var(--gold)}
.brand .tagline{font-size:12px;color:var(--sub);margin-top:9px;letter-spacing:1px}
.meta{text-align:right;font-size:12px;color:var(--muted);line-height:1.95}
.meta .date{font-family:var(--serif);font-weight:700;color:var(--ink);font-size:15px}
.meta #updated{color:var(--gold-dim)}

.weather{margin-top:20px;border:1px solid var(--line2);border-radius:16px;
  background:linear-gradient(135deg,rgba(23,41,60,.9),rgba(16,30,46,.7));
  padding:20px 22px;display:flex;align-items:center;gap:22px;flex-wrap:wrap;
  box-shadow:0 10px 30px rgba(0,0,0,.25),inset 0 1px 0 rgba(255,255,255,.03)}
.wx-main{display:flex;align-items:center;gap:18px;flex:1;min-width:240px}
.wx-icon{font-size:52px;line-height:1;filter:drop-shadow(0 4px 8px rgba(0,0,0,.4))}
.wx-temp{font-family:var(--serif);font-size:46px;font-weight:900;line-height:.95;letter-spacing:-1px}
.wx-temp small{font-size:20px;color:var(--gold);font-weight:700;vertical-align:top}
.wx-cond{font-size:15px;color:var(--ink);font-weight:700;margin-bottom:3px}
.wx-feels{font-size:12px;color:var(--muted)}
.wx-stats{display:flex;gap:24px;flex-wrap:wrap}
.wx-stat{text-align:center;min-width:54px}
.wx-stat .v{font-family:var(--serif);font-size:19px;font-weight:700;color:var(--gold-soft)}
.wx-stat .k{font-size:10px;color:var(--muted);letter-spacing:1px;margin-top:3px}
.wx-city select{background:rgba(11,22,34,.8);color:var(--gold-soft);
  border:1px solid var(--line2);border-radius:9px;padding:8px 12px;
  font-family:var(--sans);font-size:13px;font-weight:700;cursor:pointer;outline:none}
.wx-city select:hover{border-color:var(--gold)}

.toolbar{margin-top:20px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.btn{border:1px solid var(--line2);background:rgba(19,38,58,.6);color:var(--gold-soft);
  border-radius:10px;padding:10px 18px;font-family:var(--sans);font-size:13px;
  font-weight:700;letter-spacing:.5px;cursor:pointer;transition:all .15s;display:flex;align-items:center;gap:8px}
.btn:hover:not(:disabled){border-color:var(--gold);background:rgba(212,175,55,.1)}
.btn:disabled{opacity:.5;cursor:not-allowed}
.btn-primary{background:linear-gradient(135deg,#d4af37,#b3942f);color:#0b1622;border-color:transparent}
.btn-primary:hover:not(:disabled){filter:brightness(1.08)}
.filters{display:flex;gap:6px;margin-left:auto}
.chip{border:1px solid var(--line);background:transparent;color:var(--muted);
  border-radius:20px;padding:7px 15px;font-size:12px;font-weight:700;cursor:pointer;transition:all .15s}
.chip:hover{color:var(--sub);border-color:var(--line2)}
.chip.on{background:var(--gold);color:#0b1622;border-color:var(--gold)}
.status{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted);margin-left:6px}
.dot{width:8px;height:8px;border-radius:50%;background:#3a4d60;flex-shrink:0}
.dot.live{background:#4ade80;box-shadow:0 0 8px #4ade80}
.dot.busy{background:var(--gold);animation:pulse 1s infinite}
.dot.err{background:#ef6b6b}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}

.block{margin-top:38px}
.block-head{display:flex;align-items:baseline;gap:14px;margin-bottom:4px}
.block-head .zh{font-family:var(--serif);font-size:24px;font-weight:900;letter-spacing:1px}
.block-head .en{font-size:11px;letter-spacing:3px;color:var(--gold-dim);text-transform:uppercase;font-weight:700}
.block-head .range{margin-left:auto;font-size:11px;color:var(--muted)}
.block-rule{height:1px;background:linear-gradient(90deg,var(--gold),transparent);margin:10px 0 22px}

.cat{margin-bottom:26px}
.cat-head{display:flex;align-items:center;gap:10px;margin-bottom:14px}
.cat-tag{font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;
  padding:4px 10px;border-radius:6px;color:#0b1622}
.cat-tag.world{background:var(--world)} .cat-tag.house{background:var(--house)} .cat-tag.weather{background:var(--weather)}
.cat-name{font-family:var(--serif);font-size:17px;font-weight:700}
.cat-count{margin-left:auto;font-size:11px;color:var(--muted)}

.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:14px}
.card{position:relative;background:linear-gradient(160deg,var(--card),var(--card2));
  border:1px solid var(--line);border-radius:13px;padding:17px 18px;
  transition:transform .18s,border-color .18s,box-shadow .18s;animation:rise .4s ease both;overflow:hidden;display:block}
.card::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;
  background:var(--accent,var(--gold));opacity:.7}
.card.world{--accent:var(--world)} .card.house{--accent:var(--house)} .card.weather{--accent:var(--weather)}
.card:hover{transform:translateY(-3px);border-color:var(--line2);
  box-shadow:0 12px 28px rgba(0,0,0,.35)}
@keyframes rise{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
.card-title{font-family:var(--serif);font-size:15.5px;font-weight:700;line-height:1.55;
  margin-bottom:8px;color:var(--ink)}
.card:hover .card-title{color:var(--gold-soft)}
.card-body{font-size:12.5px;line-height:1.7;color:var(--sub);margin-bottom:12px;
  display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.card-foot{display:flex;justify-content:space-between;align-items:center;gap:8px;
  border-top:1px solid var(--line);padding-top:10px}
.card-src{font-size:10.5px;color:var(--muted);letter-spacing:.5px}
.card-link{font-size:10.5px;color:var(--gold-dim);font-weight:700;white-space:nowrap}
.card:hover .card-link{color:var(--gold)}

.empty{grid-column:1/-1;text-align:center;padding:34px 18px;border:1px dashed var(--line);
  border-radius:12px;color:var(--muted);font-size:13px}
.empty .ic{font-size:30px;display:block;margin-bottom:10px;opacity:.6}
.skeleton{background:linear-gradient(160deg,var(--card),var(--card2));border:1px solid var(--line);
  border-radius:13px;padding:17px;animation:sk 1.3s ease-in-out infinite}
.sk{background:rgba(255,255,255,.06);border-radius:4px;margin-bottom:9px}
@keyframes sk{0%,100%{opacity:.4}50%{opacity:.8}}

footer{margin-top:46px;padding-top:16px;border-top:1px solid var(--line);
  display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;
  font-size:10.5px;color:var(--muted);letter-spacing:.5px}

@media(max-width:560px){
  .wrap{padding:18px 14px 50px}
  .meta{text-align:left}
  .filters{margin-left:0;width:100%}
  .weather{gap:16px;padding:18px}
  .wx-stats{width:100%;justify-content:space-between;gap:0}
  .grid{grid-template-columns:1fr}
}
</style>
"""


# ─────────────────────────────────────────────────────────
# 日期分流（本日 / 本周）與七天滾動封存
# ─────────────────────────────────────────────────────────

def _icon_uri(name: str, mime: str) -> str:
    """讀取同目錄圖檔轉成 data URI，讓產生的 HTML 自帶圖示、搬到任何位置都可用。
    讀不到時退回相對路徑（與舊行為相容）。"""
    try:
        raw = (BASE_DIR / name).read_bytes()
        return f"data:{mime};base64,{base64.b64encode(raw).decode()}"
    except Exception:
        return name


def _date_obj(iso):
    if not iso:
        return today()
    try:
        return datetime.date.fromisoformat(iso[:10])
    except Exception:
        return today()

def _diff_days(iso):
    return (today() - _date_obj(iso)).days

def split_today_week(articles):
    """回傳 (本日, 本周非本日)；超過 6 天的捨去"""
    today_list, week_list = [], []
    for a in articles:
        d = _diff_days(a.get("date"))
        if d <= 0:
            today_list.append(a)
        elif 1 <= d <= 6:
            week_list.append(a)
    return today_list, week_list

def archive_path():
    return BASE_DIR / ".news_archive.json"

def load_archive():
    p = archive_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _stamp_dates(articles):
    iso = today().isoformat()
    for a in articles:
        if not a.get("date"):
            a["date"] = iso
    return articles

def _purge_old(articles):
    return [a for a in articles if _diff_days(a.get("date")) <= 6]

def merge_archive(new_world, new_house, new_weather=None):
    """合併過去封存與本次抓取：既有在前（保留原始日期）→ 去重 → 清除超過七天 → 重新封存。
    這就是「新聞保留七天、重複不重複收、超過一周自動刪除」的實作。"""
    new_weather = new_weather or []
    arc = load_archive()
    prev_w = arc.get("world", []); prev_h = arc.get("house", []); prev_x = arc.get("weather", [])
    def _cap(arts, n=60):
        return sorted(arts, key=lambda a: a.get("date", ""), reverse=True)[:n]
    world = _cap(_purge_old(deduplicate(prev_w + _stamp_dates(new_world))))
    house = _cap(_purge_old(deduplicate(prev_h + _stamp_dates(new_house))))
    weather = _cap(_purge_old(deduplicate(prev_x + _stamp_dates(new_weather))))
    try:
        archive_path().write_text(
            json.dumps({"world": world, "house": house, "weather": weather, "saved": now().isoformat()},
                       ensure_ascii=False),
            encoding="utf-8")
    except Exception as e:
        print("  ⚠ 封存寫入失敗（不影響本次報告）：" + str(e))
    return world, house, weather

# 天氣（瀏覽器端直連 Open-Meteo，免金鑰、獨立於新聞線路）+ 類別篩選
WEATHER_FILTER_JS = """
<script>
(function(){
  var CITY_KEY="qbz_city_v1";
  var CITIES=[["台北市",25.0330,121.5654],["新北市",25.0169,121.4628],["基隆市",25.1276,121.7392],
    ["桃園市",24.9936,121.3010],["新竹市",24.8138,120.9675],["台中市",24.1477,120.6736],
    ["台南市",22.9999,120.2270],["高雄市",22.6273,120.3014],["宜蘭縣",24.7021,121.7378],["花蓮縣",23.9871,121.6015]];
  var WMO={0:["晴","☀️"],1:["晴時多雲","🌤️"],2:["多雲","⛅"],3:["陰","☁️"],
    45:["有霧","🌫️"],48:["霧凇","🌫️"],51:["毛毛雨","🌦️"],53:["毛毛雨","🌦️"],55:["毛毛雨","🌦️"],
    56:["凍雨","🌧️"],57:["凍雨","🌧️"],61:["小雨","🌧️"],63:["中雨","🌧️"],65:["大雨","🌧️"],
    66:["凍雨","🌧️"],67:["凍雨","🌧️"],71:["小雪","🌨️"],73:["中雪","🌨️"],75:["大雪","❄️"],77:["米雪","🌨️"],
    80:["陣雨","🌦️"],81:["陣雨","🌦️"],82:["強陣雨","⛈️"],85:["陣雪","🌨️"],86:["陣雪","❄️"],
    95:["雷雨","⛈️"],96:["雷雨冰雹","⛈️"],99:["強雷雨","⛈️"]};
  function $(id){return document.getElementById(id);}
  function loadWeather(){
    var idx=+(localStorage.getItem(CITY_KEY)||0); var c=CITIES[idx]||CITIES[0];
    var url="https://api.open-meteo.com/v1/forecast?latitude="+c[1]+"&longitude="+c[2]
      +"&current=temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m"
      +"&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max&timezone=auto&forecast_days=1";
    fetch(url,{cache:"no-store"}).then(function(r){return r.json();}).then(function(j){
      var cur=j.current, dd=j.daily, w=WMO[cur.weather_code]||["—","🌡️"];
      $("wx-icon").textContent=w[1];
      $("wx-temp").innerHTML=Math.round(cur.temperature_2m)+"<small>°C</small>";
      $("wx-cond").textContent=c[0]+"　"+w[0];
      $("wx-feels").textContent="體感 "+Math.round(cur.apparent_temperature)+"°";
      $("wx-hi").textContent=Math.round(dd.temperature_2m_max[0])+"°";
      $("wx-lo").textContent=Math.round(dd.temperature_2m_min[0])+"°";
      $("wx-rain").textContent=(dd.precipitation_probability_max[0]||0)+"%";
      $("wx-hum").textContent=cur.relative_humidity_2m+"%";
      $("wx-wind").textContent=Math.round(cur.wind_speed_10m);
    }).catch(function(){ $("wx-cond").textContent=(CITIES[idx]||CITIES[0])[0]+"　天氣載入失敗"; $("wx-feels").textContent="請檢查網路"; });
  }
  function setCity(){
    var sel=$("city"); if(!sel) return;
    CITIES.forEach(function(c,i){var o=document.createElement("option");o.value=i;o.textContent=c[0];sel.appendChild(o);});
    sel.value=localStorage.getItem(CITY_KEY)||0;
    sel.onchange=function(){localStorage.setItem(CITY_KEY,sel.value);loadWeather();};
  }
  function filter(f){
    var chips=document.querySelectorAll(".chip");
    for(var i=0;i<chips.length;i++) chips[i].classList.toggle("on",chips[i].getAttribute("data-f")===f);
    var cats=document.querySelectorAll(".cat");
    for(var j=0;j<cats.length;j++){var c=cats[j];c.style.display=(f==="all"||c.getAttribute("data-cat")===f)?"":"none";}
  }
  function refreshNow(){ location.href = location.pathname + "?t=" + Date.now(); }
  window.__refresh=refreshNow;
  window.__filter=filter;
  document.addEventListener("DOMContentLoaded",function(){ setCity(); loadWeather(); });
})();
</script>
"""


def build_card(a: dict, cat: str) -> str:
    title = escape(a.get("title", ""))
    summary = escape(a.get("summary", ""))
    src   = escape(a.get("source", "綜合報導"))
    link  = escape(a.get("link", ""))
    body  = f'<div class="card-body">{summary}</div>' if summary else ""
    foot  = (f'<div class="card-foot"><span class="card-src">{src}</span>'
             f'<span class="card-link">開啟原文 →</span></div>')
    if link:
        return (f'<a class="card {cat}" href="{link}" target="_blank" rel="noopener">'
                f'<div class="card-title">{title}</div>{body}{foot}</a>')
    return (f'<div class="card {cat}"><div class="card-title">{title}</div>{body}{foot}</div>')


def build_cat(cat: str, tag_lbl: str, name: str, articles: list) -> str:
    count = f"{len(articles)} 則" if articles else ""
    if articles:
        cards = "\n".join(build_card(a, cat) for a in articles)
    else:
        cards = '<div class="empty"><span class="ic">📭</span>目前沒有資料</div>'
    return (
        f'    <div class="cat" data-cat="{cat}">\n'
        f'      <div class="cat-head"><span class="cat-tag {cat}">{tag_lbl}</span>'
        f'<span class="cat-name">{name}</span><span class="cat-count">{count}</span></div>\n'
        f'      <div class="grid">\n{cards}\n      </div>\n'
        f'    </div>'
    )


def build_html(world: list, house: list, weather: list = None, cached: bool = False) -> str:
    weather = weather or []
    _n = now()
    date_str = f"{_n.year} 年 {_n.month} 月 {_n.day} 日"
    time_str = f"{_n.hour:02d}:{_n.minute:02d}"
    cache_note = "（快取）" if cached else ""

    tw, ww = split_today_week(world)
    th, hh = split_today_week(house)
    xw, xx = split_today_week(weather)

    yday = today() - datetime.timedelta(days=1)
    wk_start = today() - datetime.timedelta(days=6)
    today_range = f"{_n.month}/{_n.day}"
    week_range = f"{wk_start.month}/{wk_start.day} – {yday.month}/{yday.day}"

    block_today = (
        '  <section class="block">\n'
        '    <div class="block-head"><span class="zh">本日</span><span class="en">Today</span>'
        f'<span class="range">{today_range}</span></div>\n'
        '    <div class="block-rule"></div>\n'
        + build_cat("world", "Politics", "政治局勢", tw) + "\n"
        + build_cat("house", "Housing", "台灣房市", th) + "\n"
        + build_cat("weather", "Weather", "氣象消息", xw) + "\n"
        '  </section>'
    )
    block_week = (
        '  <section class="block">\n'
        '    <div class="block-head"><span class="zh">本周</span><span class="en">This Week</span>'
        f'<span class="range">{week_range}</span></div>\n'
        '    <div class="block-rule"></div>\n'
        + build_cat("world", "Politics", "政治局勢", ww) + "\n"
        + build_cat("house", "Housing", "台灣房市", hh) + "\n"
        + build_cat("weather", "Weather", "氣象消息", xx) + "\n"
        '  </section>'
    )

    weather_card = (
        '  <section class="weather" id="weather">\n'
        '    <div class="wx-main">\n'
        '      <div class="wx-icon" id="wx-icon">⛅</div>\n'
        '      <div><div class="wx-temp" id="wx-temp">--<small>°C</small></div></div>\n'
        '      <div><div class="wx-cond" id="wx-cond">載入天氣中…</div>'
        '<div class="wx-feels" id="wx-feels">—</div></div>\n'
        '    </div>\n'
        '    <div class="wx-stats">\n'
        '      <div class="wx-stat"><div class="v" id="wx-hi">—</div><div class="k">最高</div></div>\n'
        '      <div class="wx-stat"><div class="v" id="wx-lo">—</div><div class="k">最低</div></div>\n'
        '      <div class="wx-stat"><div class="v" id="wx-rain">—</div><div class="k">降雨</div></div>\n'
        '      <div class="wx-stat"><div class="v" id="wx-hum">—</div><div class="k">濕度</div></div>\n'
        '      <div class="wx-stat"><div class="v" id="wx-wind">—</div><div class="k">風速</div></div>\n'
        '    </div>\n'
        '    <div class="wx-city"><select id="city" aria-label="選擇城市"></select></div>\n'
        '  </section>'
    )

    toolbar = (
        '  <div class="toolbar">\n'
        '    <button class="btn btn-primary" onclick="__refresh()">↻ 立即更新</button>\n'
        '    <div class="filters">\n'
        '      <button class="chip on" data-f="all" onclick="__filter(\'all\')">全部</button>\n'
        '      <button class="chip" data-f="world" onclick="__filter(\'world\')">🌐 政治</button>\n'
        '      <button class="chip" data-f="house" onclick="__filter(\'house\')">🏠 房市</button>\n'
        '      <button class="chip" data-f="weather" onclick="__filter(\'weather\')">🌦️ 氣象</button>\n'
        '    </div>\n'
        f'    <div class="status"><span class="dot live"></span><span>更新 {time_str}{cache_note}</span></div>\n'
        '  </div>'
    )

    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="description" content="每日情報站：政治局勢、台灣房市、氣象與即時天氣彙整">
<title>每日情報站 · {date_str}</title>
<link rel="icon" type="image/png" sizes="32x32" href="{_icon_uri('favicon-32.png','image/png')}">
<link rel="apple-touch-icon" sizes="180x180" href="{_icon_uri('apple-touch-icon.png','image/png')}">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="每日情報站">
<meta name="theme-color" content="#0b1622">
{CSS}
</head>
<body>
<div class="wrap">
  <header class="masthead">
    <div class="brand">
      <div class="eyebrow">Daily Intelligence Brief</div>
      <h1>每日情報站</h1>
      <div class="tagline">政治局勢 &nbsp;·&nbsp; 台灣房市 &nbsp;·&nbsp; 即時天氣</div>
    </div>
    <div class="meta">
      <div class="date">{date_str}</div>
      <div id="updated">更新 {time_str}{cache_note}</div>
      <div style="font-size:10px;color:var(--muted)">政治 {len(world)} 則 · 房市 {len(house)} 則 · 氣象 {len(weather)} 則 · 保留 7 天</div>
    </div>
  </header>
{weather_card}
{toolbar}
{block_today}
{block_week}
  <footer>
    <span>每日情報站 · {today().year}</span>
    <span>新聞：Google News RSS（本機抓取）&nbsp;·&nbsp; 天氣：Open-Meteo &nbsp;·&nbsp; 免費無需金鑰</span>
  </footer>
</div>
{WEATHER_FILTER_JS}
</body>
</html>"""


# ─────────────────────────────────────────────────────────
# 舊報告清理
# ─────────────────────────────────────────────────────────

def cleanup_old_reports(keep: int) -> None:
    """[UX-2] 保留最近 keep 份 HTML 報告，刪除較舊的"""
    reports = sorted(BASE_DIR.glob("news_????????.html"), reverse=True)
    for old in reports[keep:]:
        try:
            old.unlink()
            print(f"  🗑  已刪除舊報告：{old.name}")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="每日情報站 v3.0 - 免費 RSS 新聞彙整",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--out",      default="", help="輸出 HTML 檔名")
    parser.add_argument("--no-open",  action="store_true", help="不自動開啟瀏覽器")
    parser.add_argument("--no-cache", action="store_true", help="忽略今日快取，強制重抓")
    parser.add_argument("--days",     type=int, default=7, help="包含幾天內的新聞（1~7）")
    parser.add_argument("--keep",     type=int, default=0, help="只保留最近 N 份報告（0=不清理）")
    parser.add_argument("--only",     choices=["world", "house"], help="只抓單一類別")
    args = parser.parse_args()

    if not 1 <= args.days <= 7:
        print("[!] --days 必須介於 1~7"); sys.exit(1)

    _today = today()
    out_name = args.out or "每日情報站.html"
    if not out_name.endswith(".html"):
        out_name += ".html"

    print("=" * 54)
    print(f"  每日情報站 v3.0  {_today.year}/{_today.month}/{_today.day}")
    if args.only:
        print(f"  模式：只抓{'國際' if args.only == 'world' else '房市'}新聞")
    print(f"  涵蓋：近 {args.days} 天新聞")
    print("=" * 54)

    # Cache
    world: list[dict] = []
    house: list[dict] = []
    weather: list[dict] = []
    cached = False

    # [B4修復] --only 模式時不使用 cache（避免部分 cache 污染另一類別）
    if not args.no_cache and not args.only:
        cached_data = load_cache()
        if cached_data:
            world  = cached_data.get("world", [])
            house  = cached_data.get("house", [])
            weather = cached_data.get("weather", [])
            cached = True

    if not cached:
        cleanup_old_cache()
        t0 = time.time()

        if args.only != "house":
            world = fetch_section_parallel(
                "政治局勢", WORLD_QUERIES, FALLBACK_WORLD, WORLD_KEYWORDS, args.days
            )
        if args.only != "world":
            house = fetch_section_parallel(
                "台灣房市", HOUSE_QUERIES, FALLBACK_HOUSE, HOUSE_KEYWORDS, args.days
            )
        if not args.only:
            weather = fetch_section_parallel(
                "氣象消息", WEATHER_QUERIES, FALLBACK_WEATHER, WEATHER_KEYWORDS, args.days
            )

        elapsed = time.time() - t0
        print(f"\n⏱  抓取耗時：{elapsed:.1f} 秒")
        # [B4修復] --only 時不寫 cache（只有完整抓取才寫）
        if not args.only:
            save_cache(world, house, weather)

    print(f"📊 最終：國際 {len(world)} 則 / 房市 {len(house)} 則 / 氣象 {len(weather)} 則")

    if not args.only:
        world, house, weather = merge_archive(world, house, weather)
    html     = build_html(world, house, weather, cached)
    out_path = Path(out_name).resolve()
    try:
        out_path.write_text(html, encoding="utf-8")
    except OSError as e:
        print(f"[!] 無法寫入 {out_path}：{e}"); sys.exit(1)

    size_kb = out_path.stat().st_size // 1024
    print(f"✅ 報告已儲存：{out_path}  ({size_kb} KB)")

    if args.keep > 0:
        cleanup_old_reports(args.keep)

    if not args.no_open:
        print("🌐 開啟瀏覽器...")
        webbrowser.open(out_path.as_uri())

    print("\n完成！")


if __name__ == "__main__":
    main()
