import os
import re
import json
import queue
import threading
import requests
import yfinance as yf
import pytz
from datetime import datetime, date
from flask import Flask, Response, request, jsonify

app = Flask(__name__)

# =============================================================================
# 뀨의 AI 임무 통제실 MCP 서버 - HTML 자동 파싱 버전
# - 기존 Flask + SSE + JSON-RPC 구조 유지
# - 전역일: 2027년 7월 25일 고정
# - 브리핑 알람 시간: 환경변수 BRIEFING_ALARM_TIME_KST로 조정 가능
# - family_portfolio.html 또는 환경변수 PORTFOLIO_HTML_PATH의 HTML에서 pnlData 자동 파싱
# =============================================================================

KST = pytz.timezone("Asia/Seoul")

ENLIST_DATE = date(2026, 1, 12)
DISCHARGE_DATE = date(2027, 7, 25)
BRIEFING_ALARM_TIME_KST = os.environ.get("BRIEFING_ALARM_TIME_KST", "07:30")

CHANGWON_LAT = 35.2279
CHANGWON_LON = 128.6811

PORTFOLIO_HTML_PATH = os.environ.get("PORTFOLIO_HTML_PATH", "family_portfolio.html")
PORTFOLIO_HTML_URL = os.environ.get("PORTFOLIO_HTML_URL", "").strip()

# yfinance가 바로 조회하기 어려운 국내 ETF/묶음/현금성 항목은 제외한다.
# 필요 시 여기에 예외 티커를 추가하면 된다.
NON_YFINANCE_KEYWORDS = [
    "KODEX", "TIGER", "RISE", "KOACT", "PLUS", "미국주식팔고달러보유",
    "소수점 포트폴리오", "현금", "폭락 대기 현금", "필라델피아반도체", "나스닥100",
]

DEFAULT_TICKERS = {
    "이현규": ["VRT", "OII", "BWXT", "TEM", "ALAB"],
    "임인숙": ["MSFT", "GOOGL", "NVDA", "UNH", "QQQ"],
    "이재현": ["GOOGL", "TSM", "MRVL", "NVDA", "MSFT", "AVGO", "RKLB"],
    "이재연": ["GOOGL", "TSM", "MSFT", "NVDA", "LLY", "MRVL", "TSLA"],
}

DEFAULT_RECORDS = []
for _owner, _tickers in DEFAULT_TICKERS.items():
    for _ticker in _tickers:
        DEFAULT_RECORDS.append({
            "ticker": _ticker,
            "member": _owner,
            "pnl": None,
            "amount": None,
            "cost": None,
            "cat": "unknown",
            "source": "fallback",
            "yf": True,
        })

MARKET_TICKERS = {
    "S&P500": "^GSPC",
    "나스닥": "^IXIC",
    "원달러환율": "USDKRW=X",
}

ACCOUNT_PROFILES = {
    "이현규": "전술·현금·기회포착형 포트폴리오",
    "임인숙": "코어 장기복리·은퇴축 포트폴리오",
    "이재현": "공격 성장 엔진 포트폴리오",
    "이재연": "성장 + 구조개선 포트폴리오",
}

SCHOOL_EVENTS = {
    "1학기 기말고사": date(2026, 7, 3),
    "여름방학식": date(2026, 7, 20),
    "학생부 마감일": date(2026, 8, 31),
    "2027학년도 대수능": date(2026, 11, 19),
}

FAMILY_EVENTS = {
    "이현규 생일": "12-10",
    "임인숙 생일": "11-20",
    "이재연 생일": "03-15",
    "이재현 생일": "03-12",
    "이재현 전역일": "2027-07-25",
}

client_queues = {}
client_lock = threading.Lock()
_cache = {}
_cache_lock = threading.Lock()


# =============================================================================
# 공통 유틸
# =============================================================================

def now_kst():
    return datetime.now(KST)


def today_kst():
    return now_kst().date()


def weekday_ko(dt):
    return ["월", "화", "수", "목", "금", "토", "일"][dt.weekday()]


def arrow_for_pct(pct):
    if pct is None:
        return "─"
    if pct > 0:
        return "▲"
    if pct < 0:
        return "▼"
    return "─"


def format_pct(pct, digits=1):
    if pct is None:
        return "데이터 없음"
    return f"{arrow_for_pct(pct)}{abs(pct):.{digits}f}%"


def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def fmt_amount_m(amount):
    value = safe_float(amount)
    if value is None:
        return "금액 없음"
    if value >= 10000:
        return f"{value/10000:.1f}억"
    if value == int(value):
        return f"{int(value):,}만"
    return f"{value:,.1f}만"


def get_cached(key, ttl_seconds, producer):
    now_ts = datetime.utcnow().timestamp()
    with _cache_lock:
        item = _cache.get(key)
        if item and now_ts - item["ts"] <= ttl_seconds:
            return item["value"], True, None
    try:
        value = producer()
        with _cache_lock:
            _cache[key] = {"value": value, "ts": now_ts}
        return value, False, None
    except Exception as exc:
        with _cache_lock:
            item = _cache.get(key)
            if item:
                return item["value"], True, exc
        raise


def yf_history(ticker, period="2d", ttl_seconds=300):
    def producer():
        return yf.Ticker(ticker).history(period=period)
    value, _, _ = get_cached(f"yf:{ticker}:{period}", ttl_seconds, producer)
    return value


def ticker_change(ticker, period="2d"):
    hist = yf_history(ticker, period=period, ttl_seconds=300)
    if hist is None or len(hist) < 2:
        return None
    start = safe_float(hist["Close"].iloc[0])
    prev = safe_float(hist["Close"].iloc[-2])
    end = safe_float(hist["Close"].iloc[-1])
    if start is None or prev is None or end is None or prev == 0 or start == 0:
        return None
    daily_pct = (end - prev) / prev * 100
    period_pct = (end - start) / start * 100
    high_drawdown_pct = None
    try:
        high = safe_float(hist["High"].max())
        if high:
            high_drawdown_pct = (end - high) / high * 100
    except Exception:
        pass
    return {
        "ticker": ticker,
        "start": start,
        "prev": prev,
        "end": end,
        "daily_pct": daily_pct,
        "period_pct": period_pct,
        "high_drawdown_pct": high_drawdown_pct,
    }


# =============================================================================
# HTML portfolio parsing
# =============================================================================

def looks_like_yfinance_ticker(ticker):
    if not ticker or not isinstance(ticker, str):
        return False
    ticker = ticker.strip()
    for keyword in NON_YFINANCE_KEYWORDS:
        if keyword in ticker:
            return False
    if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", ticker):
        return True
    return False


def find_portfolio_html_path():
    candidates = []
    if PORTFOLIO_HTML_PATH:
        candidates.append(PORTFOLIO_HTML_PATH)
    candidates.extend([
        "family_portfolio.html",
        "family_portfolio_20260523.html",
        "family_portfolio_20260523(3).html",
        os.path.join(os.path.dirname(__file__), "family_portfolio.html"),
        os.path.join(os.path.dirname(__file__), "family_portfolio_20260523.html"),
        os.path.join(os.path.dirname(__file__), "family_portfolio_20260523(3).html"),
    ])
    seen = set()
    for path in candidates:
        if not path or path in seen:
            continue
        seen.add(path)
        if os.path.exists(path):
            return path
    return None


def read_portfolio_html():
    if PORTFOLIO_HTML_URL:
        resp = requests.get(PORTFOLIO_HTML_URL, timeout=15)
        resp.raise_for_status()
        return resp.text, PORTFOLIO_HTML_URL

    path = find_portfolio_html_path()
    if not path:
        raise FileNotFoundError(
            "포트폴리오 HTML 파일을 찾지 못했습니다. family_portfolio.html로 저장하거나 PORTFOLIO_HTML_PATH를 지정하세요."
        )
    with open(path, "r", encoding="utf-8") as f:
        return f.read(), path


def extract_js_array(html, var_name):
    # const pnlData = [...]; 형태의 배열을 안전하게 추출한다.
    pattern = rf"(?:const|let|var)\s+{re.escape(var_name)}\s*=\s*(\[.*?\])\s*;"
    match = re.search(pattern, html, flags=re.DOTALL)
    if not match:
        raise ValueError(f"{var_name} 배열을 HTML에서 찾지 못했습니다.")
    raw = match.group(1)
    return json.loads(raw)


def load_portfolio_from_html():
    html, source = read_portfolio_html()
    pnl_data = extract_js_array(html, "pnlData")
    records = []
    for row in pnl_data:
        ticker = str(row.get("ticker", "")).strip()
        member = str(row.get("member", "")).strip()
        if not ticker or not member:
            continue
        amount = safe_float(row.get("amount"))
        cost = safe_float(row.get("cost"))
        pnl = safe_float(row.get("pnl"))
        record = {
            "ticker": ticker,
            "member": member,
            "pnl": pnl,
            "amount": amount,
            "cost": cost,
            "cat": row.get("cat", "unknown"),
            "source": "html",
            "yf": looks_like_yfinance_ticker(ticker),
        }
        records.append(record)

    if not records:
        raise ValueError("pnlData를 찾았지만 유효한 보유 데이터가 없습니다.")

    return {
        "source": source,
        "loaded_at": now_kst().strftime("%Y-%m-%d %H:%M:%S KST"),
        "records": records,
        "count": len(records),
        "status": "html",
    }


def build_fallback_portfolio(reason):
    return {
        "source": "DEFAULT_TICKERS fallback",
        "loaded_at": now_kst().strftime("%Y-%m-%d %H:%M:%S KST"),
        "records": DEFAULT_RECORDS,
        "count": len(DEFAULT_RECORDS),
        "status": "fallback",
        "error": str(reason),
    }


def initialize_portfolio_data():
    try:
        return load_portfolio_from_html()
    except Exception as exc:
        return build_fallback_portfolio(exc)


PORTFOLIO_DATA = initialize_portfolio_data()


def get_portfolio_records(owner="전체", yfinance_only=False):
    records = PORTFOLIO_DATA.get("records", [])
    if owner != "전체":
        records = [r for r in records if r.get("member") == owner]
    if yfinance_only:
        records = [r for r in records if r.get("yf")]
    return records


def get_dynamic_tickers(owner="전체"):
    records = get_portfolio_records(owner=owner, yfinance_only=True)
    grouped = {}
    for r in records:
        grouped.setdefault(r["member"], [])
        if r["ticker"] not in grouped[r["member"]]:
            grouped[r["member"]].append(r["ticker"])
    if owner == "전체":
        return grouped
    return {owner: grouped.get(owner, [])}


def all_unique_tickers():
    seen = []
    for record in get_portfolio_records(yfinance_only=True):
        ticker = record["ticker"]
        if ticker not in seen:
            seen.append(ticker)
    return seen


def get_portfolio_summary_by_owner(owner="전체"):
    records = get_portfolio_records(owner=owner)
    summary = {}
    for r in records:
        member = r["member"]
        summary.setdefault(member, {"amount": 0.0, "cost": 0.0, "count": 0, "yf_count": 0})
        summary[member]["count"] += 1
        summary[member]["amount"] += safe_float(r.get("amount"), 0) or 0
        summary[member]["cost"] += safe_float(r.get("cost"), 0) or 0
        if r.get("yf"):
            summary[member]["yf_count"] += 1
    return summary


# =============================================================================
# MCP tool functions
# =============================================================================

def get_portfolio(owner="전체"):
    records = get_portfolio_records(owner=owner)
    if owner != "전체" and not records:
        return f"{owner}의 포트폴리오 정보가 없습니다. HTML 파일의 member 값을 확인하세요."

    grouped = {}
    for r in records:
        grouped.setdefault(r["member"], []).append(r)

    result = [f"[포트폴리오 현황 - HTML 기준]", f"데이터 원본: {PORTFOLIO_DATA.get('source')}"]
    if PORTFOLIO_DATA.get("status") == "fallback":
        result.append(f"주의: HTML 파싱 실패로 fallback 사용 중 ({PORTFOLIO_DATA.get('error')})")

    for member, rows in grouped.items():
        total = sum((safe_float(r.get("amount"), 0) or 0) for r in rows)
        yf_rows = [r for r in rows if r.get("yf")]
        non_yf_count = len(rows) - len(yf_rows)
        result.append("")
        result.append(f"[{member}] 총 {fmt_amount_m(total)} / 항목 {len(rows)}개 / 실시간조회 가능 {len(yf_rows)}개 / 수동·국내·현금 {non_yf_count}개")

        # 평가금액 기준 상위 12개만 표시해 너무 길어지는 것을 방지한다.
        for r in sorted(rows, key=lambda x: safe_float(x.get("amount"), 0) or 0, reverse=True)[:12]:
            ticker = r["ticker"]
            pnl = r.get("pnl")
            amount = fmt_amount_m(r.get("amount"))
            cat = r.get("cat", "unknown")
            source_tag = "실시간대상" if r.get("yf") else "수동/국내/현금"
            result.append(f"  {ticker}: {amount} / 수익률 {format_pct(pnl, 2)} / {cat} / {source_tag}")

    return "\n".join(result)


def get_portfolio_source_status():
    records = PORTFOLIO_DATA.get("records", [])
    yf_count = len([r for r in records if r.get("yf")])
    manual_count = len(records) - yf_count
    summary = get_portfolio_summary_by_owner()
    result = [
        "[포트폴리오 데이터 원본 상태]",
        f"상태: {PORTFOLIO_DATA.get('status')}",
        f"원본: {PORTFOLIO_DATA.get('source')}",
        f"로드 시각: {PORTFOLIO_DATA.get('loaded_at')}",
        f"전체 항목: {len(records)}개",
        f"yfinance 조회 가능 항목: {yf_count}개",
        f"수동/국내ETF/현금 항목: {manual_count}개",
    ]
    if PORTFOLIO_DATA.get("error"):
        result.append(f"오류: {PORTFOLIO_DATA.get('error')}")
    result.append("")
    result.append("[구성원별 평가액]")
    for member, data in summary.items():
        result.append(f"  {member}: {fmt_amount_m(data['amount'])} / 항목 {data['count']}개")
    return "\n".join(result)


def get_discharge_countdown():
    today = today_kst()
    total_days = (DISCHARGE_DATE - ENLIST_DATE).days
    served_days = (today - ENLIST_DATE).days
    dday = (DISCHARGE_DATE - today).days

    if dday < 0:
        return "이재현 전역 완료! 대한민국 해병대 병장 만기전역을 축하합니다."
    if dday == 0:
        return "이재현 오늘 전역! 당당한 사회 복귀를 환영합니다."

    progress = max(0.0, min(100.0, served_days / total_days * 100)) if total_days > 0 else 100.0
    months_left = max(1, round(dday / 30))
    if dday <= 7:
        msg = "거의 다 왔다!"
    elif dday <= 30:
        msg = "한 달 남음"
    else:
        msg = f"약 {months_left}개월 남음"

    return (
        f"이재현 전역 D-{dday} ({msg})\n"
        f"  복무 진행률: {progress:.1f}%\n"
        f"  전역일: 2027년 7월 25일"
    )


def get_changwon_weather():
    def producer():
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={CHANGWON_LAT}&longitude={CHANGWON_LON}"
            "&current=temperature_2m,precipitation_probability,weathercode,windspeed_10m"
            "&timezone=Asia%2FSeoul"
        )
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["current"]

    try:
        current, from_cache, error = get_cached("weather:changwon", 600, producer)
        weather_map = {
            0: "맑음", 1: "대체로 맑음", 2: "구름 조금", 3: "흐림",
            45: "안개", 48: "안개", 51: "이슬비", 53: "이슬비", 55: "이슬비",
            61: "비", 63: "비", 65: "강한 비", 71: "눈", 73: "눈", 75: "강한 눈",
            80: "소나기", 81: "소나기", 82: "강한 소나기", 95: "뇌우", 96: "뇌우", 99: "뇌우",
        }
        desc = weather_map.get(current.get("weathercode"), "알 수 없음")
        temp = current.get("temperature_2m")
        precip = current.get("precipitation_probability")
        wind = current.get("windspeed_10m")
        umbrella = " / 우산 챙기기" if safe_float(precip, 0) >= 40 else ""
        cache_note = " (캐시)" if from_cache else ""
        return f"창원 날씨{cache_note}: {desc} {temp}°C / 강수확률 {precip}% / 풍속 {wind}km/h{umbrella}"
    except Exception as exc:
        return f"날씨 조회 실패 (원인: {exc})"


def get_market_status():
    result = ["[실시간 글로벌 금융 지표]"]
    for name, ticker in MARKET_TICKERS.items():
        try:
            change = ticker_change(ticker, period="2d")
            if not change:
                result.append(f"  {name}: 데이터 없음")
                continue
            if "환율" in name:
                result.append(f"  {name}: {change['end']:.2f}원 ({format_pct(change['daily_pct'], 2)})")
            else:
                result.append(f"  {name}: {format_pct(change['daily_pct'], 2)} ({change['end']:.1f}pt)")
        except Exception:
            result.append(f"  {name}: 조회 실패")
    return "\n".join(result)


def get_weekly_performance(owner="이현규"):
    tickers = get_dynamic_tickers(owner).get(owner, [])
    if not tickers:
        return f"{owner}의 yfinance 조회 가능 종목이 없습니다. 국내 ETF/현금 항목은 HTML 평가액 기준으로만 표시됩니다."

    result = [f"[{owner} 계좌 주간 수익률 - yfinance 조회 가능 종목]"]
    for ticker in tickers:
        try:
            change = ticker_change(ticker, period="7d")
            if change:
                result.append(f"  {ticker}: {format_pct(change['period_pct'], 1)} (${change['end']:.2f})")
            else:
                result.append(f"  {ticker}: 데이터 없음")
        except Exception:
            result.append(f"  {ticker}: 조회 실패")
    return "\n".join(result)


def get_today_info():
    now = now_kst()
    return "\n".join([
        f"{now.strftime('%Y년 %m월 %d일')} {weekday_ko(now)}요일",
        get_discharge_countdown(),
        get_changwon_weather(),
        get_market_status(),
        get_portfolio_source_status(),
    ])


def get_drop_alert():
    alerts = []
    for ticker in all_unique_tickers():
        try:
            daily = ticker_change(ticker, period="2d")
            weekly = ticker_change(ticker, period="7d")
            if not daily:
                continue
            daily_pct = daily["daily_pct"]
            weekly_pct = weekly["period_pct"] if weekly else None
            drawdown = daily.get("high_drawdown_pct")
            labels = []
            if daily_pct <= -5:
                labels.append("당일 -5% 이하 주의")
            elif daily_pct <= -3:
                labels.append("당일 -3% 이하 관심")
            if weekly_pct is not None and weekly_pct <= -10:
                labels.append("주간 -10% 이하")
            if drawdown is not None and drawdown <= -15:
                labels.append("단기 고점 대비 -15% 이하")
            if labels:
                alerts.append((daily_pct, f"  {ticker}: 오늘 {daily_pct:.1f}% / 주간 {weekly_pct:.1f}% / {'; '.join(labels)}"))
        except Exception:
            pass

    if not alerts:
        return "[급락 감지]\n  오늘 주요 yfinance 조회 가능 종목 중 급락 신호가 없습니다."
    alerts.sort(key=lambda x: x[0])
    return "[급락 감지]\n" + "\n".join(x[1] for x in alerts) + "\n\n  판단: 매수 지시가 아니라 추가매수 검토 후보입니다. 섹터 악재와 개별 악재를 구분하세요."


def get_buy_candidates():
    candidates = []
    records_by_ticker = {}
    for r in get_portfolio_records(yfinance_only=True):
        records_by_ticker.setdefault(r["ticker"], []).append(r)

    for ticker in all_unique_tickers():
        try:
            daily = ticker_change(ticker, period="2d")
            weekly = ticker_change(ticker, period="7d")
            twenty = ticker_change(ticker, period="20d")
            if not daily:
                continue
            score = 0
            reasons = []
            if daily["daily_pct"] <= -3:
                score += 1
                reasons.append(f"당일 {daily['daily_pct']:.1f}%")
            if weekly and weekly["period_pct"] <= -5:
                score += 1
                reasons.append(f"주간 {weekly['period_pct']:.1f}%")
            if twenty and twenty.get("high_drawdown_pct") is not None and twenty["high_drawdown_pct"] <= -10:
                score += 1
                reasons.append(f"20일 고점 대비 {twenty['high_drawdown_pct']:.1f}%")
            if score:
                owners = sorted({r["member"] for r in records_by_ticker.get(ticker, [])})
                amount = sum((safe_float(r.get("amount"), 0) or 0) for r in records_by_ticker.get(ticker, []))
                candidates.append((score, daily["daily_pct"], ticker, owners, amount, reasons))
        except Exception:
            pass

    if not candidates:
        return "[추가매수 검토 후보]\n  현재 조건에 맞는 후보가 없습니다. 관망 우선입니다."

    candidates.sort(key=lambda x: (-x[0], x[1]))
    result = ["[추가매수 검토 후보]", "매수 지시가 아니라 분할 접근 가능성 점검 목록입니다."]
    for idx, (_, daily_pct, ticker, owners, amount, reasons) in enumerate(candidates[:10], start=1):
        result.append(f"{idx}. {ticker}: {fmt_amount_m(amount)} / 보유자 {' / '.join(owners)} / {', '.join(reasons)}")
    return "\n".join(result)


def get_overlap_analysis():
    ticker_owners = {}
    ticker_amounts = {}
    total_amount = 0.0
    for r in get_portfolio_records():
        ticker = r["ticker"]
        member = r["member"]
        amount = safe_float(r.get("amount"), 0) or 0
        total_amount += amount
        ticker_owners.setdefault(ticker, set()).add(member)
        ticker_amounts[ticker] = ticker_amounts.get(ticker, 0.0) + amount

    duplicates = [(t, sorted(owners), ticker_amounts.get(t, 0.0)) for t, owners in ticker_owners.items() if len(owners) >= 2]
    duplicates.sort(key=lambda x: x[2], reverse=True)
    if not duplicates:
        return "[가족 전체 중복 보유 분석]\n  중복 보유 종목 없음."

    result = ["[가족 전체 중복 보유 분석 - HTML 기준]"]
    for ticker, owners, amount in duplicates[:20]:
        pct = amount / total_amount * 100 if total_amount else 0
        result.append(f"  {ticker} ({len(owners)}개 계좌): {' / '.join(owners)} / {fmt_amount_m(amount)} / {pct:.1f}%")
    result.append("\n  해석: 추가매수 전 가족 전체 기준 중복 노출과 테마 집중도를 먼저 확인하세요.")
    return "\n".join(result)


def get_account_profile(owner="전체"):
    summary = get_portfolio_summary_by_owner(owner=owner)
    if owner != "전체" and owner not in summary:
        return f"{owner}의 계좌 정보가 없습니다."
    result = ["[가족 계좌 성격 진단 - HTML 기준]"]
    for member, data in summary.items():
        profile = ACCOUNT_PROFILES.get(member, "포트폴리오 성격 미정")
        result.append(f"  {member}: {profile} / {fmt_amount_m(data['amount'])} / 항목 {data['count']}개")
    return "\n".join(result)


def get_family_events():
    today = today_kst()
    year = today.year
    items = []
    for event, date_str in FAMILY_EVENTS.items():
        try:
            if len(date_str) == 5:
                m, d = map(int, date_str.split("-"))
                event_date = date(year, m, d)
                if event_date < today:
                    event_date = date(year + 1, m, d)
            else:
                y, m, d = map(int, date_str.split("-"))
                event_date = date(y, m, d)
            dday = (event_date - today).days
            if dday >= 0:
                items.append((dday, event, event_date))
        except Exception:
            pass
    items.sort()
    result = ["[가족 기념일 및 D-day]"]
    for dday, event, event_date in items[:8]:
        if dday == 0:
            result.append(f"  오늘: {event}!")
        else:
            result.append(f"  D-{dday:3d}: {event} ({event_date.strftime('%m/%d')})")
    return "\n".join(result)


def get_school_briefing():
    today = today_kst()
    result = ["[고3 진로진학 학사일정]"]
    for event, event_date in sorted(SCHOOL_EVENTS.items(), key=lambda x: x[1]):
        dday = (event_date - today).days
        if dday > 0:
            result.append(f"  D-{dday:3d}: {event} ({event_date.strftime('%m/%d')})")
        elif dday == 0:
            result.append(f"  오늘: {event}!")
        else:
            result.append(f"  완료: {event}")
    return "\n".join(result)


def get_alarm_info():
    return f"[브리핑 알람 설정]\n  기본 브리핑 알람 시간: 매일 {BRIEFING_ALARM_TIME_KST} KST\n  Render 환경변수 BRIEFING_ALARM_TIME_KST로 변경 가능"


def get_system_status():
    result = [f"[시스템 상태] {now_kst().strftime('%Y-%m-%d %H:%M')} KST"]
    result.append(f"  MCP 서버: 정상 (Flask SSE)")
    result.append(f"  브리핑 알람 기준: {BRIEFING_ALARM_TIME_KST} KST")
    result.append(f"  전역일: 2027-07-25")
    result.append(f"  포트폴리오 원본: {PORTFOLIO_DATA.get('source')}")
    result.append(f"  포트폴리오 상태: {PORTFOLIO_DATA.get('status')} / 항목 {PORTFOLIO_DATA.get('count')}개")
    try:
        test = yf.Ticker("AAPL").history(period="1d")
        result.append(f"  yfinance: {'정상' if len(test) > 0 else '데이터 없음'}")
    except Exception as exc:
        result.append(f"  yfinance: 오류 ({exc})")
    try:
        r = requests.get(
            f"https://api.open-meteo.com/v1/forecast?latitude={CHANGWON_LAT}&longitude={CHANGWON_LON}&current=temperature_2m",
            timeout=5,
        )
        r.raise_for_status()
        result.append("  Open-Meteo: 정상")
    except Exception as exc:
        result.append(f"  Open-Meteo: 오류 ({exc})")
    return "\n".join(result)


def get_kkyu_briefing():
    now = now_kst()
    header = (
        f"[뀨의 AI 임무 통제실 종합 브리핑]\n"
        f"{now.strftime('%Y년 %m월 %d일')} {weekday_ko(now)}요일 {now.strftime('%H:%M')} KST\n"
        f"브리핑 알람 기준: {BRIEFING_ALARM_TIME_KST} KST\n"
        + "=" * 44
    )
    sections = [
        ("1. 오늘 날씨", get_changwon_weather()),
        ("2. 이재현 복무 현황", get_discharge_countdown()),
        ("3. 포트폴리오 원본", get_portfolio_source_status()),
        ("4. 가족 기념일", get_family_events()),
        ("5. 학교 일정", get_school_briefing()),
        ("6. 글로벌 시장", get_market_status()),
        ("7. 이현규 계좌 현황", get_portfolio("이현규")),
        ("8. 이현규 계좌 주간", get_weekly_performance("이현규")),
        ("9. 급락 감지", get_drop_alert()),
        ("10. 추가매수 검토 후보", get_buy_candidates()),
    ]
    parts = [header]
    for title, content in sections:
        parts.append(f"\n[{title}]\n{content}")
    parts.append("\n" + "=" * 44)
    parts.append("오늘의 판단:\n  HTML 기준 실제 보유 종목을 먼저 확인하고, yfinance 조회 가능 종목만 실시간 변동을 참고하세요.\n  국내 ETF·현금·묶음형 항목은 HTML 평가금액 기준으로 관리하세요.")
    return "\n".join(parts)


# =============================================================================
# MCP tool schema
# =============================================================================

TOOLS = [
    {"name": "get_kkyu_briefing", "description": "HTML 포트폴리오 기준 날씨·전역·학교일정·시장·포트폴리오·급락·추가매수 후보를 종합 브리핑한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_portfolio", "description": "HTML 파일에서 파싱한 가족 포트폴리오 현황을 조회한다. owner는 이현규/임인숙/이재현/이재연/전체 중 선택.", "inputSchema": {"type": "object", "properties": {"owner": {"type": "string", "description": "계좌 소유자 이름 또는 전체"}}}},
    {"name": "get_portfolio_source_status", "description": "서버가 어떤 HTML/URL/fallback에서 포트폴리오를 읽었는지 확인한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_weekly_performance", "description": "HTML 기준 보유 종목 중 yfinance 조회 가능 종목의 최근 7일 수익률을 조회한다.", "inputSchema": {"type": "object", "properties": {"owner": {"type": "string", "description": "계좌 소유자 이름"}}}},
    {"name": "get_market_status", "description": "S&P500·나스닥 지수 등락률과 원달러 환율을 조회한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_drop_alert", "description": "HTML 기준 보유 종목 중 yfinance 조회 가능 종목의 급락 신호를 감지한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_buy_candidates", "description": "HTML 기준 보유 종목 중 추가매수 검토 후보를 추출한다. 매수 지시가 아닌 검토 후보다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_overlap_analysis", "description": "HTML 기준 가족 전체 중복 보유 종목과 평가액 집중도를 분석한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_account_profile", "description": "HTML 기준 계좌별 투자 성격과 평가액을 요약한다.", "inputSchema": {"type": "object", "properties": {"owner": {"type": "string", "description": "계좌 소유자 이름 또는 전체"}}}},
    {"name": "get_discharge_countdown", "description": "이재현 해병대 전역 D-day와 복무 진행률을 계산한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_changwon_weather", "description": "창원 현재 날씨를 조회한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_today_info", "description": "오늘 날짜·날씨·전역·시장·포트폴리오 원본 상태를 반환한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_family_events", "description": "가족 생일·전역일 등 D-day를 조회한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_school_briefing", "description": "창원경일고 고3 진로진학 학사일정 D-day를 조회한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_alarm_info", "description": "브리핑 알람 시간 설정을 확인한다.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "get_system_status", "description": "MCP 서버·포트폴리오 HTML 파싱·외부 API 상태를 점검한다.", "inputSchema": {"type": "object", "properties": {}}},
]


# =============================================================================
# JSON-RPC handler
# =============================================================================

def handle_jsonrpc(data):
    if not isinstance(data, dict):
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}}

    method = data.get("method", "")
    req_id = data.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "뀨의 AI 임무 통제실", "version": "3.0.0-html-auto"},
            },
        }
    if method == "notifications/initialized":
        return None
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
    if method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}
    if method == "tools/call":
        tool_name = data.get("params", {}).get("name", "")
        arguments = data.get("params", {}).get("arguments", {}) or {}
        dispatch = {
            "get_kkyu_briefing": lambda: get_kkyu_briefing(),
            "get_portfolio": lambda: get_portfolio(arguments.get("owner", "전체")),
            "get_portfolio_source_status": lambda: get_portfolio_source_status(),
            "get_weekly_performance": lambda: get_weekly_performance(arguments.get("owner", "이현규")),
            "get_market_status": lambda: get_market_status(),
            "get_drop_alert": lambda: get_drop_alert(),
            "get_buy_candidates": lambda: get_buy_candidates(),
            "get_overlap_analysis": lambda: get_overlap_analysis(),
            "get_account_profile": lambda: get_account_profile(arguments.get("owner", "전체")),
            "get_discharge_countdown": lambda: get_discharge_countdown(),
            "get_changwon_weather": lambda: get_changwon_weather(),
            "get_today_info": lambda: get_today_info(),
            "get_family_events": lambda: get_family_events(),
            "get_school_briefing": lambda: get_school_briefing(),
            "get_alarm_info": lambda: get_alarm_info(),
            "get_system_status": lambda: get_system_status(),
        }
        try:
            fn = dispatch.get(tool_name)
            result = fn() if fn else f"알 수 없는 도구: {tool_name}"
        except Exception as exc:
            result = f"도구 실행 중 오류: {tool_name} / {exc}"
        return {"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "text", "text": result}]}}

    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": "Method not found"}}


# =============================================================================
# Flask routes
# =============================================================================

@app.route("/")
def index():
    return jsonify({
        "status": "ok",
        "name": "뀨의 AI 임무 통제실 MCP 서버",
        "version": "3.0.0-html-auto",
        "description": "HTML pnlData 자동 파싱 기반 개인 임무 통제실",
        "alarm_time_kst": BRIEFING_ALARM_TIME_KST,
        "discharge_date": DISCHARGE_DATE.isoformat(),
        "portfolio": {
            "status": PORTFOLIO_DATA.get("status"),
            "source": PORTFOLIO_DATA.get("source"),
            "loaded_at": PORTFOLIO_DATA.get("loaded_at"),
            "count": PORTFOLIO_DATA.get("count"),
        },
        "endpoints": {"sse": "/sse", "message": "/message"},
        "tools": [t["name"] for t in TOOLS],
    })


@app.route("/sse", methods=["GET", "POST"])
def sse():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        response = handle_jsonrpc(data)
        if response:
            return jsonify(response)
        return jsonify({"status": "ok"}), 202

    client_id = id(request)
    base_url = request.url_root.rstrip("/")
    q = queue.Queue()
    with client_lock:
        client_queues[client_id] = q

    def generate():
        yield f"event: endpoint\ndata: {base_url}/message?client_id={client_id}\n\n"
        try:
            while True:
                try:
                    msg = q.get(timeout=30)
                    if msg is None:
                        break
                    yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    yield ": ping\n\n"
        finally:
            with client_lock:
                client_queues.pop(client_id, None)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Access-Control-Allow-Origin": "*"},
    )


@app.route("/message", methods=["POST"])
def message():
    client_id = int(request.args.get("client_id", 0))
    data = request.get_json(silent=True) or {}
    response = handle_jsonrpc(data)

    delivered = False
    if response is not None:
        with client_lock:
            q = client_queues.get(client_id)
            if q:
                q.put(response)
                delivered = True

    if delivered:
        return jsonify({"status": "accepted"}), 202
    # fallback: SSE 큐가 없거나 클라이언트가 직접 POST 응답을 기대하는 경우에도 JSON-RPC 응답 반환
    if response is not None:
        return jsonify(response), 200
    return jsonify({"status": "ok"}), 202


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, threaded=True)
