import os
import json
import queue
import threading
import requests
import yfinance as yf
import pytz
from datetime import datetime, date, timedelta
from flask import Flask, Response, request, jsonify

app = Flask(__name__)

# =============================================================================
# 뀨의 AI 임무 통제실 MCP 서버
# - 기존 Flask + SSE + JSON-RPC 구조 유지
# - 전역일: 2027년 7월 25일 고정
# - 브리핑 알람 시간: 환경변수 BRIEFING_ALARM_TIME_KST로 조정 가능
# =============================================================================

KST = pytz.timezone("Asia/Seoul")

# [중요] 실제 전역일 기준
ENLIST_DATE = date(2026, 1, 12)
DISCHARGE_DATE = date(2027, 7, 25)

# [중요] 알람 시간
# 첨부 코드에는 알람 시간이 직접 들어 있지 않아 기본값을 07:30으로 두었습니다.
# Render 환경변수에 BRIEFING_ALARM_TIME_KST=07:00 처럼 넣으면 즉시 변경됩니다.
BRIEFING_ALARM_TIME_KST = os.environ.get("BRIEFING_ALARM_TIME_KST", "07:30")

CHANGWON_LAT = 35.2279
CHANGWON_LON = 128.6811

TICKERS = {
    "이현규": ["VRT", "OII", "BWXT", "TEM", "ALAB"],
    "임인숙": ["MSFT", "GOOGL", "NVDA", "UNH", "QQQ"],
    "이재현": ["GOOGL", "TSM", "MRVL", "NVDA", "MSFT", "AVGO", "RKLB"],
    "이재연": ["GOOGL", "TSM", "MSFT", "NVDA", "LLY", "MRVL", "TSLA"],
}

MARKET_TICKERS = {
    "S&P500": "^GSPC",
    "나스닥": "^IXIC",
    "원달러환율": "USDKRW=X",
}

ACCOUNT_PROFILES = {
    "이현규": "공격형 AI 인프라·원전·에너지 성장 포트폴리오",
    "임인숙": "안정형 빅테크·ETF 중심 포트폴리오",
    "이재현": "반도체·우주항공·AI 고성장 포트폴리오",
    "이재연": "빅테크·반도체·헬스케어 혼합 성장 포트폴리오",
}

# 필요 시 날짜만 수정하면 됩니다.
SCHOOL_EVENTS = {
    "1학기 기말고사": date(2026, 7, 3),
    "여름방학식": date(2026, 7, 20),
    "학생부 마감일": date(2026, 8, 31),
    "2027학년도 대수능": date(2026, 11, 19),
}

# 월-일 반복 기념일과 특정 날짜 기념일을 함께 지원합니다.
FAMILY_EVENTS = {
    "이현규 생일": "12-10",
    "임인숙 생일": "11-20",
    "이재연 생일": "03-15",
    "이재현 생일": "03-12",
    "이재현 전역일": "2027-07-25",
}

# 클라이언트별 큐 저장소
client_queues = {}
client_lock = threading.Lock()

# 간단한 메모리 캐시: Render 무료 플랜과 yfinance 과호출 방지
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
    days = ["월", "화", "수", "목", "금", "토", "일"]
    return days[dt.weekday()]


def arrow_for_pct(pct):
    if pct > 0:
        return "▲"
    if pct < 0:
        return "▼"
    return "─"


def format_pct(pct, digits=1):
    arrow = arrow_for_pct(pct)
    return f"{arrow}{abs(pct):.{digits}f}%"


def months_left_from_days(days):
    if days <= 0:
        return 0
    return max(1, round(days / 30))


def get_cached(key, ttl_seconds, producer):
    """
    producer()를 실행해 값을 만들고 ttl_seconds 동안 재사용합니다.
    외부 API 실패 시 만료된 직전 캐시라도 있으면 fallback으로 반환합니다.
    """
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
    key = f"yf:{ticker}:{period}"

    def producer():
        t = yf.Ticker(ticker)
        hist = t.history(period=period)
        return hist

    value, from_cache, error = get_cached(key, ttl_seconds, producer)
    return value


def safe_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


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

    high_20 = None
    high_drawdown_pct = None
    if len(hist) >= 2:
        try:
            high_20 = safe_float(hist["High"].max())
            if high_20 and high_20 != 0:
                high_drawdown_pct = (end - high_20) / high_20 * 100
        except Exception:
            pass

    return {
        "ticker": ticker,
        "start": start,
        "prev": prev,
        "end": end,
        "daily_pct": daily_pct,
        "period_pct": period_pct,
        "high_20": high_20,
        "high_drawdown_pct": high_drawdown_pct,
    }


def all_unique_tickers():
    seen = []
    for tickers in TICKERS.values():
        for ticker in tickers:
            if ticker not in seen:
                seen.append(ticker)
    return seen


# =============================================================================
# 핵심 도구 함수
# =============================================================================

def get_portfolio(owner="전체"):
    """가족 포트폴리오 주식 데이터를 조회한다."""
    if owner == "전체":
        targets = TICKERS
    elif owner in TICKERS:
        targets = {owner: TICKERS[owner]}
    else:
        return f"{owner}의 계좌 정보가 없습니다. 이현규/임인숙/이재현/이재연/전체 중 선택하세요."

    result = []
    for name, tickers in targets.items():
        result.append(f"[{name} 계좌]")
        for ticker in tickers:
            try:
                change = ticker_change(ticker, period="2d")
                if change:
                    result.append(
                        f"  {ticker}: {format_pct(change['daily_pct'], 1)} (${change['end']:.2f})"
                    )
                else:
                    result.append(f"  {ticker}: 데이터 없음")
            except Exception:
                result.append(f"  {ticker}: 조회 실패")
        result.append("")
    return "\n".join(result).strip()


def get_discharge_countdown():
    """이재현의 해병대 전역 D-day, 복무 진행률, 남은 개월 수를 계산한다."""
    today = today_kst()
    dday = (DISCHARGE_DATE - today).days
    total_days = (DISCHARGE_DATE - ENLIST_DATE).days
    served_days = (today - ENLIST_DATE).days
    progress = (served_days / total_days * 100) if total_days > 0 else 100
    progress = max(0.0, min(100.0, progress))
    remain_months = months_left_from_days(dday)

    if dday < 0:
        return "🎖️ 이재현 전역 완료! 대한민국 해병대 병장 만기전역을 축하합니다."
    if dday == 0:
        return "🎉 이재현 오늘 전역! 당당한 사회 복귀를 환영합니다."

    if dday <= 7:
        mood = "거의 다 왔다!"
    elif dday <= 30:
        mood = "한 달 이내"
    elif dday <= 100:
        mood = f"약 {remain_months}개월 남음"
    else:
        mood = f"약 {remain_months}개월 남음"

    return (
        "[이재현 전역 현황]\n"
        f"  🪖 전역 D-{dday} ({mood})\n"
        f"  복무 진행률: {progress:.1f}%\n"
        f"  남은 기간: 약 {remain_months}개월\n"
        "  전역일: 2027년 7월 25일"
    )


def get_changwon_weather():
    """창원 현재 날씨를 조회한다."""
    def producer():
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={CHANGWON_LAT}&longitude={CHANGWON_LON}"
            "&current=temperature_2m,precipitation_probability,weathercode,windspeed_10m"
            "&timezone=Asia%2FSeoul"
        )
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()

    try:
        data, from_cache, error = get_cached("weather:changwon:current", 600, producer)
        current = data["current"]
        temp = current.get("temperature_2m")
        precip = current.get("precipitation_probability")
        wind = current.get("windspeed_10m")
        code = current.get("weathercode")

        weather_map = {
            0: "맑음", 1: "대체로 맑음", 2: "구름 조금", 3: "흐림",
            45: "안개", 48: "안개",
            51: "이슬비", 53: "이슬비", 55: "이슬비",
            61: "비", 63: "비", 65: "강한 비",
            71: "눈", 73: "눈", 75: "강한 눈",
            80: "소나기", 81: "소나기", 82: "강한 소나기",
            95: "뇌우", 96: "뇌우", 99: "뇌우",
        }
        desc = weather_map.get(code, "알 수 없음")

        umbrella = "필요" if (precip is not None and precip >= 50) or code in [51, 53, 55, 61, 63, 65, 80, 81, 82, 95, 96, 99] else "선택"
        suffix = " (캐시 기준)" if from_cache else ""
        return (
            f"🌦️ 창원 현재 날씨{suffix}: {desc} {temp}°C / "
            f"강수확률 {precip}% / 풍속 {wind}km/h / 우산: {umbrella}"
        )
    except Exception as exc:
        return f"❌ 창원 날씨 조회 실패: {str(exc)}"


def get_market_status():
    """S&P500, 나스닥, 원달러환율을 조회한다."""
    result = ["[실시간 글로벌 금융 지표]"]

    for name, ticker in MARKET_TICKERS.items():
        try:
            change = ticker_change(ticker, period="2d")
            if not change:
                result.append(f"  {name}: 데이터 부족")
                continue

            if "환율" in name:
                result.append(
                    f"  💵 {name}: {change['end']:.2f}원 ({format_pct(change['daily_pct'], 2)})"
                )
            else:
                result.append(
                    f"  📈 {name}: {format_pct(change['daily_pct'], 2)} ({change['end']:.1f}pt)"
                )
        except Exception:
            result.append(f"  {name}: 조회 실패")

    return "\n".join(result)


def get_weekly_performance(owner="이현규"):
    """특정 계좌의 최근 7일 수익률을 조회한다."""
    if owner == "전체":
        return "\n\n".join(get_weekly_performance(name) for name in TICKERS)

    if owner not in TICKERS:
        return f"{owner}의 계좌 정보가 없습니다. 이현규/임인숙/이재현/이재연/전체 중 선택하세요."

    result = [f"[{owner} 계좌 주간 수익률]"]
    for ticker in TICKERS[owner]:
        try:
            change = ticker_change(ticker, period="7d")
            if change:
                result.append(
                    f"  {ticker}: {format_pct(change['period_pct'], 1)} (${change['end']:.2f})"
                )
            else:
                result.append(f"  {ticker}: 데이터 부족")
        except Exception:
            result.append(f"  {ticker}: 조회 실패")
    return "\n".join(result)


def get_drop_alert(threshold=-3.0):
    """
    보유 종목 중 급락/조정 종목을 감지한다.
    - 당일 threshold 이하: 관심
    - 당일 -5% 이하 또는 주간 -10% 이하: 주의
    """
    try:
        threshold = float(threshold)
    except Exception:
        threshold = -3.0

    interest = []
    caution = []
    checked = set()

    for ticker in all_unique_tickers():
        if ticker in checked:
            continue
        checked.add(ticker)

        try:
            daily = ticker_change(ticker, period="2d")
            weekly = ticker_change(ticker, period="7d")
            if not daily:
                continue

            daily_pct = daily["daily_pct"]
            weekly_pct = weekly["period_pct"] if weekly else None
            price = daily["end"]

            owners = [owner for owner, tickers in TICKERS.items() if ticker in tickers]
            owners_text = ", ".join(owners)

            if daily_pct <= -5 or (weekly_pct is not None and weekly_pct <= -10):
                msg = f"  ⚠️ {ticker}: 오늘 {daily_pct:.1f}%"
                if weekly_pct is not None:
                    msg += f" / 주간 {weekly_pct:.1f}%"
                msg += f" (${price:.2f}) — 보유: {owners_text} → 리스크 점검 및 분할 접근 검토"
                caution.append(msg)
            elif daily_pct <= threshold:
                msg = f"  ◦ {ticker}: 오늘 {daily_pct:.1f}% (${price:.2f}) — 보유: {owners_text} → 추가매수 검토 후보"
                interest.append(msg)
        except Exception:
            continue

    lines = ["[급락 감지 결과]"]
    if not interest and not caution:
        lines.append(f"  ✅ 오늘 {threshold:.1f}% 이하 급락한 포트폴리오 종목이 없습니다.")
    if caution:
        lines.append("\n[주의]")
        lines.extend(caution)
    if interest:
        lines.append("\n[관심]")
        lines.extend(interest)

    lines.append("\n판단 원칙: 매수 지시가 아니라 '검토 후보'입니다. 분할 접근과 리스크 점검을 우선합니다.")
    return "\n".join(lines)


def get_buy_candidates(owner="전체"):
    """
    추가매수 검토 후보를 추린다.
    조건: 당일 -3% 이하, 주간 -5% 이하, 20일 고점 대비 -10% 이하 중 하나 이상.
    """
    if owner == "전체":
        target_tickers = all_unique_tickers()
    elif owner in TICKERS:
        target_tickers = TICKERS[owner]
    else:
        return f"{owner}의 계좌 정보가 없습니다. 이현규/임인숙/이재현/이재연/전체 중 선택하세요."

    candidates = []

    for ticker in target_tickers:
        try:
            daily = ticker_change(ticker, period="2d")
            weekly = ticker_change(ticker, period="7d")
            high20 = ticker_change(ticker, period="1mo")
            if not daily:
                continue

            reasons = []
            score = 0

            if daily["daily_pct"] <= -3:
                reasons.append(f"당일 {daily['daily_pct']:.1f}%")
                score += 1

            if weekly and weekly["period_pct"] <= -5:
                reasons.append(f"주간 {weekly['period_pct']:.1f}%")
                score += 1

            high_drawdown = None
            if high20 and high20.get("high_drawdown_pct") is not None:
                high_drawdown = high20["high_drawdown_pct"]
                if high_drawdown <= -10:
                    reasons.append(f"20일 고점 대비 {high_drawdown:.1f}%")
                    score += 1

            if score > 0:
                owners = [o for o, ts in TICKERS.items() if ticker in ts]
                candidates.append({
                    "ticker": ticker,
                    "score": score,
                    "price": daily["end"],
                    "reasons": reasons,
                    "owners": owners,
                })
        except Exception:
            continue

    if not candidates:
        return "[추가매수 검토 후보]\n  오늘 조건에 맞는 후보가 없습니다. 관망 우선입니다."

    candidates.sort(key=lambda x: (-x["score"], x["ticker"]))

    lines = ["[추가매수 검토 후보]", "※ 매수 지시가 아니라 분할 접근 가능성 점검용입니다."]
    for idx, item in enumerate(candidates[:10], 1):
        lines.append(
            f"\n{idx}. {item['ticker']} (${item['price']:.2f})"
            f"\n   조건: {', '.join(item['reasons'])}"
            f"\n   보유 계좌: {', '.join(item['owners'])}"
        )

    return "\n".join(lines)


def get_overlap_analysis():
    """가족 전체 계좌의 중복 보유 종목을 분석한다."""
    owners_by_ticker = {}
    for owner, tickers in TICKERS.items():
        for ticker in tickers:
            owners_by_ticker.setdefault(ticker, []).append(owner)

    overlaps = {
        ticker: owners
        for ticker, owners in owners_by_ticker.items()
        if len(owners) >= 2
    }

    lines = ["[가족 전체 중복 보유 분석]"]
    if not overlaps:
        lines.append("  중복 보유 종목이 없습니다.")
        return "\n".join(lines)

    for ticker, owners in sorted(overlaps.items(), key=lambda x: (-len(x[1]), x[0])):
        lines.append(f"  {ticker} ({len(owners)}개 계좌): {' / '.join(owners)}")

    lines.append(
        "\n해석: 계좌는 분산되어 있지만 가족 전체 기준으로 "
        "AI·반도체·빅테크 테마 집중도가 높을 수 있습니다. "
        "추가매수 전 중복 노출 여부를 먼저 확인하세요."
    )
    return "\n".join(lines)

def get_account_profile(owner="전체"):
    """계좌별 성격을 진단한다."""
    if owner == "전체":
        lines = ["[계좌별 성격 진단]"]
        for name, profile in ACCOUNT_PROFILES.items():
            lines.append(f"  {name}: {profile}")
        return "\n".join(lines)

    if owner not in ACCOUNT_PROFILES:
        return f"{owner}의 계좌 성격 정보가 없습니다. 이현규/임인숙/이재현/이재연/전체 중 선택하세요."

    return f"[{owner} 계좌 성격]\n  {ACCOUNT_PROFILES[owner]}"


def get_family_events(months_ahead=2):
    """가까운 가족 기념일을 조회한다."""
    try:
        months_ahead = int(months_ahead)
    except Exception:
        months_ahead = 2

    today = today_kst()
    horizon = today + timedelta(days=31 * max(1, months_ahead))
    events = []

    for name, value in FAMILY_EVENTS.items():
        try:
            if len(value) == 10:
                event_date = datetime.strptime(value, "%Y-%m-%d").date()
            else:
                month, day = map(int, value.split("-"))
                event_date = date(today.year, month, day)
                if event_date < today:
                    event_date = date(today.year + 1, month, day)

            if today <= event_date <= horizon:
                events.append((event_date, name))
        except Exception:
            continue

    if not events:
        return f"[가족 기념일]\n  앞으로 약 {months_ahead}개월 안에 등록된 가족 기념일이 없습니다."

    events.sort()
    lines = [f"[가족 기념일 — 앞으로 약 {months_ahead}개월]"]
    for event_date, name in events:
        dday = (event_date - today).days
        if dday == 0:
            lines.append(f"  🎉 {name}: 오늘 ({event_date.strftime('%Y-%m-%d')})")
        else:
            lines.append(f"  {name}: D-{dday} ({event_date.strftime('%Y-%m-%d')})")
    return "\n".join(lines)


def get_school_briefing():
    """창원경일고 진로진학 업무용 주요 D-day를 조회한다."""
    today = today_kst()
    lines = ["[🏫 고3 진로진학 업무 및 학사일정 D-day]"]

    for event_name, event_date in sorted(SCHOOL_EVENTS.items(), key=lambda x: x[1]):
        dday = (event_date - today).days
        if dday > 0:
            lines.append(f"  • {event_name}: D-{dday} ({event_date.strftime('%Y-%m-%d')})")
        elif dday == 0:
            lines.append(f"  • 🎉 {event_name}: 오늘")
        else:
            lines.append(f"  • {event_name}: 종료됨 ({event_date.strftime('%Y-%m-%d')})")

    lines.append("\n이번 주 체크: 상담 누락 학생, 학생부 기재 초안, 학부모 상담 자료를 함께 점검하세요.")
    return "\n".join(lines)


def get_alarm_info():
    """브리핑 알람 시간을 반환한다."""
    return (
        "[브리핑 알람 설정]\n"
        f"  기본 브리핑 알람 시간: 매일 {BRIEFING_ALARM_TIME_KST} KST\n"
        "  변경 방법: Render 환경변수 BRIEFING_ALARM_TIME_KST 값을 예: 07:00 으로 수정\n"
        "  참고: 이 MCP 서버는 조회 도구입니다. 실제 푸시 알림 발송은 별도 스케줄러/자동화와 연결해야 합니다."
    )


def get_today_info():
    """오늘 날짜, 날씨, 전역 현황, 시장 지표를 간단히 반환한다."""
    now = now_kst()
    header = f"📅 {now.strftime('%Y년 %m월 %d일')} {weekday_ko(now)}요일"
    return "\n\n".join([
        header,
        get_alarm_info(),
        get_changwon_weather(),
        get_discharge_countdown(),
        get_market_status(),
    ])


def get_kkyu_briefing(owner="이현규"):
    """오늘의 날씨, 전역, 학교 일정, 시장, 포트폴리오, 급락 감지를 종합 브리핑한다."""
    if owner not in TICKERS and owner != "전체":
        owner = "이현규"

    now = now_kst()
    header = (
        "🚀 [뀨의 AI 임무 통제실 마스터 브리핑]\n"
        f"기준: {now.strftime('%Y년 %m월 %d일')} {weekday_ko(now)}요일 {now.strftime('%H:%M')} KST\n"
        f"브리핑 알람 기준: 매일 {BRIEFING_ALARM_TIME_KST} KST\n"
        + "=" * 56
    )

    sections = [
        ("1. 오늘 날씨", get_changwon_weather()),
        ("2. 이재현 복무 현황", get_discharge_countdown()),
        ("3. 가족 기념일", get_family_events(months_ahead=2)),
        ("4. 학교 일정", get_school_briefing()),
        ("5. 글로벌 시장", get_market_status()),
        ("6. 계좌 성격", get_account_profile(owner if owner != "전체" else "전체")),
        ("7. 포트폴리오 당일 흐름", get_portfolio(owner)),
        ("8. 포트폴리오 주간 흐름", get_weekly_performance(owner)),
        ("9. 급락 감지", get_drop_alert(threshold=-3.0)),
        ("10. 가족 중복 보유 리스크", get_overlap_analysis()),
    ]

    parts = [header]
    for title, content in sections:
        parts.append(f"\n[{title}]\n{content}")

    parts.append("\n" + "=" * 56)
    parts.append(
        "오늘의 판단:\n"
        "  • 급락 종목은 '추가매수 검토 후보'일 뿐, 바로 매수 지시가 아닙니다.\n"
        "  • 가족 전체 중복 보유가 큰 종목은 추가 진입 전 집중도부터 확인하세요.\n"
        "  • 학교 일정은 학생부·상담·수능 D-day 중심으로 선제 점검하세요."
    )
    return "\n".join(parts)

def get_system_status():
    """서버와 외부 API 상태를 점검한다."""
    now = now_kst()
    lines = ["[시스템 상태 점검]"]
    lines.append(f"  MCP 서버: 정상")
    lines.append(f"  현재 시각: {now.strftime('%Y-%m-%d %H:%M:%S')} KST")
    lines.append(f"  전역일 설정: {DISCHARGE_DATE.isoformat()}")
    lines.append(f"  브리핑 알람 시간: {BRIEFING_ALARM_TIME_KST} KST")

    try:
        _ = yf_history("MSFT", period="2d", ttl_seconds=300)
        lines.append("  yfinance: 정상")
    except Exception as exc:
        lines.append(f"  yfinance: 점검 필요 ({str(exc)})")

    try:
        weather = get_changwon_weather()
        if "조회 실패" in weather:
            lines.append("  Open-Meteo: 점검 필요")
        else:
            lines.append("  Open-Meteo: 정상")
    except Exception as exc:
        lines.append(f"  Open-Meteo: 점검 필요 ({str(exc)})")

    with _cache_lock:
        lines.append(f"  캐시 항목 수: {len(_cache)}")

    return "\n".join(lines)


# =============================================================================
# MCP 도구 스키마
# =============================================================================

TOOLS = [
    {
        "name": "get_kkyu_briefing",
        "description": "날짜, 알람시간, 창원 날씨, 전역 진행률, 학교 일정, 시장 지수, 포트폴리오, 급락 감지, 중복 보유 리스크를 한 번에 종합 브리핑한다.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "owner": {"type": "string", "description": "대표로 볼 계좌 소유자. 기본값: 이현규. 전체도 가능."}
            }
        },
    },
    {
        "name": "get_today_info",
        "description": "오늘 날짜, 알람시간, 창원 날씨, 전역 D-day, 시장 지표를 간단히 반환한다.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_portfolio",
        "description": "가족 포트폴리오 미국 주식 데이터를 조회한다. owner는 이현규/임인숙/이재현/이재연/전체 중 선택.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "owner": {"type": "string", "description": "계좌 소유자 이름 또는 전체"}
            },
        },
    },
    {
        "name": "get_weekly_performance",
        "description": "특정 계좌의 최근 7일간 주간 수익률을 조회한다. owner는 이현규/임인숙/이재현/이재연/전체 중 선택.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "owner": {"type": "string", "description": "계좌 소유자 이름 또는 전체"}
            },
        },
    },
    {
        "name": "get_market_status",
        "description": "S&P500, 나스닥, 원달러환율을 조회한다.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_drop_alert",
        "description": "가족 포트폴리오 종목 중 당일 급락 또는 주간 조정 종목을 감지한다. 매수 지시가 아니라 검토 후보로 표시한다.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "threshold": {"type": "number", "description": "당일 급락 기준. 기본값 -3.0"}
            },
        },
    },
    {
        "name": "get_buy_candidates",
        "description": "당일·주간·20일 고점 대비 조정 조건을 바탕으로 추가매수 검토 후보를 추린다. 매수 지시가 아니라 검토 후보로만 표시한다.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "owner": {"type": "string", "description": "계좌 소유자 이름 또는 전체"}
            },
        },
    },
    {
        "name": "get_overlap_analysis",
        "description": "가족 전체 계좌에서 중복 보유 종목과 집중 리스크를 분석한다.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_account_profile",
        "description": "계좌별 투자 성격을 진단한다. owner는 이현규/임인숙/이재현/이재연/전체 중 선택.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "owner": {"type": "string", "description": "계좌 소유자 이름 또는 전체"}
            },
        },
    },
    {
        "name": "get_discharge_countdown",
        "description": "이재현의 해병대 전역 잔여 일수, 복무 진행률, 남은 개월 수를 계산한다. 전역일은 2027년 7월 25일.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_changwon_weather",
        "description": "창원 현재 날씨, 강수확률, 풍속, 우산 필요 여부를 조회한다.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_family_events",
        "description": "가까운 가족 기념일과 D-day를 조회한다.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "months_ahead": {"type": "integer", "description": "몇 개월 앞까지 볼지. 기본값 2"}
            },
        },
    },
    {
        "name": "get_school_briefing",
        "description": "창원경일고 진로진학 업무용 주요 학사 일정과 D-day를 조회한다.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_alarm_info",
        "description": "브리핑 알람 기준 시간을 확인한다.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_system_status",
        "description": "MCP 서버, yfinance, Open-Meteo, 캐시 상태를 점검한다.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# =============================================================================
# JSON-RPC 처리
# =============================================================================

def handle_jsonrpc(data):
    if not isinstance(data, dict):
        return {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": "Invalid JSON-RPC request"},
        }

    method = data.get("method", "")
    req_id = data.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "뀨의 AI 임무 통제실", "version": "2.2.0"},
            },
        }

    if method == "notifications/initialized":
        return None

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": TOOLS},
        }

    if method == "tools/call":
        params = data.get("params", {}) or {}
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {}) or {}

        try:
            if tool_name == "get_kkyu_briefing":
                result = get_kkyu_briefing(arguments.get("owner", "이현규"))
            elif tool_name == "get_today_info":
                result = get_today_info()
            elif tool_name == "get_portfolio":
                result = get_portfolio(arguments.get("owner", "전체"))
            elif tool_name == "get_weekly_performance":
                result = get_weekly_performance(arguments.get("owner", "이현규"))
            elif tool_name == "get_market_status":
                result = get_market_status()
            elif tool_name == "get_drop_alert":
                result = get_drop_alert(arguments.get("threshold", -3.0))
            elif tool_name == "get_buy_candidates":
                result = get_buy_candidates(arguments.get("owner", "전체"))
            elif tool_name == "get_overlap_analysis":
                result = get_overlap_analysis()
            elif tool_name == "get_account_profile":
                result = get_account_profile(arguments.get("owner", "전체"))
            elif tool_name == "get_discharge_countdown":
                result = get_discharge_countdown()
            elif tool_name == "get_changwon_weather":
                result = get_changwon_weather()
            elif tool_name == "get_family_events":
                result = get_family_events(arguments.get("months_ahead", 2))
            elif tool_name == "get_school_briefing":
                result = get_school_briefing()
            elif tool_name == "get_alarm_info":
                result = get_alarm_info()
            elif tool_name == "get_system_status":
                result = get_system_status()
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"알 수 없는 도구: {tool_name}"},
                }

            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": result}]},
            }
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": f"도구 실행 중 오류: {str(exc)}"},
            }

    if method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": "Method not found"},
    }


# =============================================================================
# Flask 라우팅
# =============================================================================

@app.route("/")
def index():
    return jsonify({
        "status": "ok",
        "name": "뀨의 AI 임무 통제실 MCP 서버",
        "version": "2.2.0",
        "timezone": "Asia/Seoul",
        "briefing_alarm_time_kst": BRIEFING_ALARM_TIME_KST,
        "discharge_date": DISCHARGE_DATE.isoformat(),
        "sse_endpoint": "/sse",
        "message_endpoint": "/message",
        "tools": [tool["name"] for tool in TOOLS],
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
        endpoint_msg = f"event: endpoint\ndata: {base_url}/message?client_id={client_id}\n\n"
        yield endpoint_msg

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
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.route("/message", methods=["POST"])
def message():
    try:
        client_id = int(request.args.get("client_id", 0))
    except Exception:
        client_id = 0

    data = request.get_json(silent=True) or {}
    response = handle_jsonrpc(data)

    if response is not None:
        delivered = False
        with client_lock:
            q = client_queues.get(client_id)
            if q:
                q.put(response)
                delivered = True
        if delivered:
            return jsonify({"status": "accepted"}), 202
        # 일부 MCP 클라이언트가 POST 응답 자체를 기대하는 경우를 위해 fallback 반환
        return jsonify(response), 200

    return jsonify({"status": "accepted"}), 202


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, threaded=True)
