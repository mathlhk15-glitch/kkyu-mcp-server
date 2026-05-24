import requests
import yfinance as yf
import pytz
from datetime import datetime, date
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("뀨의 AI 임무 통제실")

KST            = pytz.timezone("Asia/Seoul")
DISCHARGE_DATE = date(2027, 7, 26)
CHANGWON_LAT   = 35.2279
CHANGWON_LON   = 128.6811

TICKERS = {
    "이현규": ["VRT", "OII", "BWXT", "TEM", "ALAB"],
    "임인숙": ["MSFT", "GOOGL", "NVDA", "UNH", "QQQ"],
    "이재현": ["GOOGL", "TSM", "MRVL", "NVDA", "MSFT", "AVGO", "RKLB"],
    "이재연": ["GOOGL", "TSM", "MSFT", "NVDA", "LLY", "MRVL", "TSLA"],
}

@mcp.tool()
def get_portfolio(owner: str = "전체") -> str:
    """가족 포트폴리오 주식 데이터를 조회한다. owner는 이현규/임인숙/이재현/이재연/전체 중 선택."""
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
                t    = yf.Ticker(ticker)
                hist = t.history(period="2d")
                if len(hist) >= 2:
                    prev  = hist["Close"].iloc[-2]
                    today = hist["Close"].iloc[-1]
                    pct   = (today - prev) / prev * 100
                    arrow = "▲" if pct > 0 else "▼" if pct < 0 else "─"
                    result.append(f"  {ticker}: {arrow}{abs(pct):.1f}% (${today:.2f})")
                else:
                    result.append(f"  {ticker}: 데이터 없음")
            except Exception:
                result.append(f"  {ticker}: 조회 실패")
        result.append("")
    return "\n".join(result)

@mcp.tool()
def get_discharge_countdown() -> str:
    """이재현의 해병대 전역까지 남은 일수를 계산한다."""
    today = datetime.now(KST).date()
    dday  = (DISCHARGE_DATE - today).days
    if dday < 0:
        return "이재현 전역 완료!"
    elif dday == 0:
        return "이재현 오늘 전역!"
    elif dday <= 7:
        return f"이재현 전역 D-{dday} (거의 다 왔다!)"
    elif dday <= 30:
        return f"이재현 전역 D-{dday} (한 달 남음)"
    elif dday <= 100:
        return f"이재현 전역 D-{dday} (약 {dday//30}개월 남음)"
    else:
        return f"이재현 전역 D-{dday} (전역일: 2027년 7월 26일)"

@mcp.tool()
def get_changwon_weather() -> str:
    """창원 현재 날씨를 조회한다."""
    try:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={CHANGWON_LAT}&longitude={CHANGWON_LON}"
            "&current=temperature_2m,precipitation_probability,weathercode,windspeed_10m"
            "&timezone=Asia%2FSeoul"
        )
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data    = resp.json()
        current = data["current"]
        temp    = current["temperature_2m"]
        precip  = current["precipitation_probability"]
        wind    = current["windspeed_10m"]
        code    = current["weathercode"]
        weather_map = {
            0: "맑음", 1: "대체로 맑음", 2: "구름 조금", 3: "흐림",
            45: "안개", 48: "안개",
            51: "이슬비", 53: "이슬비", 55: "이슬비",
            61: "비", 63: "비", 65: "강한 비",
            71: "눈", 73: "눈", 75: "강한 눈",
            80: "소나기", 81: "소나기", 82: "강한 소나기",
            95: "뇌우", 96: "뇌우", 99: "뇌우"
        }
        desc = weather_map.get(code, "알 수 없음")
        return f"창원 현재 날씨: {desc} {temp}°C / 강수확률 {precip}% / 풍속 {wind}km/h"
    except Exception:
        return "날씨 조회 실패"

@mcp.tool()
def get_weekly_performance(owner: str = "이현규") -> str:
    """특정 계좌의 주간 수익률을 조회한다. owner는 이현규/임인숙/이재현/이재연 중 선택."""
    if owner not in TICKERS:
        return f"{owner}의 계좌 정보가 없습니다."
    tickers = TICKERS[owner]
    result  = [f"[{owner} 계좌 주간 수익률]"]
    for ticker in tickers:
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(period="7d")
            if len(hist) >= 2:
                start = hist["Close"].iloc[0]
                end   = hist["Close"].iloc[-1]
                pct   = (end - start) / start * 100
                arrow = "▲" if pct > 0 else "▼" if pct < 0 else "─"
                result.append(f"  {ticker}: {arrow}{abs(pct):.1f}%")
            else:
                result.append(f"  {ticker}: 데이터 없음")
        except Exception:
            result.append(f"  {ticker}: 조회 실패")
    return "\n".join(result)

@mcp.tool()
def get_today_info() -> str:
    """오늘 날짜, 요일, 전역 D-day를 한번에 반환한다."""
    now   = datetime.now(KST)
    days  = ["월", "화", "수", "목", "금", "토", "일"]
    today = f"{now.strftime('%Y년 %m월 %d일')} {days[now.weekday()]}요일"
    dday  = get_discharge_countdown()
    weather = get_changwon_weather()
    return f"{today}\n{dday}\n{weather}"

if __name__ == "__main__":
    mcp.run(transport="sse")
