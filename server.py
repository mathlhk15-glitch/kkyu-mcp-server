import os
import json
import requests
import yfinance as yf
import pytz
from datetime import datetime, date
from flask import Flask, Response, request, jsonify

app = Flask(__name__)

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

def get_portfolio(owner="전체"):
    if owner == "전체":
        targets = TICKERS
    elif owner in TICKERS:
        targets = {owner: TICKERS[owner]}
    else:
        return f"{owner}의 계좌 정보가 없습니다."
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

def get_discharge_countdown():
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
    else:
        return f"이재현 전역 D-{dday} (전역일: 2027년 7월 26일)"

def get_changwon_weather():
    try:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={CHANGWON_LAT}&longitude={CHANGWON_LON}"
            "&current=temperature_2m,precipitation_probability,weathercode"
            "&timezone=Asia%2FSeoul"
        )
        resp = requests.get(url, timeout=10)
        data    = resp.json()
        current = data["current"]
        temp    = current["temperature_2m"]
        precip  = current["precipitation_probability"]
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
        return f"창원 날씨: {desc} {temp}C / 강수확률 {precip}%"
    except Exception:
        return "날씨 조회 실패"

TOOLS = [
    {
        "name": "get_portfolio",
        "description": "가족 포트폴리오 주식 데이터를 조회한다. owner는 이현규/임인숙/이재현/이재연/전체 중 선택.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "owner": {"type": "string", "description": "계좌 소유자 이름 또는 전체"}
            }
        }
    },
    {
        "name": "get_discharge_countdown",
        "description": "이재현의 해병대 전역까지 남은 일수를 계산한다.",
        "inputSchema": {"type": "object", "properties": {}}
    },
    {
        "name": "get_changwon_weather",
        "description": "창원 현재 날씨를 조회한다.",
        "inputSchema": {"type": "object", "properties": {}}
    }
]

@app.route("/")
def index():
    return jsonify({"status": "ok", "name": "뀨의 AI 임무 통제실 MCP 서버"})

@app.route("/sse")
def sse():
    def generate():
        init_msg = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }
        yield f"data: {json.dumps(init_msg)}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/mcp", methods=["POST"])
def mcp_endpoint():
    data   = request.get_json()
    method = data.get("method", "")
    req_id = data.get("id")

    if method == "initialize":
        return jsonify({
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "뀨의 AI 임무 통제실", "version": "1.0.0"}
            }
        })

    elif method == "tools/list":
        return jsonify({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"tools": TOOLS}
        })

    elif method == "tools/call":
        tool_name = data.get("params", {}).get("name", "")
        arguments = data.get("params", {}).get("arguments", {})

        if tool_name == "get_portfolio":
            owner  = arguments.get("owner", "전체")
            result = get_portfolio(owner)
        elif tool_name == "get_discharge_countdown":
            result = get_discharge_countdown()
        elif tool_name == "get_changwon_weather":
            result = get_changwon_weather()
        else:
            result = f"알 수 없는 도구: {tool_name}"

        return jsonify({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"content": [{"type": "text", "text": result}]}
        })

    return jsonify({"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": "Method not found"}})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
