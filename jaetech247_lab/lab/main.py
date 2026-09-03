"""
lab/main.py — jaetech247.pro/lab
바이낸스 선물 그리드 8구역 멀티 실험실
포트 8081 — 메인(8080)과 독립 운영
"""
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import asyncio
import os
import sys
from pathlib import Path

# 상위 디렉토리의 trading 모듈을 가져오기 위한 경로 설정
sys.path.insert(0, str(Path(__file__).parent.parent))
from trading.futures_runner import FuturesAPI, FuturesGridRunner

# ── 환경변수 및 다중 테스터 설정 ─────────────────────────────
_ENV_FILE = Path.home() / "jaetech247_lab" / ".lab_env"
# 환경변수 파일이 저장될 폴더가 없다면 생성
_ENV_FILE.parent.mkdir(parents=True, exist_ok=True)

# 8개의 테스터 탭 아이디 생성
TESTERS = [f"TESTER{i}" for i in range(1, 9)]

# 테스터별 전역 상태 관리 딕셔너리
runners = {t: None for t in TESTERS}
tester_keys = {t: {"api_key": "", "secret_key": ""} for t in TESTERS}
tester_passwords = {t: "1234" for t in TESTERS} # 기본 비밀번호는 1234로 초기화
ADMIN_PASSWORD = "changeme!"

def _load_keys():
    """홈의 .lab_env 파일에서 최고 관리자 및 각 테스터별 키 로드"""
    global ADMIN_PASSWORD
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text().splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                os.environ[k.strip()] = v.strip()
    
    # 로드된 환경변수를 딕셔너리에 매핑
    ADMIN_PASSWORD = os.environ.get("LAB_ADMIN_PASSWORD", "changeme!")
    for t in TESTERS:
        tester_keys[t]["api_key"] = os.environ.get(f"{t}_API_KEY", "")
        tester_keys[t]["secret_key"] = os.environ.get(f"{t}_SECRET_KEY", "")
        tester_passwords[t] = os.environ.get(f"{t}_PASSWORD", "1234")

# 앱 시작 시 키 파일 로드
_load_keys()

def _persist_keys():
    """현재 메모리의 설정값들을 .lab_env 파일에 영구 저장"""
    lines = [f"LAB_ADMIN_PASSWORD={ADMIN_PASSWORD}"]
    for t in TESTERS:
        lines.append(f"{t}_API_KEY={tester_keys[t]['api_key']}")
        lines.append(f"{t}_SECRET_KEY={tester_keys[t]['secret_key']}")
        lines.append(f"{t}_PASSWORD={tester_passwords[t]}")
    _ENV_FILE.write_text("\n".join(lines))


# ── 서버 및 웹소켓 설정 ───────────────────────────────────────────
_ws_clients: set = set()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 구동 시 웹소켓 브로드캐스트 루프 시작
    broadcast_task = asyncio.create_task(_ws_broadcast_loop())
    yield
    # 서버 종료 시 실행 중인 모든 테스터의 봇을 안전하게 정지
    for t in TESTERS:
        if runners[t] and runners[t].running:
            runners[t].stop("서버종료")
    broadcast_task.cancel()

app = FastAPI(root_path="/lab", lifespan=lifespan)

# 정적 파일 경로 마운트 (CSS, JS 등)
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

def _get_all_status():
    """모든 테스터의 현재 상태를 하나의 딕셔너리로 취합하여 반환"""
    res = {}
    for t in TESTERS:
        res[t] = runners[t].get_status() if runners[t] else {
            "running": False, "symbol": "QQQUSDT", "cur_price": 0, "pnl_usdt": 0,
            "fill_count": 0, "orders": [], "fills": [], "sessions": [], "log": []
        }
    return res

async def broadcast_status():
    """연결된 모든 웹소켓 클라이언트에게 최신 상태 전송"""
    dead = set()
    msg = {"type": "lab_status", "data": _get_all_status()}
    for ws in list(_ws_clients):
        try: 
            await ws.send_json(msg)
        except Exception: 
            dead.add(ws)
    # 연결이 끊긴 클라이언트는 정리
    _ws_clients.difference_update(dead)

async def _ws_broadcast_loop():
    """5초마다 상태를 브로드캐스트하는 무한 루프"""
    while True:
        await asyncio.sleep(5)
        await broadcast_status()


# ── 라우터 (API 엔드포인트) ───────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def lab_page(request: Request):
    """메인 실험실 HTML 페이지 반환"""
    return templates.TemplateResponse("lab.html", {"request": request})

@app.websocket("/ws")
async def lab_ws(ws: WebSocket):
    """실시간 현황 업데이트용 웹소켓 연결"""
    await ws.accept()
    _ws_clients.add(ws)
    # 연결 즉시 현재 상태 1회 전송
    await ws.send_json({"type": "lab_status", "data": _get_all_status()})
    try:
        while True: 
            # 클라이언트로부터의 메시지 대기 (keep-alive)
            await ws.receive_text()
    except Exception:
        _ws_clients.discard(ws)

@app.get("/api/status")
async def get_status():
    """수동 상태 조회 API"""
    return _get_all_status()

@app.post("/api/{tester_id}/password/change")
async def change_password(tester_id: str, body: dict):
    """각 테스터의 비밀번호 변경 (최고 관리자 인증 필요)"""
    if tester_id not in TESTERS: 
        raise HTTPException(status_code=400, detail="유효하지 않은 테스터 ID입니다")
    
    if body.get("admin_password") != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="최고 관리자 암호가 틀립니다")
    
    tester_passwords[tester_id] = body.get("new_password", "1234").strip()
    await asyncio.to_thread(_persist_keys)
    return {"ok": True, "message": f"{tester_id} 비밀번호 변경 완료"}

@app.post("/api/{tester_id}/keys/save")
async def save_keys(tester_id: str, body: dict):
    """각 테스터의 API Key 저장 (해당 테스터 비밀번호 인증 필요)"""
    if tester_id not in TESTERS: 
        raise HTTPException(status_code=400, detail="유효하지 않은 테스터 ID입니다")
    
    if body.get("password") != tester_passwords[tester_id]:
        raise HTTPException(status_code=403, detail="해당 테스터의 암호가 틀립니다")
    
    tester_keys[tester_id]["api_key"] = body.get("api_key", "").strip()
    tester_keys[tester_id]["secret_key"] = body.get("secret_key", "").strip()
    await asyncio.to_thread(_persist_keys)
    return {"ok": True, "message": f"{tester_id} API 키 세팅 완료"}

@app.post("/api/{tester_id}/grid/start")
async def start_grid(tester_id: str, body: dict):
    """특정 테스터의 그리드 봇 시작"""
    if tester_id not in TESTERS: 
        raise HTTPException(status_code=400, detail="유효하지 않은 테스터 ID입니다")
    
    if body.get("password") != tester_passwords[tester_id]:
        raise HTTPException(status_code=403, detail="테스터 암호가 틀립니다")
    
    api_k = tester_keys[tester_id]["api_key"]
    sec_k = tester_keys[tester_id]["secret_key"]
    if not api_k: 
        raise HTTPException(status_code=400, detail="API 키를 먼저 등록하세요")
    
    runner = runners[tester_id]
    if runner and runner.running: 
        return {"ok": False, "error": "이미 실행 중입니다"}
    
    # 봇 객체 생성 및 파라미터 전달 (기준거래량, 최대이탈수량 추가됨)
    runners[tester_id] = FuturesGridRunner(
        symbol      = body.get("symbol", "QQQUSDT"),
        center      = float(body["center"]),
        unit        = float(body["unit"]),
        base_qty    = float(body.get("base_qty", 1.0)),
        max_dev_qty = float(body.get("max_dev_qty", 10.0)),
        limit       = int(body.get("limit", 6)),
        sell_adj    = float(body.get("sell_adj", 0.0)),
        trend       = body.get("trend", "중"),
    )
    
    api = FuturesAPI(api_k, sec_k)
    await runners[tester_id].start(api, broadcast_status)
    return {"ok": True}

@app.post("/api/{tester_id}/grid/stop")
async def stop_grid(tester_id: str, body: dict):
    """특정 테스터의 그리드 봇 정지"""
    if tester_id not in TESTERS: 
        raise HTTPException(status_code=400, detail="유효하지 않은 테스터 ID입니다")
    
    if body.get("password") != tester_passwords[tester_id]:
        raise HTTPException(status_code=403, detail="테스터 암호가 틀립니다")
    
    runner = runners[tester_id]
    if not runner or not runner.running: 
        return {"ok": False, "error": "실행 중인 그리드가 없습니다"}
    
    runner.stop("수동종료")
    await broadcast_status()
    return {"ok": True}

@app.get("/api/{tester_id}/balance")
async def get_balance(tester_id: str):
    """해당 테스터의 선물 지갑 잔고 조회"""
    if tester_id not in TESTERS: 
        return {"ok": False, "error": "유효하지 않은 테스터"}
    
    api_k = tester_keys[tester_id]["api_key"]
    sec_k = tester_keys[tester_id]["secret_key"]
    
    if not api_k: 
        return {"ok": False, "error": "API 키 없음"}
    
    try:
        api = FuturesAPI(api_k, sec_k)
        # 네트워크 I/O 병목을 방지하기 위해 비동기 쓰레드에서 실행
        bal = await asyncio.to_thread(api.balance)
        return {"ok": True, "usdt": bal["usdt"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}