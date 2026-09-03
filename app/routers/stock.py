"""
app/routers/stock.py
주식 기본형 채널 라우터 (채널 31~35)

- 채널당 최대 3개 종목 독립 GridEngine 구동 (grid_A / grid_B / grid_C)
- API 키는 유저 DB에서 복호화하여 KISBroker에 주입
- KIS Mock 모드 지원 (마이페이지에서 실거래/시뮬 전환)
- WebSocket: /stock/{ch_id}/ws — 실시간 그리드 상태 push
"""
import asyncio, time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from app.core.security import decrypt_api_key
from app.models.database import AsyncSessionLocal, Channel, User
from app.routers.auth import get_current_user

from trading.stock.broker_kis import KISBroker
from app.services import discord_alert as dc_svc
from trading.stock.broker_base import is_market_open, seconds_to_open
from trading.stock.grid_engine import GridEngine

router = APIRouter(prefix="/stock", tags=["stock"])
templates = Jinja2Templates(directory="app/templates")

# ── 런타임 저장소 ─────────────────────────────────────────────────────
# { channel_id: { ticker: GridEngine } }
_grids: dict[int, dict[str, GridEngine]] = {}
# { channel_id: KISBroker }
_brokers: dict[int, KISBroker] = {}
# { channel_id: set[WebSocket] }
_ws_pool: dict[int, set[WebSocket]] = {}

MAX_GRIDS_PER_CHANNEL = 3   # 채널당 최대 종목 수


# ── 브로드캐스트 ─────────────────────────────────────────────────────
async def _broadcast(ch_id: int, msg: dict):
    dead = set()
    for ws in list(_ws_pool.get(ch_id, set())):
        try: await ws.send_json(msg)
        except: dead.add(ws)
    if dead and ch_id in _ws_pool:
        _ws_pool[ch_id] -= dead


async def _bg_push_loop(ch_id: int):
    """WS 클라이언트가 있는 채널에 3초마다 전체 상태 push."""
    while True:
        await asyncio.sleep(3)
        grids = _grids.get(ch_id, {})
        if grids and _ws_pool.get(ch_id):
            from trading.stock.broker_base import is_market_open, seconds_to_open
            await _broadcast(ch_id, {
                "type": "all_states",
                "data": [g.get_state() for g in grids.values()],
                "market_open": is_market_open(),
                "seconds_to_open": seconds_to_open(),
            })

_bg_tasks: dict[int, asyncio.Task] = {}


# ── 채널 접근 / API 키 로드 ──────────────────────────────────────────
async def _check_access(ch_id: int, user: User) -> Channel:
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(Channel).where(Channel.channel_id == ch_id))
        ch  = res.scalar_one_or_none()
    if not ch:
        raise HTTPException(404, "채널 없음")
    if user.role != "admin" and ch.owner_id != user.id:
        raise HTTPException(403, "권한 없음")
    if ch.channel_type != "stock_basic":
        raise HTTPException(400, "주식 채널이 아닙니다.")
    return ch


async def _get_or_create_broker(ch_id: int, owner_id: str, mock: bool = False) -> KISBroker:
    """채널 브로커 인스턴스 반환 (없으면 생성, API 키 DB 주입)."""
    if ch_id in _brokers:
        return _brokers[ch_id]

    async with AsyncSessionLocal() as session:
        res  = await session.execute(select(User).where(User.id == owner_id))
        user = res.scalar_one_or_none()

    if not user or not user.kis_api_key_enc:
        raise HTTPException(400, "KIS API 키 미등록 — 마이페이지에서 먼저 등록하세요.")

    broker = KISBroker(
        app_key    = decrypt_api_key(user.kis_api_key_enc),
        app_secret = decrypt_api_key(user.kis_api_secret_enc or ""),
        account_no = user.kis_account_no or "",
        mock       = mock,
    )
    _brokers[ch_id] = broker
    return broker


# ── UI 서빙 ──────────────────────────────────────────────────────────
@router.get("/{ch_id}", response_class=HTMLResponse)
async def stock_channel_page(ch_id: int, request: Request,
                              user: User = Depends(get_current_user)):
    ch = await _check_access(ch_id, user)
    return templates.TemplateResponse("user/channel_stock.html", {
        "request": request, "user": user, "channel": ch, "channel_id": ch_id,
    })


# ── WebSocket ─────────────────────────────────────────────────────────
@router.websocket("/{ch_id}/ws")
async def stock_ws(ch_id: int, websocket: WebSocket):
    await websocket.accept()
    token = websocket.cookies.get("jt247_token")
    from app.core.security import decode_token
    if not decode_token(token):
        await websocket.send_json({"error": "Unauthorized"})
        await websocket.close(4001); return

    if ch_id not in _ws_pool:
        _ws_pool[ch_id] = set()
    _ws_pool[ch_id].add(websocket)

    # 현재 상태 즉시 push (페이지 재진입 시 복원용)
    grids = _grids.get(ch_id, {})
    await websocket.send_json({
        "type": "all_states",
        "data": [g.get_state() for g in grids.values()],
        "market_open": is_market_open(),
        "seconds_to_open": seconds_to_open(),
    })

    # bg_loop 시작 (첫 클라이언트 연결 시)
    if ch_id not in _bg_tasks or _bg_tasks[ch_id].done():
        _bg_tasks[ch_id] = asyncio.create_task(_bg_push_loop(ch_id))

    try:
        while True:
            await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
    except (asyncio.TimeoutError, WebSocketDisconnect):
        pass
    except Exception:
        pass
    finally:
        if ch_id in _ws_pool:
            _ws_pool[ch_id].discard(websocket)


# ── 그리드 시작 ───────────────────────────────────────────────────────
@router.post("/{ch_id}/grid/start")
async def start_grid(ch_id: int, body: dict,
                     user: User = Depends(get_current_user)):
    ch = await _check_access(ch_id, user)
    ticker = body.get("ticker","").upper().strip()
    if not ticker:
        return {"ok": False, "error": "종목코드 필수"}

    if ch_id not in _grids:
        _grids[ch_id] = {}

    if ticker in _grids[ch_id] and _grids[ch_id][ticker].running:
        return {"ok": False, "error": f"{ticker} 이미 실행 중"}

    if len([g for g in _grids[ch_id].values() if g.running]) >= MAX_GRIDS_PER_CHANNEL:
        return {"ok": False, "error": f"채널당 최대 {MAX_GRIDS_PER_CHANNEL}개 종목만 구동 가능"}

    mock = body.get("mock", True)
    try:
        broker = await _get_or_create_broker(ch_id, ch.owner_id, mock)
    except HTTPException as e:
        return {"ok": False, "error": e.detail}

    g = GridEngine(ticker, broker, user_id=ch.owner_id)
    g.set_config(
        base_qty   = int(body.get("base_qty", 1)),
        max_levels = int(body.get("max_levels", 5)),
        buy_gap    = int(body.get("buy_gap", 500)),
        sell_gap   = int(body.get("sell_gap", 500)),
        init_qty   = int(body.get("init_qty", 0)),
    )
    init_center = int(body.get("init_center", 0))
    if init_center > 0:
        g.center = init_center; g._init_center = init_center

    _grids[ch_id][ticker] = g

    async def _bcast(msg):
        await _broadcast(ch_id, msg)

    await g.start(_bcast)
    return {"ok": True, "ticker": ticker}


# ── 그리드 정지 ───────────────────────────────────────────────────────
@router.post("/{ch_id}/grid/{ticker}/stop")
async def stop_grid(ch_id: int, ticker: str,
                    user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    g = _grids.get(ch_id, {}).get(ticker.upper())
    if not g:
        return {"ok": False, "error": "없음"}
    await g.stop()
    await _broadcast(ch_id, {"type": "stock_update", "data": g.get_state()})
    return {"ok": True}


# ── 그리드 삭제 ───────────────────────────────────────────────────────
@router.delete("/{ch_id}/grid/{ticker}")
async def delete_grid(ch_id: int, ticker: str,
                      user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    t = ticker.upper()
    g = _grids.get(ch_id, {}).get(t)
    if g and g.running:
        await g.stop()
    _grids.get(ch_id, {}).pop(t, None)
    return {"ok": True}


# ── 수동 재배치 ───────────────────────────────────────────────────────
@router.post("/{ch_id}/grid/{ticker}/rebalance")
async def rebalance_grid(ch_id: int, ticker: str, body: dict = {},
                         user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    g = _grids.get(ch_id, {}).get(ticker.upper())
    if not g or not g.running:
        return {"ok": False, "error": "실행 중 아님"}
    nc = int(body["center"]) if body.get("center") else None
    await g.manual_rebalance(nc)
    return {"ok": True, "new_center": g.center}


# ── 전량 매도 ─────────────────────────────────────────────────────────
@router.post("/{ch_id}/grid/{ticker}/sell_all")
async def sell_all(ch_id: int, ticker: str,
                   user: User = Depends(get_current_user)):
    ch = await _check_access(ch_id, user)
    t  = ticker.upper()
    g  = _grids.get(ch_id, {}).get(t)
    if not g:
        return {"ok": False, "error": "그리드 없음"}
    if g.holding_qty <= 0:
        return {"ok": False, "error": "보유 없음"}
    broker = _brokers.get(ch_id)
    if not broker:
        return {"ok": False, "error": "브로커 없음"}
    r = await asyncio.to_thread(
        broker.place_order, t, "SELL", g.holding_qty, g.current_price
    )
    if r.get("ok"):
        g._log(f"수동전량매도 {g.holding_qty}주@{g.current_price:,}")
    return r


# ── 수동 주문 ─────────────────────────────────────────────────────────
@router.post("/{ch_id}/manual_order")
async def manual_order(ch_id: int, body: dict,
                       user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    broker = _brokers.get(ch_id)
    if not broker:
        return {"ok": False, "error": "브로커 없음 — 먼저 그리드를 시작하세요"}
    t    = body.get("ticker","").upper()
    side = body.get("side","").upper()
    qty  = int(body.get("qty", 0))
    price = int(body.get("price", 0))
    if not t or not side or qty <= 0 or price <= 0:
        return {"ok": False, "error": "종목/방향/수량/가격 필수"}
    return await asyncio.to_thread(broker.place_order, t, side, qty, price)


# ── 잔고 조회 ─────────────────────────────────────────────────────────
@router.get("/{ch_id}/balance")
async def get_balance(ch_id: int, user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    broker = _brokers.get(ch_id)
    if not broker:
        return {"ok": False, "error": "브로커 없음"}
    return await asyncio.to_thread(broker.get_balance)


# ── 현재가 조회 ───────────────────────────────────────────────────────
@router.get("/{ch_id}/price/{ticker}")
async def get_price(ch_id: int, ticker: str,
                    user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    broker = _brokers.get(ch_id)
    if not broker:
        # 브로커 없어도 공개 API로 조회 (Mock)
        tmp = KISBroker(mock=True)
        return await asyncio.to_thread(tmp.get_price, ticker.upper())
    return await asyncio.to_thread(broker.get_price, ticker.upper())


# ── 전체 상태 ─────────────────────────────────────────────────────────
@router.get("/{ch_id}/grids")
async def all_grids(ch_id: int, user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    grids = _grids.get(ch_id, {})
    return {
        "grids": [g.get_state() for g in grids.values()],
        "market_open": is_market_open(),
        "seconds_to_open": seconds_to_open(),
    }


# ── Mock 전환 ─────────────────────────────────────────────────────────
@router.post("/{ch_id}/broker/mock")
async def set_mock(ch_id: int, body: dict,
                   user: User = Depends(get_current_user)):
    ch = await _check_access(ch_id, user)
    broker = _brokers.get(ch_id)
    if broker:
        broker.mock = bool(body.get("mock", True))
        return {"ok": True, "mock": broker.mock}
    return {"ok": False, "error": "브로커 없음"}


# ── 디스코드 알림 엔드포인트 ──────────────────────────────────────
@router.post("/{ch_id}/alert/save")
async def stock_alert_save(ch_id: int, body: dict,
                            user: User = Depends(get_current_user)):
    ch = await _check_access(ch_id, user)
    webhook  = body.get("webhook", "").strip()
    interval = body.get("interval", "1h")
    if webhook and not webhook.startswith("https://discord.com/api/webhooks/"):
        return {"ok": False, "error": "Discord Webhook URL 형식이 올바르지 않습니다."}

    alert = dc_svc.get_or_create(ch_id)

    def state_provider():
        return [g.get_state() for g in _grids.get(ch_id, {}).values() if g.running]

    alert.configure(webhook, interval, state_provider, bithumb_provider=None)
    return {"ok": True, "interval": interval}


@router.post("/{ch_id}/alert/toggle")
async def stock_alert_toggle(ch_id: int,
                              user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    alert = dc_svc.get_or_create(ch_id)
    if not alert.webhook:
        return {"ok": False, "error": "Webhook URL을 먼저 저장하세요."}

    if alert.enabled:
        await alert.stop()
        return {"ok": True, "enabled": False}
    else:
        if not alert._state_provider:
            def state_provider():
                return [g.get_state() for g in _grids.get(ch_id, {}).values() if g.running]
            alert.configure(alert.webhook, "1h", state_provider)
        await alert.start()
        return {"ok": True, "enabled": True}


@router.post("/{ch_id}/alert/send_now")
async def stock_alert_send_now(ch_id: int,
                                user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    alert = dc_svc.get(ch_id)
    if not alert or not alert.webhook:
        return {"ok": False, "error": "Webhook URL을 먼저 저장하세요."}
    ok = await alert.send_now()
    return {"ok": ok}


@router.get("/{ch_id}/alert/status")
async def stock_alert_status(ch_id: int,
                              user: User = Depends(get_current_user)):
    await _check_access(ch_id, user)
    alert = dc_svc.get(ch_id)
    return alert.get_status() if alert else {"enabled": False, "webhook": "", "interval_key": "1h"}


# ── Graceful Shutdown Hook ────────────────────────────────────────────────────
async def stop_all_runners():
    """lifespan 종료 시 호출 — 모든 채널 GridEngine 안전 정지 및 상태 저장."""
    tasks = []
    for ch_id, grids in _grids.items():
        for ticker, g in grids.items():
            if g.running:
                await g.stop()   # cancel_all + save_state 포함
    # bg_tasks 취소
    for ch_id, task in _bg_tasks.items():
        if not task.done():
            task.cancel()
    if _bg_tasks:
        await asyncio.gather(*list(_bg_tasks.values()), return_exceptions=True)
