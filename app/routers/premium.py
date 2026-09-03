"""
app/routers/premium.py
프리미엄 채널 전용 라우터 (채널 01~06, 관리자/VIP)

제공하는 API:
  GET  /premium/grid                     — grid.html 페이지 서빙
  POST /premium/api/grid/start           — 빗썸 Grid2 시작
  POST /premium/api/grid/{sym}/stop      — 빗썸 Grid2 정지
  GET  /premium/api/grid                 — 전체 슬롯 상태
  GET  /premium/api/grid/{sym}           — 단일 슬롯 상태
  POST /premium/api/binance/keys          — 바이낸스 API 키 저장
  GET  /premium/api/binance/portfolio     — 바이낸스 현물+선물 자산
  GET  /premium/api/bithumb/portfolio     — 빗썸 자산 조회
  POST /premium/api/binance/grid/start    — 바이낸스 그리드 시작
  POST /premium/api/binance/grid/{sym}/stop
  GET  /premium/api/binance/grid          — 바이낸스 그리드 전체 상태
  GET  /premium/api/binance/ticker/{sym}
  POST /premium/api/alert/toggle          — 디스코드 알림 ON/OFF
  POST /premium/api/alert/send_now        — 디스코드 즉시 전송
  WS   /premium/ws                        — WebSocket 브로드캐스트
  GET  /premium/api/pnl15/{sym}           — 15분 손익
  GET  /premium/api/grid/{sym}/default_tick
"""

import asyncio
import hashlib
import hmac
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from app.core.security import decrypt_api_key
from app.models.database import AsyncSessionLocal, Channel, User, get_db
from app.routers.auth import require_admin, require_active_user
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trading.premium.grid_runner import Grid2Runner
from trading.premium.binance_runner import BinanceAPI, BnGridRunner

router = APIRouter(prefix="/premium", tags=["premium"])

# ── 실행기 저장소 ─────────────────────────────────────────────────────
grid_runners: dict[str, Grid2Runner]       = {}   # { symbol: Grid2Runner }
binance_grid_runners: dict[str, BnGridRunner] = {} # { "BN:BTCUSDT": BnGridRunner }
binance_api = BinanceAPI()

# ── WebSocket 브로드캐스트 ────────────────────────────────────────────
_ws_clients: list[WebSocket] = []


async def broadcast(msg: dict):
    dead = []
    for ws in list(_ws_clients):
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.remove(ws)


@router.websocket("/ws")
async def premium_ws(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in _ws_clients:
            _ws_clients.remove(websocket)


# ── grid.html 서빙 ───────────────────────────────────────────────────
_GRID2_HTML = Path(__file__).parent.parent / "templates" / "premium" / "grid.html"


@router.get("/grid", response_class=HTMLResponse)
async def grid_page(admin: User = Depends(require_active_user)):
    """
    grid.html 서빙.
    string replace 대신 <script> 상수 블록을 <head> 앞에 주입하여
    BASE_API, WS_PATH 를 /premium/* 경로로 오버라이드.
    """
    if not _GRID2_HTML.exists():
        raise HTTPException(status_code=404, detail="grid.html 없음")
    content = _GRID2_HTML.read_text(encoding="utf-8")

    # JS 상수 주입 블록 — grid.html이 이 변수들을 참조하도록 앞에 삽입
    inject = """<script>
/* JaeTech247 경로 오버라이드 — premium 라우터에서 주입 */
window.__BASE_API__ = '/premium/api';
window.__WS_PATH__  = '/premium/ws';
</script>
"""
    # <head> 또는 <html> 바로 뒤에 삽입 (없으면 맨 앞)
    for tag in ['<head>', '<html>', '<!DOCTYPE']:
        idx = content.lower().find(tag.lower())
        if idx != -1:
            end = content.index('>', idx) + 1
            content = content[:end] + '\n' + inject + content[end:]
            break
    else:
        content = inject + content

    return HTMLResponse(content)


# ── 빗썸 API 클라이언트 (채널 소유자 키 기반) ────────────────────────
async def _get_bithumb_api_for_channel(channel_id: int):
    """채널 소유자의 복호화된 빗썸 API 키로 BithumbAPI 인스턴스 생성."""
    from app.models.database import AsyncSessionLocal, Channel, User
    from trading.channels.crypto_basic import BithumbClient
    import httpx

    async with AsyncSessionLocal() as session:
        ch_res = await session.execute(select(Channel).where(Channel.channel_id == channel_id))
        ch = ch_res.scalar_one_or_none()
        if not ch:
            return None, None
        u_res = await session.execute(select(User).where(User.id == ch.owner_id))
        user = u_res.scalar_one_or_none()
        if not user:
            return None, None

        api_key    = decrypt_api_key(user.bithumb_api_key_enc or "")
        api_secret = decrypt_api_key(user.bithumb_api_secret_enc or "")

    return BithumbAPICompat(api_key, api_secret), ch.owner_id


# ── 빗썸 호환 API 래퍼 (서버 임베드) ─────────────────────────────────
# server.py의 BithumbAPI를 그대로 여기 내장합니다.
import jwt as pyjwt
import uuid as _uuid


class BithumbAPICompat:
    """
    server.py BithumbAPI 완전 이식.
    grid_runner.py가 self.api.ticker(), self.api._private() 등을 호출하므로
    동일 인터페이스 유지.
    """
    BASE = "https://api.bithumb.com"

    def __init__(self, api_key: str, secret_key: str):
        self.api_key    = api_key.strip()
        self.secret_key = secret_key.strip()

    def _market(self, sym: str) -> str:
        return f"KRW-{sym.upper()}"

    def _server_timestamp(self) -> int:
        try:
            import email.utils
            r = requests.get(f"{self.BASE}/v1/ticker?markets=KRW-BTC", timeout=3)
            return round(email.utils.parsedate_to_datetime(r.headers['Date']).timestamp() * 1000)
        except Exception:
            return round(time.time() * 1000)

    def _make_jwt(self, params: dict = None) -> str:
        payload = {
            "access_key": self.api_key,
            "nonce":      str(_uuid.uuid4()),
            "timestamp":  self._server_timestamp(),
        }
        if params:
            qs = urllib.parse.urlencode(params)
            qh = hashlib.sha512(qs.encode()).hexdigest()
            payload["query_hash"]     = qh
            payload["query_hash_alg"] = "SHA512"
        return pyjwt.encode(payload, self.secret_key, algorithm="HS256")

    def _get(self, path: str, params: dict = None) -> dict:
        token = self._make_jwt(params)
        r = requests.get(self.BASE + path, params=params,
                         headers={"Authorization": f"Bearer {token}"}, timeout=5)
        return r.json() if r.ok else {"error": r.text}

    def _post(self, path: str, body: dict) -> dict:
        token = self._make_jwt(body)
        r = requests.post(self.BASE + path, json=body,
                          headers={"Authorization": f"Bearer {token}",
                                   "Content-Type": "application/json"}, timeout=5)
        return r.json() if r.ok else {"error": r.text}

    def _delete(self, path: str, params: dict) -> dict:
        token = self._make_jwt(params)
        r = requests.delete(self.BASE + path, params=params,
                            headers={"Authorization": f"Bearer {token}"}, timeout=5)
        return r.json() if r.ok else {"error": r.text}

    def ticker(self, symbol: str) -> dict:
        try:
            r = requests.get(f"{self.BASE}/v1/ticker",
                             params={"markets": self._market(symbol)}, timeout=5)
            data = r.json()
            d = data[0] if isinstance(data, list) and data else data
            if "trade_price" not in d:
                return {"status": "error"}
            return {"status": "0000", "data": {
                "closing_price":      str(d.get("trade_price", 0)),
                "prev_closing_price": str(d.get("prev_closing_price", 0)),
                "units_traded_24H":   str(d.get("acc_trade_volume_24h", 0)),
            }}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def orderbook(self, symbol: str) -> dict:
        try:
            r = requests.get(f"{self.BASE}/v1/orderbook",
                             params={"markets": self._market(symbol), "level": 0}, timeout=5)
            data = r.json()
            if isinstance(data, list) and data:
                d = data[0]
                return {"status": "0000", "data": {
                    "asks": [{"price": str(a["ask_price"]), "quantity": str(a["ask_size"])}
                             for a in d.get("orderbook_units", [])[:5]],
                    "bids": [{"price": str(b["bid_price"]), "quantity": str(b["bid_size"])}
                             for b in d.get("orderbook_units", [])[:5]],
                }}
            return {"status": "error"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def balance(self) -> dict:
        return self._get("/v1/accounts")

    def _private(self, endpoint: str, params: dict) -> dict:
        """grid_runner 호환 인터페이스"""
        try:
            sym    = params.get("order_currency", "")
            market = self._market(sym)

            if endpoint == "/trade/place":
                side = "bid" if params.get("type") == "bid" else "ask"
                raw_price = float(params["price"])
                price_str = str(round(raw_price, 2)) if raw_price < 100 else str(int(raw_price))
                body = {
                    "market":     market,
                    "side":       side,
                    "order_type": "limit",
                    "price":      price_str,
                    "volume":     str(float(params["units"])),
                }
                result = self._post("/v2/orders", body)
                oid = result.get("order_id") or result.get("uuid")
                if oid:
                    return {"status": "0000", "data": {"order_id": str(oid)}}
                return {"status": "error", "message": str(result)}

            elif endpoint == "/trade/cancel":
                result = self._delete("/v1/order", {"uuid": str(params["order_id"])})
                if isinstance(result, dict) and ("uuid" in result or "order_id" in result):
                    return {"status": "0000"}
                return {"status": "error", "message": str(result)}

            elif endpoint == "/info/orders":
                result = self._get("/v1/orders", {
                    "market": market, "state": "wait", "limit": 100,
                })
                if isinstance(result, list):
                    return {"status": "0000",
                            "data": [{"order_id": o.get("uuid")} for o in result if o.get("uuid")]}
                return {"status": "error", "message": str(result)}

            elif endpoint == "/info/balance":
                return self.balance()

            return {"status": "error", "message": f"unknown: {endpoint}"}
        except Exception as e:
            return {"status": "error", "message": str(e)}


# ── Grid2 엔드포인트 ──────────────────────────────────────────────────
@router.post("/api/grid/start")
async def start_grid(body: dict, admin: User = Depends(require_active_user)):
    sym = body.get("symbol", "").upper()
    if not sym:
        return {"ok": False, "error": "종목 필요"}
    if sym in grid_runners and grid_runners[sym].running:
        return {"ok": False, "error": "이미 실행 중"}

    runner = Grid2Runner(
        symbol     = sym,
        center     = float(body["center"]),
        unit       = float(body["unit"]),
        init_h     = float(body.get("init_h", 1.0)),
        limit      = int(body.get("limit", 6)),
        drain_mode = bool(body.get("drain_mode", False)),
        sell_adj   = float(body.get("sell_adj", 0)),
        trend      = body.get("trend", "중"),
    )
    if sym in grid_runners:
        prev = grid_runners[sym]
        runner.sessions   = prev.sessions
        runner.round_idx  = prev.round_idx
        runner.coin_delta = prev.coin_delta
        runner.pnl_krw    = prev.pnl_krw

    grid_runners[sym] = runner

    # 관리자 본인 API 키로 API 인스턴스 생성
    async with AsyncSessionLocal() as session:
        u_res = await session.execute(select(User).where(User.id == admin.id))
        user = u_res.scalar_one_or_none()
        ak = decrypt_api_key(user.bithumb_api_key_enc or "") if user else ""
        sk = decrypt_api_key(user.bithumb_api_secret_enc or "") if user else ""

    api = BithumbAPICompat(ak, sk)
    await runner.start(api, broadcast)
    return {"ok": True}


@router.post("/api/grid/{symbol}/stop")
async def stop_grid(symbol: str, body: dict = {}, admin: User = Depends(require_active_user)):
    sym = symbol.upper()
    if sym not in grid_runners:
        return {"ok": False, "error": "실행기 없음"}
    reason = body.get("reason", "수동종료") if body else "수동종료"
    grid_runners[sym].stop(reason)
    await broadcast({"type": "grid_status", "data": grid_runners[sym].get_status()})
    return {"ok": True, "sessions": len(grid_runners[sym].sessions)}


@router.get("/api/grid")
async def list_grid(admin: User = Depends(require_active_user)):
    return {sym: r.get_status() for sym, r in grid_runners.items()}


@router.get("/api/grid/{symbol}")
async def get_grid(symbol: str, admin: User = Depends(require_active_user)):
    sym = symbol.upper()
    if sym not in grid_runners:
        return {"symbol": sym, "running": False, "orders": [], "fills": [], "sessions": [], "log": []}
    return grid_runners[sym].get_status()


# ── 기본 tick 계산 ────────────────────────────────────────────────────
@router.get("/api/grid/{symbol}/default_tick")
async def default_tick(symbol: str, admin: User = Depends(require_active_user)):
    sym = symbol.upper()
    try:
        async with AsyncSessionLocal() as session:
            u_res = await session.execute(select(User).where(User.id == admin.id))
            user = u_res.scalar_one_or_none()
            ak = decrypt_api_key(user.bithumb_api_key_enc or "") if user else ""
            sk = decrypt_api_key(user.bithumb_api_secret_enc or "") if user else ""
        api = BithumbAPICompat(ak, sk)
        t   = await asyncio.to_thread(api.ticker, sym)
        cur = float(t["data"]["closing_price"])
        from trading.premium.grid_runner import tick_unit
        u = tick_unit(cur)
        raw = cur * 0.008
        n   = max(2, round(raw / u))
        if n % 2 != 0:
            n += 1
        tick = u * n
        return {"tick_size": tick, "cur_price": cur}
    except Exception as e:
        return {"error": str(e)}


# ── 바이낸스 엔드포인트 ───────────────────────────────────────────────
@router.post("/api/binance/keys")
async def set_binance_keys(body: dict, admin: User = Depends(require_active_user)):
    ak = body.get("api_key", "").strip()
    sk = body.get("secret_key", "").strip()
    if not ak or not sk:
        return {"ok": False, "error": "api_key, secret_key 필요"}
    # 런타임 세팅
    binance_api.api_key    = ak
    binance_api.secret_key = sk
    # DB 저장 (관리자 계정 KIS 필드 재활용 대신 별도 암호화 저장)
    from app.core.security import encrypt_api_key
    async with AsyncSessionLocal() as session:
        u_res = await session.execute(select(User).where(User.id == admin.id))
        user = u_res.scalar_one_or_none()
        if user:
            # kis 필드를 바이낸스용으로도 활용 (별도 컬럼 추가 전 임시)
            user.kis_api_key_enc    = encrypt_api_key(ak)
            user.kis_api_secret_enc = encrypt_api_key(sk)
            await session.commit()
    return {"ok": True}


@router.get("/api/binance/portfolio")
async def get_binance_portfolio(admin: User = Depends(require_active_user)):
    if not binance_api.api_key:
        return {"ok": False, "error": "API 키 미설정"}

    async def _safe(coro, default):
        try:
            return await asyncio.wait_for(coro, timeout=15.0)
        except Exception:
            return default

    spot_r, facc_r, fpos_r, prices_r = await asyncio.gather(
        _safe(asyncio.to_thread(binance_api.balance),             {"ok": False}),
        _safe(asyncio.to_thread(binance_api.futures_account),     {"ok": False}),
        _safe(asyncio.to_thread(binance_api.futures_positions),   {"ok": False}),
        _safe(asyncio.to_thread(binance_api.spot_prices_bulk, []), {"ok": False}),
    )

    all_prices = prices_r.get("prices", {}) if prices_r.get("ok") else {}

    def to_usd(asset, qty):
        if asset in ("USDT", "BUSD"):
            return qty
        return qty * all_prices.get(asset + "USDT", 0)

    spot_items = []
    spot_total_usd = 0.0
    if spot_r.get("ok"):
        for asset, v in spot_r["assets"].items():
            qty = v["free"] + v["locked"]
            if qty < 1e-8:
                continue
            usd = to_usd(asset, qty)
            spot_items.append({"asset": asset, "qty": qty, "usd": round(usd, 4)})
            spot_total_usd += usd

    fut = {"ok": False}
    if facc_r.get("ok"):
        fut = {
            "ok":             True,
            "wallet_usd":     round(facc_r["total_wallet_balance"], 4),
            "unrealized_pnl": round(facc_r["total_unrealized_profit"], 4),
            "margin_balance": round(facc_r["total_margin_balance"], 4),
            "available":      round(facc_r["available_balance"], 4),
            "positions":      fpos_r.get("positions", []) if fpos_r.get("ok") else [],
        }

    return {
        "ok": True,
        "grand_total_usd": round(spot_total_usd + fut.get("margin_balance", 0), 4),
        "spot": {"total_usd": round(spot_total_usd, 4), "items": spot_items},
        "futures": fut,
    }


@router.get("/api/binance/balance")
async def get_binance_balance(admin: User = Depends(require_active_user)):
    if not binance_api.api_key:
        return {"ok": False, "error": "API 키 미설정"}
    return await asyncio.to_thread(binance_api.balance)


@router.get("/api/binance/ticker/{symbol}")
async def bn_ticker(symbol: str, admin: User = Depends(require_active_user)):
    return await asyncio.to_thread(binance_api.ticker, symbol)


@router.post("/api/binance/grid/start")
async def start_bn_grid(body: dict, admin: User = Depends(require_active_user)):
    if not binance_api.api_key:
        return {"ok": False, "error": "바이낸스 API 키를 먼저 설정하세요"}
    sym = body.get("symbol", "").upper().replace("USDT", "")
    if not sym:
        return {"ok": False, "error": "종목 필요"}
    key = f"BN:{sym}USDT"
    if key in binance_grid_runners and binance_grid_runners[key].running:
        return {"ok": False, "error": "이미 실행 중"}

    runner = BnGridRunner(
        symbol      = sym,
        center      = float(body["center"]),
        unit        = float(body["unit"]),
        amount_usdt = float(body.get("amount_usdt", 100)),
        adj         = float(body.get("adj", 0)),
        limit       = int(body.get("limit", 4)),
        drain_mode  = bool(body.get("drain_mode", False)),
    )
    if key in binance_grid_runners:
        runner.sessions = binance_grid_runners[key].sessions
    binance_grid_runners[key] = runner
    await runner.start(binance_api, broadcast)
    return {"ok": True}


@router.post("/api/binance/grid/{symbol}/stop")
async def stop_bn_grid(symbol: str, body: dict = {}, admin: User = Depends(require_active_user)):
    sym = symbol.upper().replace("USDT", "")
    key = f"BN:{sym}USDT"
    if key not in binance_grid_runners:
        return {"ok": False, "error": "실행기 없음"}
    reason = body.get("reason", "수동종료") if body else "수동종료"
    binance_grid_runners[key].stop(reason)
    await broadcast({"type": "bn_grid_status", "data": binance_grid_runners[key].get_status()})
    return {"ok": True}


@router.get("/api/binance/grid")
async def list_bn_grids(admin: User = Depends(require_active_user)):
    return {k: r.get_status() for k, r in binance_grid_runners.items()}


@router.get("/api/binance/grid/{symbol}")
async def get_bn_grid(symbol: str, admin: User = Depends(require_active_user)):
    sym = symbol.upper().replace("USDT", "")
    key = f"BN:{sym}USDT"
    if key not in binance_grid_runners:
        return {"exchange": "binance", "symbol": sym + "USDT", "running": False,
                "orders": [], "fills": [], "sessions": [], "log": []}
    return binance_grid_runners[key].get_status()


# ── 빗썸 포트폴리오 ───────────────────────────────────────────────────
@router.get("/api/bithumb/portfolio")
async def get_bithumb_portfolio(admin: User = Depends(require_active_user)):
    async with AsyncSessionLocal() as session:
        u_res = await session.execute(select(User).where(User.id == admin.id))
        user = u_res.scalar_one_or_none()
        ak = decrypt_api_key(user.bithumb_api_key_enc or "") if user else ""
        sk = decrypt_api_key(user.bithumb_api_secret_enc or "") if user else ""

    api = BithumbAPICompat(ak, sk)
    try:
        result = await asyncio.wait_for(asyncio.to_thread(api.balance), timeout=8.0)
        if not isinstance(result, list):
            return {"ok": False, "error": str(result)}

        cash_krw   = 0.0
        coin_items = []

        for a in result:
            currency = a.get("currency", "")
            total = float(a.get("balance") or 0) + float(a.get("locked") or 0)
            if total <= 0:
                continue
            if currency == "KRW":
                cash_krw = total
            else:
                try:
                    t = await asyncio.to_thread(api.ticker, currency)
                    price = float(t["data"]["closing_price"]) if t.get("status") == "0000" else 0
                except Exception:
                    price = 0
                if price > 0:
                    coin_items.append({
                        "symbol": currency, "qty": total,
                        "price":  price, "krw": round(total * price),
                    })
                await asyncio.sleep(0.05)

        coin_krw  = sum(c["krw"] for c in coin_items)
        total_krw = cash_krw + coin_krw
        return {
            "ok": True, "total_krw": round(total_krw),
            "cash_krw": round(cash_krw), "coin_krw": round(coin_krw),
            "items": sorted(coin_items, key=lambda x: -x["krw"]),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── 디스코드 알림 ─────────────────────────────────────────────────────
DISCORD_WEBHOOK = ""   # admin이 런타임 설정
DISCORD_ALERT_ENABLED = False
_dc_task: Optional[asyncio.Task] = None
DC_INTERVAL = 1770


async def _dc_send(text: str):
    chunks = [text[i:i+1900] for i in range(0, len(text), 1900)]
    for chunk in chunks:
        try:
            await asyncio.to_thread(
                requests.post, DISCORD_WEBHOOK,
                json={"content": chunk}, timeout=5
            )
        except Exception as e:
            print(f"[DC] 전송 실패: {e}")


async def _dc_loop():
    global DISCORD_ALERT_ENABLED
    while DISCORD_ALERT_ENABLED:
        try:
            msg = await _build_portfolio_msg()
            await _dc_send(msg)
        except Exception as e:
            print(f"[DC] 루프 오류: {e}")
        for _ in range(DC_INTERVAL):
            if not DISCORD_ALERT_ENABLED:
                break
            await asyncio.sleep(1)


async def _build_portfolio_msg() -> str:
    lines = []
    lines.append(f"📊 **그리드 손익** — {datetime.now().strftime('%m/%d %H:%M')}")
    for sym, r in grid_runners.items():
        if r.running:
            s   = r.get_status()
            cur = s["cur_price"] or s["center"]
            ctr = s["start_center"]
            pnl = s["pnl_krw"]
            cd  = s["coin_delta"]
            mavg = (ctr + cur) / 2
            total = pnl + cd * mavg
            lines.append(
                f"  **{sym}** | {cur:,}원 (기준:{ctr:,})\n"
                f"  그리드 {pnl:+,.0f}원 | 코인Δ {cd:+.4f} | 통산 **{total:+,.0f}원**\n"
                f"  회차{s['round_idx']} 체결{s['fill_count']}회"
            )
    return "\n".join(lines) if lines else "실행중 그리드 없음"


@router.post("/api/alert/toggle")
async def toggle_alert(body: dict = {}, admin: User = Depends(require_active_user)):
    global DISCORD_ALERT_ENABLED, _dc_task, DISCORD_WEBHOOK
    if body.get("webhook"):
        DISCORD_WEBHOOK = body["webhook"]
    DISCORD_ALERT_ENABLED = not DISCORD_ALERT_ENABLED
    if DISCORD_ALERT_ENABLED:
        _dc_task = asyncio.create_task(_dc_loop())
    elif _dc_task:
        _dc_task.cancel()
        _dc_task = None
    return {"ok": True, "enabled": DISCORD_ALERT_ENABLED}


@router.post("/api/alert/send_now")
async def alert_send_now(admin: User = Depends(require_active_user)):
    asyncio.create_task((lambda: asyncio.to_thread(lambda: None))())
    msg = await _build_portfolio_msg()
    asyncio.create_task(_dc_send(msg))
    return {"ok": True}


# ── 15분 손익 ─────────────────────────────────────────────────────────
@router.get("/api/pnl15/{symbol}")
async def get_pnl15(symbol: str, admin: User = Depends(require_active_user)):
    sym = symbol.upper()
    async with AsyncSessionLocal() as session:
        u_res = await session.execute(select(User).where(User.id == admin.id))
        user = u_res.scalar_one_or_none()
        ak = decrypt_api_key(user.bithumb_api_key_enc or "") if user else ""
        sk = decrypt_api_key(user.bithumb_api_secret_enc or "") if user else ""

    api = BithumbAPICompat(ak, sk)
    try:
        cutoff = time.time() - 900
        result = await asyncio.to_thread(api._get, "/v1/orders", {
            "market": f"KRW-{sym}", "state": "done", "limit": 100,
        })
        if not isinstance(result, list):
            return {"ok": False, "error": str(result)}

        recent = []
        for o in result:
            try:
                ts_str = o.get("created_at", "").replace("Z", "").split("+")[0]
                ts = datetime.fromisoformat(ts_str).timestamp()
                if ts > cutoff:
                    recent.append(o)
            except Exception:
                continue

        sell_krw = buy_krw = sell_vol = buy_vol = 0.0
        for o in recent:
            vol   = float(o.get("executed_volume") or 0)
            avg_p = float(o.get("avg_price") or o.get("price") or 0)
            krw   = avg_p * vol
            if o.get("side") == "ask":
                sell_krw += krw; sell_vol += vol
            else:
                buy_krw  += krw; buy_vol  += vol

        remain_vol = buy_vol - sell_vol
        t   = await asyncio.to_thread(api.ticker, sym)
        cur = float(t["data"]["closing_price"]) if t.get("status") == "0000" else 0
        remain_krw = remain_vol * cur if cur > 0 else 0
        pnl_eval   = round(sell_krw + remain_krw - buy_krw)
        return {
            "ok": True, "symbol": sym, "pnl": pnl_eval,
            "pnl_cash": round(sell_krw - buy_krw),
            "sell_krw": round(sell_krw), "buy_krw": round(buy_krw),
            "remain_vol": round(remain_vol, 4), "cur_price": cur,
            "fill_count": len(recent),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
