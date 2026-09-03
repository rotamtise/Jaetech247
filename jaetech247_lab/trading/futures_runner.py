"""
trading/futures_runner.py — v3
바이낸스 선물(FAPI) 그리드 러너
crypto_basic.py CryptoGrid2Runner 로직 그대로 이식
손익 단위: USDT
"""
from __future__ import annotations
import asyncio, hashlib, hmac, time, requests
from datetime import datetime
from typing import Optional

KST_OFF = 9 * 3600

def _now_kst() -> str:
    return datetime.utcfromtimestamp(time.time() + KST_OFF).strftime("%H:%M:%S")

def _now_kst_iso() -> str:
    return datetime.utcfromtimestamp(time.time() + KST_OFF).isoformat()


# ── 바이낸스 선물 API ─────────────────────────────────────────────
class FuturesAPI:
    FAPI = "https://fapi.binance.com"

    def __init__(self, api_key: str = "", secret_key: str = ""):
        self.api_key    = api_key.strip()
        self.secret_key = secret_key.strip()

    def _ts(self)   -> int:  return int(time.time() * 1000)
    def _hdrs(self) -> dict: return {"X-MBX-APIKEY": self.api_key}

    def _sign(self, params: dict) -> str:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(self.secret_key.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def ticker_price(self, sym: str) -> float:
        r = requests.get(f"{self.FAPI}/fapi/v1/ticker/price",
                         params={"symbol": sym}, timeout=5)
        return float(r.json().get("price", 0))

    def balance(self) -> dict:
        p = {"timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.get(f"{self.FAPI}/fapi/v2/balance",
                         params=p, headers=self._hdrs(), timeout=5)
        items = r.json()
        if not isinstance(items, list):
            return {"usdt": 0.0, "error": str(items)}
        usdt = next((float(x["balance"]) for x in items if x["asset"] == "USDT"), 0.0)
        return {"usdt": usdt}

    def open_orders(self, sym: str) -> list:
        p = {"symbol": sym, "timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.get(f"{self.FAPI}/fapi/v1/openOrders",
                         params=p, headers=self._hdrs(), timeout=5)
        data = r.json()
        return data if isinstance(data, list) else []

    def place_order(self, sym: str, side: str, qty: float, price: float) -> dict:
        p = {
            "symbol": sym, "side": side.upper(),
            "type": "LIMIT", "timeInForce": "GTC",
            "quantity": str(qty), "price": str(price),
            "timestamp": self._ts(), "recvWindow": 5000,
        }
        p["signature"] = self._sign(p)
        r = requests.post(f"{self.FAPI}/fapi/v1/order",
                          params=p, headers=self._hdrs(), timeout=5)
        return r.json()

    def cancel_order(self, sym: str, order_id) -> dict:
        p = {"symbol": sym, "orderId": order_id,
             "timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.delete(f"{self.FAPI}/fapi/v1/order",
                            params=p, headers=self._hdrs(), timeout=5)
        return r.json()

    def cancel_all(self, sym: str) -> dict:
        p = {"symbol": sym, "timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.delete(f"{self.FAPI}/fapi/v1/allOpenOrders",
                            params=p, headers=self._hdrs(), timeout=5)
        return r.json()

    def exchange_info(self, sym: str) -> dict:
        r = requests.get(f"{self.FAPI}/fapi/v1/exchangeInfo", timeout=5)
        for s in r.json().get("symbols", []):
            if s["symbol"] == sym:
                step = tick = 0.0
                for f in s.get("filters", []):
                    if f["filterType"] == "LOT_SIZE":
                        step = float(f["stepSize"])
                    if f["filterType"] == "PRICE_FILTER":
                        tick = float(f["tickSize"])
                return {"step": step, "tick": tick}
        return {"step": 0.001, "tick": 0.01}


# ── 반올림 ────────────────────────────────────────────────────────
def _round_to(val: float, unit: float) -> float:
    if unit <= 0:
        return round(val, 6)
    import math
    precision = max(0, -int(math.floor(math.log10(unit))))
    return round(round(val / unit) * unit, precision)


# ── 선물 그리드 러너 ─────────────────────────────────────────────
class FuturesGridRunner:
    CYCLE_SEC = 6

    def __init__(self, symbol: str, center: float, unit: float,
                 init_qty: float, limit: int = 6, sell_adj: float = 0.0,
                 trend: str = "중"):
        self.symbol       = symbol.upper()
        self.center       = center
        self.start_center = center
        self.unit         = unit
        self.init_qty     = init_qty   # 초기 보유 수량(계약)
        self.limit        = limit
        self.sell_adj     = sell_adj
        self.trend        = trend

        self.running      = False
        self._task: Optional[asyncio.Task] = None
        self.api: Optional[FuturesAPI] = None
        self.broadcast    = None

        # 손익
        self.pnl_usdt:   float = 0.0
        self.coin_delta: float = 0.0
        self.cur_price:  float = 0.0

        # 주문: {order_id(int): {order_id, side, price, qty, k, filled}}
        self.orders: dict = {}
        self.fills:  list = []

        # 중복 방지
        self.filled_ids: set = set()
        self.pending:    set = set()

        # 회차
        self.round_idx:        int = 1
        self.round_fill_count: int = 0
        self.round_started_at: Optional[str] = None

        # resting (회차 전환 대기)
        self.resting:           bool  = False
        self.rest_anchor_price: Optional[float] = None
        self.rest_anchor_side:  Optional[str]   = None

        # 세션
        self.sessions:     list = []
        self.session_start: Optional[str] = None
        self.fill_count:   int  = 0
        self.log:          list = []

        self._step: float = 0.0
        self._tick: float = 0.0

    def _log(self, msg: str):
        entry = f"[{_now_kst()}] {msg}"
        self.log.insert(0, entry)
        self.log = self.log[:200]

    def _rp(self, p: float) -> float:
        return round(p, 2)

    def _rq(self, q: float) -> float:
        return round(q, 2)

    def _grid_prices(self):
        """매수/매도 가격 리스트 반환"""
        buys  = [self._rp(self.center - k * self.unit)
                 for k in range(1, self.limit + 1)]
        sells = [self._rp(self.center + k * self.unit + self.sell_adj)
                 for k in range(1, self.limit + 1)]
        return buys, sells

    def _calc_qty(self, k: int, side: str) -> float:
        """비대칭 수량 계산 (기본형 동일)"""
        sc_map = {"강": 0.045, "중": 0.030, "약": 0.015}
        sc     = sc_map.get(self.trend, 0.030)
        alpha  = 0.045
        p_pen  = 0.002
        H, H0  = self.init_qty + self.coin_delta, self.init_qty
        d      = max(-1.5, min(1.5, (H - H0) / H0)) if H0 > 0 else 0
        Heff   = H0 + 0.7 * (H - H0)
        if side == "buy":
            base = max(0.0, alpha * (1 - sc * d) - k * p_pen)
        else:
            base = max(0.0, alpha * (1 + sc * d) - k * p_pen)
        qty = self._rq(base * Heff)
        return max(qty, self._step)

    async def _call(self, fn, *args, **kwargs):
        return await asyncio.wait_for(
            asyncio.to_thread(fn, *args, **kwargs), timeout=10.0)

    async def _bcast(self):
        if self.broadcast:
            await self.broadcast({"type": "lab_status", "data": self.get_status()})

    async def _cancel_all(self):
        try:
            await self._call(self.api.cancel_all, self.symbol)
            self.orders.clear()
            self._log("전체 주문 취소")
        except Exception as e:
            self._log(f"취소 실패: {e}")

    async def _place(self, side: str, price: float, k: int,
                     qty_override=None, k_override=None):
        qty = qty_override if qty_override else self._calc_qty(k, side)
        if qty <= 0:
            return
        try:
            res = await self._call(self.api.place_order,
                                   self.symbol, side, qty, price)
            if "orderId" in res:
                oid = res["orderId"]
                self.orders[oid] = {
                    "order_id": oid, "side": side,
                    "price": price, "qty": qty,
                    "k": k_override if k_override is not None else k,
                    "filled": False,
                }
            elif "code" in res:
                self._log(f"주문 오류 {side} {price}: {res.get('msg','')}")
        except Exception as e:
            self._log(f"주문 실패 {side} {price}: {e}")

    async def _setup_grid(self):
        self._log(f"격자배치 회차{self.round_idx} | 기준:{self.center} 간격:{self.unit}")
        # 현재가 조회
        try:
            self.cur_price = await self._call(self.api.ticker_price, self.symbol)
        except Exception:
            self.cur_price = self.center

        buys, sells = self._grid_prices()
        for k, bp in enumerate(buys, 1):
            if bp < self.cur_price:
                await self._place("buy", bp, k)
                await asyncio.sleep(0.2)
        for k, sp in enumerate(sells, 1):
            if sp > self.cur_price:
                await self._place("sell", sp, k)
                await asyncio.sleep(0.2)

    async def _try_resolve_resting(self) -> bool:
        """resting 상태에서 현재가가 기준 벗어나면 회차 시작"""
        ap = self.rest_anchor_price
        side = self.rest_anchor_side
        cur = self.cur_price
        u = self.unit
        if side == "buy":
            if cur >= ap - 0.8 * u:
                return False
        else:
            if cur <= ap + 0.8 * u:
                return False
        self._log(f"쉬어감 해제 → 회차{self.round_idx+1}")
        self.round_idx += 1
        self.round_fill_count = 0
        self.round_started_at = _now_kst_iso()
        self.center = self._rp(cur)
        self.resting = False
        self.rest_anchor_price = None
        self.rest_anchor_side = None
        await self._cancel_all()
        await self._setup_grid()
        return True

    async def _monitor(self):
        # 현재가
        try:
            self.cur_price = await self._call(self.api.ticker_price, self.symbol)
        except Exception as e:
            self._log(f"시세 조회 실패: {e}"); return

        # resting 상태
        if self.resting:
            if await self._try_resolve_resting():
                await self._bcast()
            return

        # 미체결 주문 목록
        try:
            open_orders = await self._call(self.api.open_orders, self.symbol)
            live_ids = {str(o["orderId"]) for o in open_orders}
        except Exception as e:
            self._log(f"주문 조회 실패: {e}"); return

        if not live_ids and self.orders:
            unfilled = [o for o in self.orders.values() if not o["filled"]]
            if unfilled:
                self._log("미체결 빈 결과 스킵"); return

        # 체결 감지
        newly_filled = []
        seen = {"buy": set(), "sell": set()}

        for oid, order in list(self.orders.items()):
            sid = str(oid)
            if sid in self.filled_ids:
                del self.orders[oid]; continue
            if order["filled"]: continue
            if sid in live_ids: continue

            pk = int(round(order["price"] * 1000))
            side = order["side"]
            if pk in seen[side]:
                self.filled_ids.add(sid); del self.orders[oid]; continue
            seen[side].add(pk)
            self.filled_ids.add(sid)
            order["filled"] = True
            newly_filled.append(order)
            self._log(f"{'매도' if side=='sell' else '매수'} 체결 ${order['price']} × {order['qty']}")

        for o in newly_filled:
            self.orders.pop(o["order_id"], None)

        if not newly_filled:
            await self._bcast(); return

        # 손익 계산
        for order in newly_filled:
            p = order["price"]; q = order["qty"]; s = order["side"]
            usdt = p * q
            if s == "sell":
                self.pnl_usdt  += usdt
                self.coin_delta -= q
            else:
                self.pnl_usdt  -= usdt
                self.coin_delta += q
            self.round_fill_count += 1
            self.fill_count += 1
            diff = round(p - self.start_center, 4)
            self.fills.append({
                "time":      _now_kst_iso(),
                "side":      s,
                "price":     p,
                "qty":       q,
                "usdt":      usdt,
                "k":         order.get("k", 0),
                "round":     self.round_idx,
                "round_fill": self.round_fill_count,
                "pos":       f"기준{'+' if diff >= 0 else ''}{diff}",
                "pnl_cum":   round(self.pnl_usdt, 4),
                "coin_cum":  round(self.coin_delta, 4),
            })
            self.fills = self.fills[-1000:]

        # coin_delta 한계 체크
        ratio = (self.coin_delta / self.init_qty) if self.init_qty > 0 else self.coin_delta
        if ratio < -0.95:
            self._log(f"coin_delta 한계 종료({ratio:.4f})")
            await self._cancel_all()
            self.sessions.append({
                "round": self.round_idx,
                "started_at": self.round_started_at,
                "ended_at": _now_kst_iso(),
                "fill_count": self.round_fill_count,
                "pnl_usdt": self.pnl_usdt,
                "reason": f"한계({ratio:.4f})"
            })
            self.running = False
            await self._bcast(); return

        # 회차 완료 체크
        if self.round_fill_count >= self.limit:
            cur = self.cur_price or self.center
            anchor = min(newly_filled, key=lambda o: abs(o["price"] - cur))
            ag = anchor["price"] - self.sell_adj if anchor["side"] == "sell" else anchor["price"]
            u = self.unit
            mode = None; nc = None

            if anchor["side"] == "buy":
                if cur < ag - 2 * u:        mode, nc = "follow", cur
                elif ag - 2*u <= cur <= ag - 0.8*u: mode = "rest"
                else:                        mode, nc = "normal", ag
            else:
                if cur > ag + 2 * u:        mode, nc = "follow", cur
                elif ag + 0.8*u <= cur <= ag + 2*u: mode = "rest"
                else:                        mode, nc = "normal", ag

            self.sessions.append({
                "round": self.round_idx,
                "started_at": self.round_started_at,
                "ended_at": _now_kst_iso(),
                "fill_count": self.round_fill_count,
                "pnl_usdt": round(self.pnl_usdt, 4),
                "mode": mode,
                "reason": f"{self.limit}회 체결({mode})"
            })
            await self._cancel_all()

            if mode == "rest":
                self.resting = True
                self.rest_anchor_price = ag
                self.rest_anchor_side  = anchor["side"]
                self._log(f"쉬어감 앵커:${ag}")
            else:
                self.round_idx        += 1
                self.round_fill_count  = 0
                self.round_started_at  = _now_kst_iso()
                self.center            = self._rp(nc)
                self.resting           = False
                self.rest_anchor_price = None
                self.rest_anchor_side  = None
                self._log(f"회차→{self.round_idx}({mode}) center=${self.center}")
                await self._setup_grid()

            await self._bcast(); return

        # 카운터 주문
        cq = {}
        for order in newly_filled:
            p  = order["price"]
            s  = order["side"]
            q  = order["qty"]
            ko = order.get("k", 0)
            gp = p - self.sell_adj if s == "sell" else p

            if s == "sell":
                np   = self._rp(gp - self.unit)
                gmin = self._rp(self.center - self.limit * self.unit)
                if np < gmin: continue
                nk   = max(1, min(self.limit, round((self.center - np) / self.unit)))
                near = abs(np - self.center) <= 0.5 * self.unit
                cq[int(round(np * 1000))] = ("buy",  np, nk,
                                              q if near else None,
                                              ko if near else None)
            else:
                np   = self._rp(gp + self.unit + self.sell_adj)
                gmax = self._rp(self.center + self.limit * self.unit + self.sell_adj)
                if np > gmax: continue
                nk   = max(1, min(self.limit, round((np - self.center) / self.unit)))
                near = abs(np - self.center) <= 0.5 * self.unit
                cq[int(round(np * 1000))] = ("sell", np, nk,
                                              q if near else None,
                                              ko if near else None)

        for cs, cp, ck, cu, cko in cq.values():
            await asyncio.sleep(0.2)
            await self._place(cs, cp, ck, cu, cko)

        await self._bcast()

    async def _loop(self):
        self.session_start    = _now_kst_iso()
        self.round_started_at = self.session_start
        try:
            await self._setup_grid()
            while self.running:
                await asyncio.sleep(self.CYCLE_SEC)
                if self.running:
                    await self._monitor()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log(f"루프 오류: {e}")
        finally:
            self.running = False
            self._log("그리드 종료")

    async def start(self, api: FuturesAPI, broadcast=None):
        if self.running:
            return
        self.api       = api
        self.broadcast = broadcast
        self.running   = True
        self._task     = asyncio.create_task(self._loop())

    def stop(self, reason: str = "수동종료"):
        self.running = False
        if self._task:
            self._task.cancel()
        self.sessions.append({
            "round":      self.round_idx,
            "started_at": self.round_started_at,
            "ended_at":   _now_kst_iso(),
            "fill_count": self.round_fill_count,
            "pnl_usdt":   round(self.pnl_usdt, 4),
            "reason":     reason,
        })
        self.round_fill_count  = 0
        self.round_started_at  = None
        self.resting           = False
        self.rest_anchor_price = None
        self.rest_anchor_side  = None
        self.filled_ids        = set()
        self.pending           = set()
        self.orders            = {}
        self._log(f"정지: {reason}")

    def get_status(self) -> dict:
        elapsed_h = 0.0
        if self.session_start:
            try:
                elapsed_h = (time.time() - time.mktime(
                    time.strptime(self.session_start[:19],
                                  "%Y-%m-%dT%H:%M:%S"))) / 3600
            except Exception:
                pass

        mavg     = (self.start_center + self.cur_price) / 2 if self.cur_price else self.center
        grid_pnl = round(self.pnl_usdt + self.coin_delta * mavg, 4)
        eval_pnl = round(self.coin_delta * (self.cur_price - mavg), 4) if self.cur_price else 0

        return {
            "running":          self.running,
            "symbol":           self.symbol,
            "center":           self.center,
            "start_center":     self.start_center,
            "unit":             self.unit,
            "limit":            self.limit,
            "sell_adj":         self.sell_adj,
            "trend":            self.trend,
            "cur_price":        self.cur_price,
            "pnl_usdt":         round(self.pnl_usdt, 4),
            "coin_delta":       round(self.coin_delta, 4),
            "grid_pnl":         grid_pnl,
            "eval_pnl":         eval_pnl,
            "total_pnl":        round(grid_pnl + eval_pnl, 4),
            "fill_count":       self.fill_count,
            "round_idx":        self.round_idx,
            "round_fill_count": self.round_fill_count,
            "resting":          self.resting,
            "elapsed_h":        round(elapsed_h, 2),
            "orders": [
                {"price": o["price"], "side": o["side"],
                 "qty": o["qty"], "k": o["k"]}
                for o in self.orders.values() if not o["filled"]
            ],
            "fills":    self.fills[-50:],
            "sessions": self.sessions,
            "log":      self.log[:30],
        }
