"""
trading/futures_runner.py
바이낸스 선물(FAPI) 그리드 러너 — 다중 테스터 지원
손익은 USDT 기준
"""
from __future__ import annotations
import asyncio, hashlib, hmac, time, requests
from datetime import datetime
from typing import Optional

KST_OFF = 9 * 3600

def _now_kst() -> str:
    # 체결 시각에 날짜까지 포함하도록 수정
    return datetime.utcfromtimestamp(time.time() + KST_OFF).strftime("%Y-%m-%d %H:%M:%S (KST)")

# ── 바이낸스 선물 API ─────────────────────────────────────────────────
class FuturesAPI:
    FAPI = "https://fapi.binance.com"

    def __init__(self, api_key: str = "", secret_key: str = ""):
        self.api_key    = api_key.strip()
        self.secret_key = secret_key.strip()

    def _ts(self)   -> int:   return int(time.time() * 1000)
    def _hdrs(self) -> dict:  return {"X-MBX-APIKEY": self.api_key}
    def _sign(self, params: dict) -> str:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(self.secret_key.encode(), qs.encode(), hashlib.sha256).hexdigest()

    # 현재가
    def ticker_price(self, sym: str) -> float:
        r = requests.get(f"{self.FAPI}/fapi/v1/ticker/price",
                         params={"symbol": sym}, timeout=5)
        return float(r.json().get("price", 0))

    # 잔고
    def balance(self) -> dict:
        p = {"timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.get(f"{self.FAPI}/fapi/v2/balance",
                         params=p, headers=self._hdrs(), timeout=5)
        items = r.json()
        usdt = next((float(x["balance"]) for x in items if x["asset"] == "USDT"), 0.0)
        return {"usdt": usdt}

    # 포지션
    def positions(self, sym: str) -> dict:
        p = {"symbol": sym, "timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.get(f"{self.FAPI}/fapi/v2/positionRisk",
                         params=p, headers=self._hdrs(), timeout=5)
        data = r.json()
        if isinstance(data, list) and data:
            d = data[0]
            return {
                "amt":   float(d.get("positionAmt", 0)),
                "entry": float(d.get("entryPrice", 0)),
                "pnl":   float(d.get("unRealizedProfit", 0)),
            }
        return {"amt": 0, "entry": 0, "pnl": 0}

    # 주문 (LIMIT)
    def place_order(self, sym: str, side: str, qty: float, price: float) -> dict:
        p = {
            "symbol":       sym,
            "side":         side.upper(),
            "type":         "LIMIT",
            "timeInForce":  "GTC",
            "quantity":     str(qty),
            "price":        str(price),
            "timestamp":    self._ts(),
            "recvWindow":   5000,
        }
        p["signature"] = self._sign(p)
        r = requests.post(f"{self.FAPI}/fapi/v1/order",
                          params=p, headers=self._hdrs(), timeout=5)
        return r.json()

    # 주문 취소
    def cancel_order(self, sym: str, order_id: int) -> dict:
        p = {"symbol": sym, "orderId": order_id,
             "timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.delete(f"{self.FAPI}/fapi/v1/order",
                            params=p, headers=self._hdrs(), timeout=5)
        return r.json()

    # 열린 주문 목록 (에러 발생 시 강제로 빈 리스트를 반환하지 않도록 수정)
    def open_orders(self, sym: str) -> list:
        p = {"symbol": sym, "timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.get(f"{self.FAPI}/fapi/v1/openOrders",
                         params=p, headers=self._hdrs(), timeout=5)
        data = r.json()
        if isinstance(data, list):
            return data
        else:
            raise ValueError(f"정상적인 주문 목록을 받지 못했습니다. 응답: {data}")

    # 전체 취소
    def cancel_all(self, sym: str) -> dict:
        p = {"symbol": sym, "timestamp": self._ts(), "recvWindow": 5000}
        p["signature"] = self._sign(p)
        r = requests.delete(f"{self.FAPI}/fapi/v1/allOpenOrders",
                            params=p, headers=self._hdrs(), timeout=5)
        return r.json()

    # exchange info (step/tick size)
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


# ── 반올림 헬퍼 ──────────────────────────────────────────────────────
def _round_to(val: float, unit: float) -> float:
    if unit <= 0:
        return round(val, 6)
    import math
    precision = max(0, -int(math.floor(math.log10(unit))))
    return round(round(val / unit) * unit, precision)


# ── 선물 그리드 러너 ─────────────────────────────────────────────────
class FuturesGridRunner:
    CYCLE_SEC = 6

    def __init__(self, symbol: str, center: float, unit: float,
                 base_qty: float, max_dev_qty: float, limit: int = 5, 
                 sell_adj: float = 0.0, trend: str = "중"):
        self.symbol      = symbol.upper()
        self.center      = center
        self.unit        = unit
        self.base_qty    = base_qty      
        self.max_dev_qty = max_dev_qty   
        self.limit       = limit
        self.sell_adj    = sell_adj
        self.trend       = trend

        self.running   = False
        self._task: Optional[asyncio.Task] = None

        self.api: Optional[FuturesAPI] = None
        self.broadcast = None

        self.pnl_usdt:   float = 0.0   
        self.coin_delta: float = 0.0   
        self.start_center: float = center
        self.cur_price:  float  = 0.0

        self.orders: dict = {}         
        self.fills:  list = []

        self.sessions: list = []
        self.session_start: Optional[str] = None
        self.round_idx:  int = 1
        self.fill_count: int = 0
        self.log: list = []

        self._step: float = 0.0
        self._tick: float = 0.0
        self._tick_count: int = 0

    def _log(self, msg: str):
        entry = f"[{_now_kst()}] {msg}"
        self.log.insert(0, entry)
        self.log = self.log[:200]

    def _rp(self, p: float) -> float:
        return _round_to(p, self._tick)

    def _rq(self, q: float) -> float:
        return _round_to(q, self._step)

    def _qty(self, k: int, side: str) -> float:
        sc_map = {"강": 0.045, "중": 0.030, "약": 0.015}
        sc = sc_map.get(self.trend, 0.030)
        p_ratio = 0.002 / 0.045

        d = self.coin_delta / self.max_dev_qty if self.max_dev_qty > 0 else 0
        d = max(-1.5, min(1.5, d))

        if side == "buy":
            base = max(0.0, self.base_qty * (1 - sc * d) - k * (self.base_qty * p_ratio))
        else:
            base = max(0.0, self.base_qty * (1 + sc * d) - k * (self.base_qty * p_ratio))
            
        qty = self._rq(base)
        return max(qty, self._step)

    async def _call(self, fn, *args, **kwargs):
        return await asyncio.wait_for(
            asyncio.to_thread(fn, *args, **kwargs), timeout=10.0)

    async def _setup(self):
        info = await self._call(self.api.exchange_info, self.symbol)
        self._step = info["step"]
        self._tick = info["tick"]
        self._log(f"거래소 정보: step={self._step}, tick={self._tick}")
        await self._cancel_all_orders()
        await self._place_grid()

    async def _cancel_all_orders(self):
        try:
            await self._call(self.api.cancel_all, self.symbol)
            self.orders.clear()
            self._log("전체 주문 취소")
        except Exception as e:
            self._log(f"주문 취소 실패: {e}")

    async def _place_grid(self):
        for k in range(1, self.limit + 1):
            buy_p  = self._rp(self.center - k * self.unit)
            sell_p = self._rp(self.center + k * self.unit + self.sell_adj)

            for side, price in [("buy", buy_p), ("sell", sell_p)]:
                qty = self._qty(k, side)
                if qty <= 0:
                    continue
                try:
                    res = await self._call(self.api.place_order,
                                           self.symbol, side, qty, price)
                    if "orderId" in res:
                        self.orders[price] = {
                            "id": res["orderId"], "side": side, "qty": qty, "k": k
                        }
                except Exception as e:
                    self._log(f"주문 실패 {side} {price}: {e}")

        self._log(f"그리드 배치 완료 ({len(self.orders)}건)")

    async def _monitor(self):
        try:
            price = await self._call(self.api.ticker_price, self.symbol)
            self.cur_price = price
        except Exception as e:
            self._log(f"시세 조회 실패: {e}")
            return

        try:
            open_orders = await self._call(self.api.open_orders, self.symbol)
            open_ids = {o["orderId"] for o in open_orders}
        except Exception as e:
            self._log(f"주문 조회 실패: {e}")
            return

        filled = {p: o for p, o in self.orders.items()
                  if o["id"] not in open_ids}

        for price, order in filled.items():
            side = order["side"]
            qty  = order["qty"]
            usdt = price * qty

            if side == "buy":
                self.pnl_usdt  -= usdt
                self.coin_delta += qty
            else:
                self.pnl_usdt  += usdt
                self.coin_delta -= qty

            self.fill_count += 1
            
            # 1. 누적 손익(pnl_cum) 표기 시 자산 변동분을 함께 반영하도록 수정
            self.fills.append({
                "time":     _now_kst(),
                "side":     side,
                "price":    price,
                "qty":      qty,
                "usdt":     usdt,
                "pnl_cum":  round(self.pnl_usdt + (self.coin_delta * price), 4),
                "coin_cum": round(self.coin_delta, 4),
            })
            self.fills = self.fills[-1000:]
            self._log(f"{'매수' if side=='buy' else '매도'} 체결 ${price} × {qty} = ${usdt:.2f}")
            del self.orders[price]

            if self.max_dev_qty > 0 and abs(self.coin_delta) >= self.max_dev_qty:
                self._log(f"최대이탈수량 도달! (현재 누적: {round(self.coin_delta, 4)} / 최대 허용: {self.max_dev_qty})")
                self.stop("최대이탈수량 도달 (자동정지)")
                if self.broadcast:
                    await self.broadcast()
                return

            # 3. 매도 체결 시 카운터 매수 주문 위치를 원래 자리로 돌려놓기 위해 수정
            counter_side  = "sell" if side == "buy" else "buy"
            counter_price = self._rp(
                price + self.unit + self.sell_adj if side == "buy"
                else price - (self.unit + self.sell_adj)) 
            counter_qty = self._qty(order["k"], counter_side)
            
            try:
                res = await self._call(self.api.place_order,
                                       self.symbol, counter_side,
                                       counter_qty, counter_price)
                if "orderId" in res:
                    self.orders[counter_price] = {
                        "id": res["orderId"], "side": counter_side,
                        "qty": counter_qty, "k": order["k"]
                    }
            except Exception as e:
                self._log(f"카운터 주문 실패: {e}")

        if filled and self.broadcast:
            await self.broadcast()

    async def _loop(self):
        self.session_start = _now_kst()
        self._tick_count = 0
        try:
            await self._setup()
            while self.running:
                await asyncio.sleep(self.CYCLE_SEC)
                self._tick_count += 1
                if self.running:
                    await self._monitor()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log(f"루프 오류: {e}")
        finally:
            self.running = False
            self._log("그리드 루프 종료")

    async def start(self, api: FuturesAPI, broadcast=None):
        if self.running:
            return
        self.api       = api
        self.broadcast = broadcast
        self.running   = True
        self._task     = asyncio.create_task(self._loop())

    def stop(self, reason: str = "수동종료"):
        if not self.running:
            return
        self.running = False
        if self._task:
            self._task.cancel()
        self.sessions.append({
            "started_at": self.session_start,
            "ended_at":   _now_kst(),
            "fill_count": self.fill_count,
            "pnl_usdt":   round(self.pnl_usdt, 4),
            "reason":     reason,
        })
        self._log(f"정지: {reason}")

    def get_status(self) -> dict:
        mavg = (self.start_center + self.cur_price) / 2 if self.cur_price else self.center
        grid_pnl = round(self.pnl_usdt + self.coin_delta * mavg, 4)
        eval_pnl = round(self.coin_delta * (self.cur_price - mavg), 4) if self.cur_price else 0

        return {
            "running":       self.running,
            "symbol":        self.symbol,
            "center":        self.center,
            "unit":          self.unit,
            "base_qty":      self.base_qty,
            "max_dev_qty":   self.max_dev_qty,
            "limit":         self.limit,
            "sell_adj":      self.sell_adj,
            "trend":         self.trend,
            "cur_price":     self.cur_price,
            "pnl_usdt":      round(self.pnl_usdt, 4),
            "coin_delta":    round(self.coin_delta, 4),
            "grid_pnl":      grid_pnl,
            "eval_pnl":      eval_pnl,
            "total_pnl":     round(grid_pnl + eval_pnl, 4),
            "fill_count":    self.fill_count,
            "orders":        [
                {"price": p, "side": o["side"], "qty": o["qty"], "k": o["k"]}
                for p, o in self.orders.items()
            ],
            "fills":         self.fills[-100:],
            "sessions":      self.sessions,
            "log":           self.log[:30],
        }