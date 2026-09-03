"""
trading/premium/binance_runner.py
BinanceAPI + BnGridRunner — server.py에서 이식

변경: asyncio.to_thread 사용, 나머지 로직 원본 유지
"""

import asyncio
import hashlib
import hmac
import time
import urllib.parse
from datetime import datetime
from typing import Optional

import requests


def _bn_round_price(price: float, tick: float) -> float:
    if tick <= 0:
        return round(price, 4)
    decimals = max(0, -int(round(
        __import__('math').log10(tick))) if tick < 1 else 0)
    factor = round(price / tick) * tick
    return round(factor, decimals + 2)


def _bn_round_qty(qty: float, step: float) -> float:
    if step <= 0:
        return round(qty, 6)
    factor = int(qty / step) * step
    return round(factor, 8)


class BinanceAPI:
    """바이낸스 현물 + 선물 API — HMAC SHA256"""
    BASE = "https://api.binance.com"
    FAPI = "https://fapi.binance.com"

    def __init__(self, api_key: str = "", secret_key: str = ""):
        self.api_key    = api_key.strip()
        self.secret_key = secret_key.strip()

    def _sign(self, params: dict) -> str:
        qs = urllib.parse.urlencode(params)
        return hmac.new(self.secret_key.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self.api_key}

    def _ts(self) -> int:
        return int(time.time() * 1000)

    def ticker(self, symbol: str) -> dict:
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            sym += "USDT"
        try:
            r = requests.get(f"{self.BASE}/api/v3/ticker/price",
                             params={"symbol": sym}, timeout=5)
            if r.ok:
                return {"ok": True, "price": float(r.json()["price"]), "symbol": sym}
            return {"ok": False, "error": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def balance(self) -> dict:
        params = {"timestamp": self._ts(), "recvWindow": 5000}
        params["signature"] = self._sign(params)
        try:
            r = requests.get(f"{self.BASE}/api/v3/account",
                             params=params, headers=self._headers(), timeout=5)
            if r.ok:
                assets = {a["asset"]: {"free": float(a["free"]), "locked": float(a["locked"])}
                          for a in r.json().get("balances", [])
                          if float(a["free"]) + float(a["locked"]) > 0}
                return {"ok": True, "assets": assets}
            return {"ok": False, "error": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def place_limit(self, symbol: str, side: str, quantity: float, price: float) -> dict:
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            sym += "USDT"
        params = {
            "symbol": sym, "side": side.upper(), "type": "LIMIT",
            "timeInForce": "GTC", "quantity": quantity, "price": price,
            "timestamp": self._ts(), "recvWindow": 5000,
        }
        params["signature"] = self._sign(params)
        try:
            r = requests.post(f"{self.BASE}/api/v3/order",
                              params=params, headers=self._headers(), timeout=5)
            if r.ok:
                d = r.json()
                return {"ok": True, "order_id": str(d["orderId"]), "status": d["status"]}
            return {"ok": False, "error": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            sym += "USDT"
        params = {"symbol": sym, "orderId": order_id,
                  "timestamp": self._ts(), "recvWindow": 5000}
        params["signature"] = self._sign(params)
        try:
            r = requests.delete(f"{self.BASE}/api/v3/order",
                                params=params, headers=self._headers(), timeout=5)
            return {"ok": r.ok, "data": r.json()}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def open_orders(self, symbol: str) -> dict:
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            sym += "USDT"
        params = {"symbol": sym, "timestamp": self._ts(), "recvWindow": 5000}
        params["signature"] = self._sign(params)
        try:
            r = requests.get(f"{self.BASE}/api/v3/openOrders",
                             params=params, headers=self._headers(), timeout=5)
            if r.ok:
                return {"ok": True, "orders": r.json()}
            return {"ok": False, "error": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def futures_account(self) -> dict:
        params = {"timestamp": self._ts(), "recvWindow": 5000}
        params["signature"] = self._sign(params)
        try:
            r = requests.get(f"{self.FAPI}/fapi/v2/account",
                             params=params, headers=self._headers(), timeout=5)
            if r.ok:
                d = r.json()
                return {
                    "ok": True,
                    "total_wallet_balance":    float(d.get("totalWalletBalance", 0)),
                    "total_unrealized_profit": float(d.get("totalUnrealizedProfit", 0)),
                    "total_margin_balance":    float(d.get("totalMarginBalance", 0)),
                    "total_maint_margin":      float(d.get("totalMaintMargin", 0)),
                    "available_balance":       float(d.get("availableBalance", 0)),
                }
            return {"ok": False, "error": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def futures_positions(self) -> dict:
        params = {"timestamp": self._ts(), "recvWindow": 5000}
        params["signature"] = self._sign(params)
        try:
            r = requests.get(f"{self.FAPI}/fapi/v2/positionRisk",
                             params=params, headers=self._headers(), timeout=5)
            if r.ok:
                positions = []
                for p in r.json():
                    amt = float(p.get("positionAmt", 0))
                    if amt == 0:
                        continue
                    positions.append({
                        "symbol":         p["symbol"],
                        "side":           "LONG" if amt > 0 else "SHORT",
                        "amt":            amt,
                        "entry_price":    float(p.get("entryPrice", 0)),
                        "mark_price":     float(p.get("markPrice", 0)),
                        "unrealized_pnl": float(p.get("unRealizedProfit", 0)),
                        "leverage":       int(p.get("leverage", 1)),
                        "notional":       abs(float(p.get("notional", 0))),
                        "liq_price":      float(p.get("liquidationPrice", 0)),
                        "margin_type":    p.get("marginType", ""),
                    })
                return {"ok": True, "positions": positions}
            return {"ok": False, "error": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def spot_prices_bulk(self, symbols: list) -> dict:
        try:
            r = requests.get(f"{self.BASE}/api/v3/ticker/price", timeout=5)
            if r.ok:
                return {"ok": True, "prices": {d["symbol"]: float(d["price"]) for d in r.json()}}
            return {"ok": False}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def exchange_info(self, symbol: str) -> dict:
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            sym += "USDT"
        try:
            r = requests.get(f"{self.BASE}/api/v3/exchangeInfo",
                             params={"symbol": sym}, timeout=5)
            if r.ok:
                filters = {}
                for si in r.json().get("symbols", []):
                    for f in si.get("filters", []):
                        filters[f["filterType"]] = f
                return {"ok": True, "filters": filters}
            return {"ok": False}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def step_size(self, symbol: str) -> float:
        info = self.exchange_info(symbol)
        if not info.get("ok"):
            return 0.001
        s = float(info["filters"].get("LOT_SIZE", {}).get("stepSize", 0.001))
        return s if s > 0 else 0.001

    def tick_size(self, symbol: str) -> float:
        info = self.exchange_info(symbol)
        if not info.get("ok"):
            return 0.0001
        ts = float(info["filters"].get("PRICE_FILTER", {}).get("tickSize", 0.0001))
        return ts if ts > 0 else 0.0001

    def funding_rates(self, symbols: list) -> dict:
        try:
            r = requests.get(f"{self.FAPI}/fapi/v1/premiumIndex", timeout=5)
            if r.ok:
                result = {}
                for d in r.json():
                    sym = d.get("symbol", "")
                    if not symbols or sym in symbols:
                        result[sym] = {
                            "funding_rate":     float(d.get("lastFundingRate", 0)),
                            "next_funding_time": d.get("nextFundingTime", 0),
                            "mark_price":        float(d.get("markPrice", 0)),
                        }
                return {"ok": True, "rates": result}
            return {"ok": False, "error": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}


# ── BnGridRunner ─────────────────────────────────────────────────────────────
class BnGridRunner:
    """바이낸스 현물 USDT 그리드"""

    def __init__(self, symbol: str, center: float, unit: float,
                 amount_usdt: float, adj: float, limit: int,
                 drain_mode: bool = False):
        raw = symbol.upper().replace("USDT", "")
        self.base_asset  = raw
        self.symbol      = raw + "USDT"
        self.center      = center
        self.unit        = unit
        self.amount_usdt = amount_usdt
        self.adj         = adj
        self.limit       = limit
        self.drain_mode  = drain_mode

        self.running      = False
        self._task: Optional[asyncio.Task] = None

        self.outer_buy    = None
        self.outer_sell   = None
        self.buy_stopped  = False
        self.sell_stopped = False

        self.orders: dict        = {}
        self.active_index: dict  = {}
        self.filled_ids: set     = set()
        self.last_placed: tuple  = (None, None)
        self.last_filled: tuple  = (None, None)
        self.pending_counter: set = set()

        self.fills:      list  = []
        self.pnl_usdt:   float = 0.0
        self.coin_delta: float = 0.0

        self.sessions:   list  = []
        self.session_start     = None
        self.log:        list  = []
        self.api: Optional[BinanceAPI] = None
        self.broadcast         = None
        self.cur_price: float  = 0.0

        self._step_size: float = 0.0
        self._tick_size: float = 0.0

    def _log(self, msg: str):
        entry = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        self.log.insert(0, entry)
        self.log = self.log[:100]
        print(f"[BN-GRID][{self.symbol}] {msg}")

    def _rp(self, price: float) -> float:
        return _bn_round_price(price, self._tick_size)

    def _rq(self, qty: float) -> float:
        return _bn_round_qty(qty, self._step_size)

    def _price_offset(self, price: float, side: str) -> float:
        off = self._tick_size if self._tick_size > 0 else 0
        return price - off if side == 'buy' else price + off

    def _grid_prices(self):
        buys  = [self._rp(self.center - i * self.unit - self.adj) for i in range(1, self.limit + 1)]
        sells = [self._rp(self.center + i * self.unit + self.adj) for i in range(1, self.limit + 1)]
        return buys, sells

    async def _call(self, fn, *args, **kwargs):
        return await asyncio.wait_for(
            asyncio.to_thread(fn, *args, **kwargs),
            timeout=10.0,
        )

    async def _place_order(self, side: str, price: float, slot: int):
        price_r = self._rp(self._price_offset(price, side))
        if self.last_placed == (price_r, side):
            self._log(f"중복 차단: {side} {price_r}")
            return

        if side == 'buy' and self.drain_mode:
            ratio = price_r / (2 * (self.unit + 2 * self.adj) + price_r)
            order_usdt = self.amount_usdt * ratio
        else:
            order_usdt = self.amount_usdt

        qty = self._rq(order_usdt / price_r)
        if qty <= 0:
            self._log(f"수량 0 스킵 {side} {price_r}")
            return

        try:
            result = await self._call(
                self.api.place_limit, self.symbol,
                "BUY" if side == 'buy' else "SELL", qty, price_r
            )
        except asyncio.TimeoutError:
            self._log(f"타임아웃 {side} {price_r}")
            return

        oid = result.get("order_id") if result.get("ok") else None
        if oid:
            self.orders[oid] = {
                "order_id": oid, "side": side,
                "price": price_r, "units": qty,
                "slot": slot, "fill_count": 0, "partial": False,
            }
            self.last_placed = (price_r, side)
            self._log(f"배치 {side} {price_r} × {qty}")
        else:
            self._log(f"배치 실패 {side} {price_r}: {result.get('error','')}")

    async def _fill_missing_grids(self, cur_price: float):
        buys, sells = self._grid_prices()
        placed_buys  = {o["price"] for o in self.orders.values() if o["side"] == "buy"}
        placed_sells = {o["price"] for o in self.orders.values() if o["side"] == "sell"}
        for i, bp in enumerate(buys):
            if bp not in placed_buys and bp < cur_price:
                if self.buy_stopped:
                    break
                await self._place_order('buy', bp, i + 1)
                await asyncio.sleep(0.15)
        for i, sp in enumerate(sells):
            if sp not in placed_sells and sp > cur_price:
                if self.sell_stopped:
                    break
                await self._place_order('sell', sp, i + 1)
                await asyncio.sleep(0.15)

    async def _monitor(self):
        t = await self._call(self.api.ticker, self.symbol)
        if t.get("ok"):
            self.cur_price = t["price"]

        live = None
        for _ in range(3):
            try:
                res = await self._call(self.api.open_orders, self.symbol)
                if res.get("ok"):
                    live = res["orders"]
                    break
            except Exception:
                pass
            await asyncio.sleep(1)

        if live is None:
            self._log("미체결 조회 실패 — 스킵")
            await self.broadcast({"type": "bn_grid_status", "data": self.get_status()})
            return

        live_ids = {str(o["orderId"]) for o in live}

        counter_orders = {}
        for oid, order in list(self.orders.items()):
            if oid not in live_ids and oid not in self.filled_ids:
                if (order["price"], order["side"]) in self.pending_counter:
                    continue
                if self.last_filled == (order["price"], order["side"]):
                    self._log(f"중복 체결 스킵: {order['side']} {order['price']}")
                    continue

                self.filled_ids.add(oid)
                self.last_filled = (order["price"], order["side"])
                order["fill_count"] = order.get("fill_count", 0) + 1

                krw_val = order["price"] * order["units"]
                if order["side"] == "sell":
                    self.pnl_usdt   += krw_val
                    self.coin_delta -= order["units"]
                else:
                    self.pnl_usdt   -= krw_val
                    self.coin_delta += order["units"]

                self.fills.append({
                    "time":     datetime.now().isoformat(),
                    "side":     order["side"],
                    "price":    order["price"],
                    "units":    order["units"],
                    "krw":      krw_val,
                    "pos":      f"{order['price'] - self.center:+.4f}",
                    "pnl_cum":  round(self.pnl_usdt, 4),
                    "coin_cum": round(self.coin_delta, 8),
                })
                self.fills = self.fills[-1000:]
                self._log(f"체결 {order['side']} {order['price']} × {order['units']} | pnl={self.pnl_usdt:.4f}U")

                new_price    = self._rp(
                    order["price"] - self.unit if order["side"] == "sell"
                    else order["price"] + self.unit
                )
                counter_side = "buy" if order["side"] == "sell" else "sell"
                counter_orders[oid] = (counter_side, new_price, order["slot"])
                self.pending_counter.add((order["price"], order["side"]))

                if order["side"] == "buy":
                    if self.outer_buy and order["price"] <= self.outer_buy:
                        self.buy_stopped = True
                        self._log("매수 한도 도달")
                else:
                    if self.outer_sell and order["price"] >= self.outer_sell:
                        self.sell_stopped = True
                        self._log("매도 한도 도달")

        live_buy_prices  = {float(o["price"]) for o in live if o["side"] == "BUY"}
        live_sell_prices = {float(o["price"]) for o in live if o["side"] == "SELL"}

        for side, new_price, slot in counter_orders.values():
            await asyncio.sleep(0.2)
            if side == 'buy':
                min_sell = min(live_sell_prices, default=float('inf'))
                if min_sell - new_price < 3 * max(self.adj, 0) + self._tick_size:
                    self._log(f"매수 {new_price} 자전방지 스킵")
                else:
                    await self._place_order('buy', new_price, slot)
            else:
                max_buy = max(live_buy_prices, default=0)
                if new_price - max_buy < 3 * max(self.adj, 0) + self._tick_size:
                    self._log(f"매도 {new_price} 자전방지 스킵")
                else:
                    await self._place_order('sell', new_price, slot)

        await self.broadcast({"type": "bn_grid_status", "data": self.get_status()})

    async def _loop(self):
        self.session_start = datetime.now().isoformat()
        try:
            self._step_size = await self._call(self.api.step_size, self.symbol)
            self._tick_size = await self._call(self.api.tick_size, self.symbol)
        except Exception as e:
            self._log(f"필터 조회 실패: {e}")
            self._step_size = 0.001
            self._tick_size = 0.0001

        t = await self._call(self.api.ticker, self.symbol)
        self.cur_price = t["price"] if t.get("ok") else self.center

        await self._fill_missing_grids(self.cur_price)

        buy_prices  = [o["price"] for o in self.orders.values() if o["side"] == "buy"]
        sell_prices = [o["price"] for o in self.orders.values() if o["side"] == "sell"]
        self.outer_buy  = min(buy_prices)  if buy_prices  else None
        self.outer_sell = max(sell_prices) if sell_prices else None
        self._log(f"배치 완료 | cur:{self.cur_price} | 외곽매수:{self.outer_buy} 외곽매도:{self.outer_sell}")

        while self.running:
            await asyncio.sleep(3)
            await self._monitor()

    async def start(self, api: BinanceAPI, broadcast_fn):
        if self.running:
            return
        self.api       = api
        self.broadcast = broadcast_fn
        self.running   = True
        self._task     = asyncio.create_task(self._loop())
        self._log("시작")

    def stop(self, reason: str = "수동종료"):
        self.running = False
        if self._task:
            self._task.cancel()
        self.sessions.append({
            "started_at": self.session_start,
            "ended_at":   datetime.now().isoformat(),
            "fill_count": len(self.fills),
            "pnl_usdt":   self.pnl_usdt,
            "coin_delta": self.coin_delta,
            "reason":     reason,
        })
        self.fills           = []
        self.pnl_usdt        = 0.0
        self.coin_delta      = 0.0
        self.buy_stopped     = False
        self.sell_stopped    = False
        self.active_index    = {}
        self.filled_ids      = set()
        self.last_placed     = (None, None)
        self.last_filled     = (None, None)
        self.pending_counter = set()
        self.orders          = {}
        self.outer_buy       = None
        self.outer_sell      = None
        self.session_start   = None
        self._log(f"종료: {reason}")

    def get_status(self) -> dict:
        return {
            "exchange":    "binance",
            "symbol":      self.symbol,
            "base_asset":  self.base_asset,
            "running":     self.running,
            "center":      self.center,
            "unit":        self.unit,
            "amount_usdt": self.amount_usdt,
            "adj":         self.adj,
            "limit":       self.limit,
            "drain_mode":  self.drain_mode,
            "cur_price":   self.cur_price,
            "pnl_usdt":    self.pnl_usdt,
            "coin_delta":  self.coin_delta,
            "fill_count":  len(self.fills),
            "buy_stopped": self.buy_stopped,
            "sell_stopped":self.sell_stopped,
            "outer_buy":   self.outer_buy,
            "outer_sell":  self.outer_sell,
            "orders":      list(self.orders.values()),
            "fills":       self.fills[-50:],
            "sessions":    self.sessions,
            "log":         self.log[:20],
        }
