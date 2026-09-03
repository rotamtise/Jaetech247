"""
trading/channels/crypto_basic.py
Bithumb REST API grid trading for channels 07~30.

Grid strategy:
  - Divides price range into N equal intervals (grid levels)
  - Places buy orders below current price, sell orders above
  - On fill → immediately places inverse order at next grid level
  - Profit = grid_gap * qty per filled pair

API Rate limit defense: handled by engine time-staggering (offset = ch_id % 6)
"""
import hashlib
import hmac
import time
import urllib.parse
from typing import Optional

import httpx

from app.core.config import settings
from trading.channels.base import BaseChannel


class BithumbClient:
    """Minimal async Bithumb Private/Public API client."""

    BASE = "https://api.bithumb.com"

    def __init__(self, api_key: str, api_secret: str, client: httpx.AsyncClient):
        self._key = api_key
        self._secret = api_secret
        self._client = client

    def _sign(self, endpoint: str, params: dict) -> dict:
        """Generate Bithumb HMAC-SHA512 signature."""
        nonce = str(int(time.time() * 1000))
        params_str = chr(0).join([endpoint, urllib.parse.urlencode(params), nonce])
        sig = hmac.new(
            self._secret.encode("utf-8"),
            params_str.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()
        return {
            "Api-Key": self._key,
            "Api-Sign": sig,
            "Api-Nonce": nonce,
            "Content-Type": "application/x-www-form-urlencoded",
        }

    async def get_ticker(self, symbol: str) -> dict:
        """GET /public/ticker/{symbol}_KRW"""
        r = await self._client.get(f"{self.BASE}/public/ticker/{symbol}_KRW")
        r.raise_for_status()
        return r.json()

    async def get_orderbook(self, symbol: str, count: int = 10) -> dict:
        r = await self._client.get(
            f"{self.BASE}/public/orderbook/{symbol}_KRW",
            params={"count": count}
        )
        r.raise_for_status()
        return r.json()

    async def get_balance(self, currency: str = "ALL") -> dict:
        endpoint = "/info/balance"
        params = {"currency": currency}
        headers = self._sign(endpoint, params)
        r = await self._client.post(
            f"{self.BASE}{endpoint}",
            data=params,
            headers=headers,
        )
        r.raise_for_status()
        return r.json()

    async def get_orders(self, symbol: str, status: str = "0") -> dict:
        """status: 0=미체결, 1=체결완료"""
        endpoint = "/info/orders"
        params = {"order_currency": symbol, "payment_currency": "KRW", "order_status": status}
        headers = self._sign(endpoint, params)
        r = await self._client.post(f"{self.BASE}{endpoint}", data=params, headers=headers)
        r.raise_for_status()
        return r.json()

    async def place_order(self, symbol: str, side: str, price: float, qty: float) -> dict:
        """side: 'bid' = buy, 'ask' = sell"""
        endpoint = "/trade/place"
        params = {
            "order_currency": symbol,
            "payment_currency": "KRW",
            "type": side,
            "price": str(int(price)),
            "units": f"{qty:.8f}",
        }
        headers = self._sign(endpoint, params)
        r = await self._client.post(f"{self.BASE}{endpoint}", data=params, headers=headers)
        r.raise_for_status()
        return r.json()

    async def cancel_order(self, order_id: str, symbol: str, side: str) -> dict:
        endpoint = "/trade/cancel"
        params = {
            "type": side,
            "order_id": order_id,
            "order_currency": symbol,
            "payment_currency": "KRW",
        }
        headers = self._sign(endpoint, params)
        r = await self._client.post(f"{self.BASE}{endpoint}", data=params, headers=headers)
        r.raise_for_status()
        return r.json()


# ── Grid State ────────────────────────────────────────────────────────────────
class GridState:
    def __init__(self):
        self.levels: list[float] = []           # sorted price levels
        self.open_orders: dict[str, dict] = {}  # order_id -> {level, side, qty}
        self.initialized = False


class CryptoBasicChannel(BaseChannel):
    """
    Channels 07~30: single-coin Bithumb grid trading.
    Strategy params (from channel.strategy_params JSON):
      - grid_upper: upper price boundary
      - grid_lower: lower price boundary
      - grid_count: number of grids (default 10)
      - order_qty: qty per grid order (in coin units)
    """

    def __init__(self, channel_id: int, state):
        super().__init__(channel_id, state)
        self._bithumb: Optional[BithumbClient] = None
        self._grid = GridState()
        self._params: dict = {}
        self._tick_count = 0

    def set_credentials(self, creds: dict):
        super().set_credentials(creds)

    async def on_start(self):
        await super().on_start()
        client = await self._get_client()
        self._bithumb = BithumbClient(
            self.creds.get("api_key", ""),
            self.creds.get("api_secret", ""),
            client,
        )
        # Load strategy params
        import json
        try:
            from app.models.database import AsyncSessionLocal, Channel
            from sqlalchemy import select
            async with AsyncSessionLocal() as session:
                res = await session.execute(
                    select(Channel).where(Channel.channel_id == self.channel_id)
                )
                ch = res.scalar_one_or_none()
                if ch and ch.strategy_params:
                    self._params = json.loads(ch.strategy_params)
        except Exception as e:
            self.log(f"파라미터 로드 실패: {e}", "WARN")

        self.log(f"[CH{self.channel_id:02d}] Bithumb 그리드 초기화 | 심볼: {self.state.symbol}", "INFO")

    async def tick(self):
        """Called every 6s at time-staggered offset."""
        self._tick_count += 1
        symbol = self.state.symbol
        if not symbol:
            return

        try:
            # 1. Fetch current price
            ticker = await self._bithumb.get_ticker(symbol)
            price = float(ticker["data"]["closing_price"])
            self.state.current_price = price

            # 2. First tick: initialize grid
            if not self._grid.initialized:
                await self._init_grid(price)
                return

            # 3. Check filled orders (every tick)
            await self._check_fills()

            # 4. Log every 10 ticks (~60s)
            if self._tick_count % 10 == 0:
                self.log(
                    f"현재가 {price:,.0f}원 | 실현손익 {self.state.realized_pnl:+,.0f}원 | "
                    f"거래횟수 {self.state.trade_count}",
                    "INFO"
                )

        except httpx.HTTPError as e:
            self.log(f"API 오류: {e}", "ERROR")
        except Exception as e:
            self.log(f"틱 오류: {e}", "ERROR")

    async def _init_grid(self, current_price: float):
        """Set up grid levels and place initial buy orders below current price."""
        upper = self._params.get("grid_upper", current_price * 1.05)
        lower = self._params.get("grid_lower", current_price * 0.95)
        count = int(self._params.get("grid_count", 10))
        qty = float(self._params.get("order_qty", 0.001))

        gap = (upper - lower) / count
        self._grid.levels = [lower + i * gap for i in range(count + 1)]
        self._grid.initialized = True

        self.log(
            f"그리드 초기화: {lower:,.0f}~{upper:,.0f}원, {count}단계, "
            f"간격 {gap:,.0f}원, 주문수량 {qty}",
            "INFO"
        )

        # Place buy orders at levels below current price
        for level in self._grid.levels:
            if level < current_price * 0.999:
                try:
                    resp = await self._bithumb.place_order(
                        self.state.symbol, "bid", level, qty
                    )
                    if resp.get("status") == "0000":
                        oid = resp["order_id"]
                        self._grid.open_orders[oid] = {
                            "level": level, "side": "bid", "qty": qty
                        }
                        self.log(f"매수 주문 [{level:,.0f}원] → {oid[:8]}…", "INFO")
                except Exception as e:
                    self.log(f"주문 실패 [{level:,.0f}원]: {e}", "WARN")

    async def _check_fills(self):
        """Poll completed orders and place counter-orders."""
        try:
            resp = await self._bithumb.get_orders(self.state.symbol, status="1")
            if resp.get("status") != "0000":
                return
            filled_orders = resp.get("data", [])
        except Exception:
            return

        qty = float(self._params.get("order_qty", 0.001))
        gap = 0.0
        if len(self._grid.levels) >= 2:
            gap = self._grid.levels[1] - self._grid.levels[0]

        for order in filled_orders:
            oid = str(order.get("order_id", ""))
            if oid not in self._grid.open_orders:
                continue

            info = self._grid.open_orders.pop(oid)
            fill_price = float(order.get("price", info["level"]))
            side = info["side"]
            self.state.trade_count += 1

            if side == "bid":   # buy filled → place sell one level up
                sell_price = fill_price + gap
                self.state.position_qty += qty
                self.state.position_avg_price = (
                    (self.state.position_avg_price * (self.state.position_qty - qty) + fill_price * qty)
                    / self.state.position_qty
                )
                self.log(f"✅ 매수 체결 [{fill_price:,.0f}원] → 매도 예약 [{sell_price:,.0f}원]", "TRADE")
                try:
                    resp2 = await self._bithumb.place_order(
                        self.state.symbol, "ask", sell_price, qty
                    )
                    if resp2.get("status") == "0000":
                        self._grid.open_orders[resp2["order_id"]] = {
                            "level": sell_price, "side": "ask", "qty": qty
                        }
                except Exception as e:
                    self.log(f"매도 예약 실패: {e}", "WARN")

            else:               # sell filled → place buy one level down + realize profit
                buy_back = fill_price - gap
                profit = gap * qty
                self.state.realized_pnl += profit
                self.state.position_qty = max(0.0, self.state.position_qty - qty)
                self.log(
                    f"✅ 매도 체결 [{fill_price:,.0f}원] | 이익 +{profit:,.0f}원 "
                    f"| 누적 {self.state.realized_pnl:,.0f}원",
                    "TRADE"
                )
                self.state.last_order = {
                    "side": "SELL", "price": fill_price, "qty": qty, "profit": profit
                }
                try:
                    resp2 = await self._bithumb.place_order(
                        self.state.symbol, "bid", buy_back, qty
                    )
                    if resp2.get("status") == "0000":
                        self._grid.open_orders[resp2["order_id"]] = {
                            "level": buy_back, "side": "bid", "qty": qty
                        }
                except Exception as e:
                    self.log(f"재매수 예약 실패: {e}", "WARN")
