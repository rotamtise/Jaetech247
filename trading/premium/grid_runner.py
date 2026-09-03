"""
trading/premium/grid_runner.py
Grid2Runner — 빗썸 KRW 동적 그리드 엔진 (server.py에서 이식)

변경 사항:
  - BithumbAPI 인스턴스는 외부(PremiumChannel)에서 주입 (self.api)
  - broadcast 콜백도 외부 주입 (SaaS WebSocket 브로드캐스트)
  - asyncio.get_event_loop().run_in_executor → asyncio.to_thread 로 현대화
  - 나머지 알고리즘 로직은 원본 100% 유지
"""

import asyncio
import math
from datetime import datetime
from typing import Optional


# ── 가격 단위 ─────────────────────────────────────────────────────────
def tick_unit(price: float) -> float:
    if price >= 2_000_000: return 1000
    if price >= 1_000_000: return 500
    if price >= 500_000:   return 100
    if price >= 100_000:   return 50
    if price >= 10_000:    return 10
    if price >= 1_000:     return 1
    if price >= 100:       return 0.1
    if price >= 10:        return 0.01
    return 0.001


def round_price(price: float) -> float:
    u = tick_unit(price)
    if price < 100:
        return round(round(price / u) * u, 2)
    return int(round(price / u) * u)


# 추세 추종 강도 → skew 계수
TREND_SKEW = {"강": 0.045, "중": 0.030, "약": 0.015}


class Grid2Runner:
    """
    동적 그리드 (무한 회차)
    수량 모델:
      H        = init_h + coin_delta
      norm     = H / init_h
      base_qty = 0.045
      skew     = TREND_SKEW[trend] × (norm - 1)
      penalty  = k × 0.002
      ratio    = base_qty ∓ skew - penalty  (매수: -skew, 매도: +skew)
      H_eff    = init_h + 0.7 × coin_delta
      units    = ratio × H_eff
    """

    def __init__(self, symbol: str, center: float, unit: float,
                 init_h: float = 1.0, limit: int = 6,
                 drain_mode: bool = False, sell_adj: float = 0.0,
                 trend: str = "중"):
        self.symbol       = symbol.upper()
        self.center       = center
        self.start_center = center   # 재시작해도 불변 — 평가 기준점
        self.unit         = unit
        self.init_h       = float(init_h)
        self.limit        = int(limit)
        self.drain_mode   = drain_mode
        self.sell_adj     = float(sell_adj)
        self.trend        = trend if trend in TREND_SKEW else "중"

        self.running = False
        self._task: Optional[asyncio.Task] = None

        # 회차
        self.round_idx        = 1
        self.round_fill_count = 0
        self.round_started_at: Optional[str] = None

        # 쉬어감
        self.resting           = False
        self.rest_anchor_price: Optional[float] = None
        self.rest_anchor_side:  Optional[str]   = None

        self.orders: dict      = {}
        self.filled_uuids: set = set()
        self.pending: set      = set()

        self.fills:      list  = []
        self.pnl_krw:    float = 0.0
        self.coin_delta: float = 0.0

        self.sessions: list = []
        self.session_start: Optional[str] = None

        self.log: list = []
        self.api       = None    # BithumbAPI 인스턴스 (외부 주입)
        self.broadcast = None    # async broadcast fn
        self.cur_price: float = 0.0

    # ── 내부 도우미 ───────────────────────────────────────────────────
    def _log(self, msg: str):
        entry = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        self.log.insert(0, entry)
        self.log = self.log[:100]
        print(f"[GRID2][{self.symbol}] {msg}")

    def _round_price(self, price: float) -> float:
        return round_price(price)

    def _grid_prices(self):
        buys  = [self._round_price(self.center - k * self.unit)
                 for k in range(1, self.limit + 1)]
        sells = [self._round_price(self.center + k * self.unit)
                 for k in range(1, self.limit + 1)]
        return buys, sells

    def _calc_units(self, side: str, price_r: float,
                    k: int = 1,
                    counter_units: Optional[float] = None,
                    counter_k_old: Optional[int] = None) -> tuple:
        # 카운터 경로
        if counter_units is not None:
            if counter_k_old is not None and counter_k_old > 0:
                boost = 1.0 + (counter_k_old * 0.002) / 0.045
                units = counter_units * boost
            else:
                units = counter_units
            units = max(0.0001, round(units, 8))
            return units, price_r * units

        H    = max(0.0, self.init_h + self.coin_delta)
        norm = H / self.init_h if self.init_h > 0 else 1.0
        base_qty   = 0.045
        skew_coeff = TREND_SKEW.get(self.trend, 0.030)
        skew       = skew_coeff * (norm - 1.0)
        penalty    = k * 0.002

        if side == 'buy':
            ratio = base_qty * 1.006 - skew - penalty
        else:
            ratio = base_qty + skew - penalty

        H_eff = self.init_h + 0.7 * self.coin_delta
        units = ratio * H_eff

        if self.drain_mode:
            half = 0.5 * self.unit
            if side == 'buy' and price_r < self.center - half:
                units *= (price_r / self.center) ** 4
            elif side == 'sell' and price_r > self.center + half:
                units *= (self.center / price_r) ** 5

        units = max(0.0001, round(units, 8))
        return units, price_r * units

    async def _call(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    # ── 취소 ──────────────────────────────────────────────────────────
    async def _cancel_all_orders(self):
        if not self.orders:
            self._log("취소 대상 없음")
            return

        targets = [(oid, o) for oid, o in self.orders.items() if not o.get("filled")]
        self._log(f"취소 시도: {len(targets)}건")

        for oid, order in targets:
            try:
                result = await self._call(self.api._private, "/trade/cancel", {
                    "order_currency":   self.symbol,
                    "payment_currency": "KRW",
                    "order_id": oid, "type": order["side"]
                })
                await asyncio.sleep(0.15)
            except Exception as e:
                self._log(f"취소 예외 {oid[:12]}: {e}")

        # 검증 재조회
        remaining = set()
        try:
            check = await self._call(self.api._get, "/v1/orders",
                                     {"market": f"KRW-{self.symbol}", "state": "wait", "limit": 100})
            if isinstance(check, list):
                for o in check:
                    uid = str(o.get("uuid") or o.get("order_id") or "")
                    if uid:
                        remaining.add(uid)
        except Exception as e:
            self._log(f"취소 검증 실패: {e}")

        cleared = 0
        for oid in list(self.orders.keys()):
            if oid not in remaining:
                del self.orders[oid]
                cleared += 1

        if remaining:
            self._log(f"⚠ 빗썸에 {len(remaining)}건 잔존 — 다음 사이클 재시도")
        else:
            self._log(f"✓ 전량 취소 완료 ({cleared}건)")

        self.filled_uuids = set()
        self.pending      = set()

    # ── 주문 배치 ──────────────────────────────────────────────────────
    async def _place_order(self, side: str, price: float, k: int,
                           counter_units=None, counter_k_old=None):
        raw_price = price if side == 'buy' else price + self.sell_adj
        price_r   = self._round_price(raw_price)

        key = (price_r, side)
        if key in self.pending:
            self._log(f"중복 차단(pending): {side} {price_r}")
            return
        if any(not o["filled"] and o["side"] == side and o["price"] == price_r
               for o in self.orders.values()):
            self._log(f"중복 차단(orders): {side} {price_r}")
            return

        units, order_krw = self._calc_units(side, price_r, k, counter_units, counter_k_old)
        if units <= 0:
            return

        self.pending.add(key)
        try:
            result = await self._call(self.api._private, "/trade/place", {
                "order_currency":   self.symbol,
                "payment_currency": "KRW",
                "units": units, "price": price_r,
                "type":  "bid" if side == 'buy' else "ask"
            })
        except asyncio.TimeoutError:
            self._log(f"배치 타임아웃 {side} {price_r}")
            self.pending.discard(key)
            return

        self.pending.discard(key)
        oid = result.get("data", {}).get("order_id") if result.get("status") == "0000" else None
        if oid:
            self.orders[oid] = {
                "order_id": oid, "side": side,
                "price": price_r, "units": units,
                "amount_krw": order_krw,
                "k": k, "filled": False, "fill_count": 0, "partial": False,
            }
            self._log(f"{'매수' if side=='buy' else '매도'} 배치 k{k} {price_r:,}원 {units:.4f}개")
        else:
            self._log(f"배치 실패 {side} k{k} {price_r}: {result}")

    # ── 초기 배치 ─────────────────────────────────────────────────────
    async def _setup_grid(self):
        self._log(f"_setup_grid 회차{self.round_idx} — 기준:{self.center} 단위:{self.unit} 칸수:{self.limit}")
        try:
            ticker = await asyncio.wait_for(
                asyncio.to_thread(self.api.ticker, self.symbol), timeout=10.0
            )
            if ticker.get("status") == "0000":
                self.cur_price = float(ticker["data"]["closing_price"])
            else:
                self.cur_price = self.center
        except Exception as e:
            self._log(f"ticker 실패: {e}")
            self.cur_price = self.center

        buys, sells = self._grid_prices()
        self._log(f"현재가:{self.cur_price} 매수:{buys} 매도:{sells}")

        for k, bp in enumerate(buys, start=1):
            if bp < self.cur_price:
                await self._place_order('buy', bp, k)
                await asyncio.sleep(0.2)
        for k, sp in enumerate(sells, start=1):
            if sp > self.cur_price:
                await self._place_order('sell', sp, k)
                await asyncio.sleep(0.2)

        self._log(f"회차{self.round_idx} 배치 완료 | 기준:{self.center} | H={self.init_h+self.coin_delta:.4f}")

    # ── 모니터 ────────────────────────────────────────────────────────
    async def _monitor(self):
        try:
            # 미체결 조회
            res = await self._call(self.api._private, "/info/orders", {
                "order_currency": self.symbol, "payment_currency": "KRW",
                "type": "all", "count": "100"
            })
            if "error" in res:
                self._log(f"주문조회 오류: {res}")
                return

            raw_data = res.get("data") or []
            open_ids = set()
            for o in raw_data:
                uid = o.get("uuid") or o.get("order_id")
                if uid:
                    open_ids.add(str(uid))

            if self.orders and len(open_ids) == 0:
                self._log("미체결 조회 빈 결과 — 오류 가능성, 스킵")
                return

            # 현재가
            ticker = await asyncio.to_thread(self.api.ticker, self.symbol)
            if ticker.get("status") != "0000":
                return
            self.cur_price = float(ticker["data"]["closing_price"])

            # 쉬어감 모드 탈출 조건 확인
            if self.resting and self.rest_anchor_price is not None:
                u = self.unit
                a = self.rest_anchor_price
                s = self.rest_anchor_side
                if s == "buy":
                    if not (a - 2 * u <= self.cur_price <= a + 2 * u):
                        self._log(f"쉬어감 탈출: cur={self.cur_price} anchor={a} ({s})")
                        self.resting = False
                        self.round_fill_count = 0
                        self.round_started_at = datetime.now().isoformat()
                        await self._setup_grid()
                        await self.broadcast({"type": "grid2_status", "data": self.get_status()})
                        return
                else:
                    if not (a - 2 * u <= self.cur_price <= a + 2 * u):
                        self._log(f"쉬어감 탈출: cur={self.cur_price} anchor={a} ({s})")
                        self.resting = False
                        self.round_fill_count = 0
                        self.round_started_at = datetime.now().isoformat()
                        await self._setup_grid()
                        await self.broadcast({"type": "grid2_status", "data": self.get_status()})
                        return
                # 쉬어감 중이면 배치 없이 상태만 브로드캐스트
                await self.broadcast({"type": "grid2_status", "data": self.get_status()})
                return

            # 체결 확인
            newly_filled = []
            for oid, order in list(self.orders.items()):
                if str(oid) not in open_ids and not order.get("filled"):
                    order["filled"] = True
                    newly_filled.append(order)
                    self.filled_uuids.add(str(oid))
                    self._log(f"체결! [{order['side'].upper()}] {order['price']:,}원 k{order.get('k',0)}")
                    del self.orders[oid]

            if not newly_filled:
                await self.broadcast({"type": "grid2_status", "data": self.get_status()})
                return

            # 손익 누적
            for order in newly_filled:
                price = order["price"]; units = order["units"]; side = order["side"]
                krw = price * units
                if side == "sell":
                    self.pnl_krw   += krw; self.coin_delta -= units
                else:
                    self.pnl_krw   -= krw; self.coin_delta += units
                self.round_fill_count += 1
                diff = round(price - self.start_center)
                self.fills.append({
                    "time":       datetime.now().isoformat(),
                    "side":       side, "price": price,
                    "units":      units, "krw": krw,
                    "k":          order.get("k", 0),
                    "round":      self.round_idx,
                    "round_fill": self.round_fill_count,
                    "pos":        f"기준{'+'if diff>=0 else ''}{diff}",
                    "pnl_cum":    round(self.pnl_krw),
                    "coin_cum":   round(self.coin_delta, 6),
                    "h_cum":      round(self.init_h + self.coin_delta, 6),
                })
                self.fills = self.fills[-1000:]

            # coin_delta 한계
            ratio = (self.coin_delta / self.init_h) if self.init_h > 0 else self.coin_delta
            if ratio < -0.95:
                self._log(f"⛔ coin_delta/init_h={ratio:.4f} < -0.95 — 강제 종료")
                await self._cancel_all_orders()
                self.sessions.append({
                    "round": self.round_idx, "started_at": self.round_started_at,
                    "ended_at": datetime.now().isoformat(),
                    "fill_count": self.round_fill_count,
                    "pnl_krw": self.pnl_krw, "coin_delta": self.coin_delta,
                    "reason": f"coin_delta 한계 ({ratio:.4f})",
                })
                self.running = False
                await self.broadcast({"type": "grid2_status", "data": self.get_status()})
                return

            # limit 도달 → 회차 리셋
            if self.round_fill_count >= self.limit:
                cur = self.cur_price or self.center
                anchor = min(newly_filled, key=lambda o: abs(o["price"] - cur))
                anchor_price = anchor["price"]
                anchor_side  = anchor["side"]
                anchor_grid  = anchor_price - self.sell_adj if anchor_side == "sell" else anchor_price

                u = self.unit
                mode = None
                new_center = None

                if anchor_side == "buy":
                    if cur < anchor_grid - 2 * u:
                        mode, new_center = 'follow', cur
                    elif anchor_grid - 2 * u <= cur <= anchor_grid - 0.8 * u:
                        mode = 'rest'
                    else:
                        mode, new_center = 'normal', anchor_grid
                else:
                    if cur > anchor_grid + 2 * u:
                        mode, new_center = 'follow', cur
                    elif anchor_grid + 0.8 * u <= cur <= anchor_grid + 2 * u:
                        mode = 'rest'
                    else:
                        mode, new_center = 'normal', anchor_grid

                self.sessions.append({
                    "round": self.round_idx, "started_at": self.round_started_at,
                    "ended_at": datetime.now().isoformat(),
                    "fill_count": self.round_fill_count,
                    "pnl_krw": self.pnl_krw, "coin_delta": self.coin_delta,
                    "anchor_side": anchor_side, "anchor_price": anchor_price,
                    "cur_at_reset": cur,
                    "center_from": self.center,
                    "center_to": None if mode == 'rest' else self._round_price(new_center),
                    "mode": mode, "reason": f"{self.limit}회 체결 ({mode})",
                })
                await self._cancel_all_orders()

                if mode == 'rest':
                    self.resting           = True
                    self.rest_anchor_price = anchor_grid
                    self.rest_anchor_side  = anchor_side
                    self._log(f"⏸ 회차{self.round_idx} 쉬어감 | 앵커({anchor_side} {anchor_grid}) cur:{cur}")
                else:
                    self.round_idx        += 1
                    self.round_fill_count  = 0
                    self.round_started_at  = datetime.now().isoformat()
                    self.center  = self._round_price(new_center)
                    self.resting = False
                    self.rest_anchor_price = None
                    self.rest_anchor_side  = None
                    self._log(f"⟳ 회차{self.round_idx-1}→{self.round_idx} {mode} | 신 center={self.center}")
                    await self._setup_grid()

                await self.broadcast({"type": "grid2_status", "data": self.get_status()})
                return

            # 카운터 주문
            counter_orders = {}
            for order in newly_filled:
                price = order["price"]; side = order["side"]
                units = order["units"]; k_old = order.get("k", 0)
                grid_price = price - self.sell_adj if side == "sell" else price

                if side == "sell":
                    new_price = self._round_price(grid_price - self.unit)
                    grid_min  = self._round_price(self.center - self.limit * self.unit)
                    if new_price < grid_min:
                        continue
                    new_k = max(1, min(self.limit, round((self.center - new_price) / self.unit)))
                    near  = abs(new_price - self.center) <= 0.5 * self.unit
                    key   = int(round(new_price * 100))
                    counter_orders[key] = ('buy', new_price, new_k,
                                           units if near else None,
                                           k_old if near else None)
                else:
                    new_price = self._round_price(grid_price + self.unit)
                    grid_max  = self._round_price(self.center + self.limit * self.unit)
                    if new_price > grid_max:
                        continue
                    new_k = max(1, min(self.limit, round((new_price - self.center) / self.unit)))
                    near  = abs(new_price - self.center) <= 0.5 * self.unit
                    key   = int(round(new_price * 100))
                    counter_orders[key] = ('sell', new_price, new_k,
                                           units if near else None,
                                           k_old if near else None)

            for cside, cprice, ck, cu, ck_old in counter_orders.values():
                await asyncio.sleep(0.2)
                await self._place_order(cside, cprice, ck, counter_units=cu, counter_k_old=ck_old)

            await self.broadcast({"type": "grid2_status", "data": self.get_status()})

        except Exception as e:
            self._log(f"모니터 오류: {e}")

    # ── 메인 루프 ─────────────────────────────────────────────────────
    async def _loop(self):
        self.session_start    = datetime.now().isoformat()
        self.round_started_at = self.session_start
        await self._setup_grid()
        while self.running:
            await asyncio.sleep(2)
            await self._monitor()

    # ── 시작/정지 ─────────────────────────────────────────────────────
    async def start(self, api, broadcast_fn):
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
            "round":      self.round_idx,
            "started_at": self.round_started_at,
            "ended_at":   datetime.now().isoformat(),
            "fill_count": self.round_fill_count,
            "pnl_krw":    self.pnl_krw,
            "coin_delta": self.coin_delta,
            "reason":     reason,
        })
        self.round_fill_count  = 0
        self.round_started_at  = None
        self.resting           = False
        self.rest_anchor_price = None
        self.rest_anchor_side  = None
        self.filled_uuids      = set()
        self.pending           = set()
        self.orders            = {}
        self._log(f"종료: {reason}")

    def get_status(self) -> dict:
        return {
            "symbol":            self.symbol,
            "running":           self.running,
            "center":            self.center,
            "start_center":      self.start_center,
            "unit":              self.unit,
            "init_h":            self.init_h,
            "h_current":         round(self.init_h + self.coin_delta, 6),
            "limit":             self.limit,
            "drain_mode":        self.drain_mode,
            "sell_adj":          self.sell_adj,
            "trend":             self.trend,
            "round_idx":         self.round_idx,
            "round_fill_count":  self.round_fill_count,
            "resting":           self.resting,
            "rest_anchor_price": self.rest_anchor_price,
            "rest_anchor_side":  self.rest_anchor_side,
            "cur_price":         self.cur_price,
            "pnl_krw":           self.pnl_krw,
            "coin_delta":        self.coin_delta,
            "fill_count":        len(self.fills),
            "orders":            list(self.orders.values()),
            "fills":             self.fills[-50:],
            "sessions":          self.sessions,
            "log":               self.log[:20],
        }
