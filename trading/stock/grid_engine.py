"""
trading/stock/grid_engine.py
KRX 고정형 비헷징 그리드 엔진

설계 원칙:
  - Skew / alpha 없음 — 모든 칸 간격·수량 고정
  - 그리드 레벨: center - buy_gap×k (매수k칸) / center + sell_gap×k (매도k칸)
  - 카운터 주문: 체결 직후 바로 마주한 1칸에만
      매수 체결 → center + sell_gap×1 에 매도
      매도 체결 → center - buy_gap×1 에 매수
  - 종료: 매수 전칸 소진(한쪽 밀림) 또는 매도 전칸 소진 → 자동 종료
  - 단일 회차 — 회차 반복 없음
  - 갭 처리 제거 — 장 재개 시 이전 center 그대로 유지
"""

import asyncio
import json
import os
import time
from datetime import datetime
from typing import Optional

from trading.stock.broker_base import (
    BrokerBase, krx_tick, floor_tick, ceil_tick, nearest_tick,
    is_market_open, seconds_to_open,
)

STATE_BASE = "./data/stock_states"


class GridEngine:
    POLL_SEC       = 5
    FILL_CHECK_SEC = 2
    STATE_SAVE_SEC = 30

    def __init__(self, ticker: str, api: BrokerBase, user_id: str = "admin"):
        self.ticker   = ticker.upper()
        self.api      = api
        self.user_id  = user_id

        # ── 설정 ──────────────────────────────────────────
        self.base_qty   = 1      # 칸당 고정 수량
        self.max_levels = 5      # 매수/매도 각 칸 수
        self.buy_gap    = 500    # 매수 칸 간격 (원)
        self.sell_gap   = 500    # 매도 칸 간격 (원)
        self.init_qty   = 0      # 시작 시 보유 주수

        # ── 런타임 ────────────────────────────────────────
        self.running         = False
        self.center          = 0
        self.current_price   = 0
        self.current_name    = ""
        self.holding_qty     = 0
        self.holding_avg     = 0
        self.last_fill_price = 0
        self.last_fill_side  = ""

        # order_id → {side, price, level, qty, filled}
        self.orders: dict[str, dict] = {}
        # level_key → order_id  (음수=매수칸, 양수=매도칸)
        self.grid:   dict[int, str]  = {}

        # ── 손익 ──────────────────────────────────────────
        self.grid_pnl       = 0
        self.grid_trades    = 0
        self.total_buy_qty  = 0; self.total_sell_qty = 0
        self.total_buy_amt  = 0; self.total_sell_amt = 0

        # ── UI 표시용 체결 내역 ───────────────────────────
        self.fills: list[dict] = []

        # ── 종료 이유 ─────────────────────────────────────
        self.end_reason: str = ""

        # ── 내부 ──────────────────────────────────────────
        self._was_open    = False
        self._grid_placed = False
        self._init_center = 0
        self.log: list[str] = []
        self._task: Optional[asyncio.Task] = None
        self.broadcast_fn = None

    # ────────────────────────────────────────────────────
    def set_config(self, base_qty: int, max_levels: int,
                   buy_gap: int, sell_gap: int,
                   init_qty: int = 0):
        self.base_qty   = max(1, int(base_qty))
        self.max_levels = max(1, int(max_levels))
        self.buy_gap    = max(1, int(buy_gap))
        self.sell_gap   = max(1, int(sell_gap))
        self.init_qty   = max(0, int(init_qty))

    # ────────────────────────────────────────────────────
    def _log(self, msg: str):
        line = f"[{datetime.now().strftime('%H:%M:%S')}][{self.ticker}] {msg}"
        self.log.insert(0, line)
        self.log = self.log[:300]
        print(line)

    # ── 상태 저장/복원 ────────────────────────────────────
    def _state_path(self) -> str:
        d = os.path.join(STATE_BASE, self.user_id)
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{self.ticker}.json")

    def save_state(self):
        s = {k: getattr(self, k) for k in [
            "center", "holding_qty", "holding_avg", "init_qty",
            "last_fill_price", "last_fill_side",
            "grid_pnl", "grid_trades",
            "total_buy_qty", "total_sell_qty",
            "total_buy_amt", "total_sell_amt",
            "end_reason",
        ]}
        s["fills"]  = self.fills[-100:]
        s["config"] = {
            "base_qty": self.base_qty, "max_levels": self.max_levels,
            "buy_gap":  self.buy_gap,  "sell_gap":   self.sell_gap,
            "init_qty": self.init_qty,
        }
        s["saved_at"] = int(time.time())
        try:
            json.dump(s, open(self._state_path(), "w"), ensure_ascii=False, indent=2)
        except Exception as e:
            self._log(f"저장 오류: {e}")

    def load_state(self) -> bool:
        p = self._state_path()
        if not os.path.exists(p):
            return False
        try:
            s = json.load(open(p))
            for k in ["center", "holding_qty", "holding_avg", "init_qty",
                      "last_fill_price", "last_fill_side",
                      "grid_pnl", "grid_trades",
                      "total_buy_qty", "total_sell_qty",
                      "total_buy_amt", "total_sell_amt",
                      "end_reason"]:
                if k in s:
                    setattr(self, k, s[k])
            self.fills = s.get("fills", [])
            if cfg := s.get("config"):
                self.set_config(
                    cfg.get("base_qty",  self.base_qty),
                    cfg.get("max_levels",self.max_levels),
                    cfg.get("buy_gap",   self.buy_gap),
                    cfg.get("sell_gap",  self.sell_gap),
                    cfg.get("init_qty",  self.init_qty),
                )
            age_h = (time.time() - s.get("saved_at", 0)) / 3600
            self._log(f"상태 복원 (저장 {age_h:.1f}h 전 | center={self.center:,} | 보유={self.holding_qty}주)")
            return True
        except Exception as e:
            self._log(f"복원 실패: {e}")
            return False

    # ── 고정 그리드 가격 계산 ─────────────────────────────
    def _buy_price(self, level: int) -> int:
        """center에서 level칸 아래 (고정 간격)"""
        p = self.center - self.buy_gap * level
        return max(krx_tick(self.center), floor_tick(p))

    def _sell_price(self, level: int) -> int:
        """center에서 level칸 위 (고정 간격)"""
        return ceil_tick(self.center + self.sell_gap * level)

    # ── 발주 / 취소 ───────────────────────────────────────
    async def _place(self, side: str, qty: int, price: int, level: int) -> Optional[str]:
        r = await asyncio.to_thread(self.api.place_order, self.ticker, side, qty, price)
        if not r.get("ok"):
            self._log(f"발주 실패 {side} {qty}주@{price:,} lv{level}: {r.get('error')}")
            return None
        oid = r["order_id"]
        self.orders[oid] = {
            "side": side, "qty": qty, "price": price,
            "level": level, "filled": 0,
        }
        lk = level if side == "SELL" else -level
        self.grid[lk] = oid
        self._log(f"발주 {side} {qty}주@{price:,}원 lv{level}")
        return oid

    async def _cancel(self, oid: str):
        o = self.orders.get(oid)
        if not o:
            return
        r = await asyncio.to_thread(
            self.api.cancel_order, self.ticker, oid, o["qty"] - o.get("filled", 0)
        )
        if r.get("ok"):
            self.orders.pop(oid, None)
        else:
            self._log(f"취소 실패 {oid}: {r.get('error')}")

    async def _cancel_all(self):
        for oid in list(self.orders.keys()):
            await self._cancel(oid)
        self.grid.clear()
        self._grid_placed = False

    # ── 그리드 초기 배치 ──────────────────────────────────
    async def _place_grid(self):
        if not self.center:
            return
        await self._cancel_all()
        await asyncio.sleep(0.5)

        # 매수: center 아래 k칸, 매도: center 위 k칸
        buy_prices  = [self._buy_price(k)  for k in range(1, self.max_levels + 1)]
        sell_prices = [self._sell_price(k) for k in range(1, self.max_levels + 1)]

        self._log(
            f"그리드 배치 | center={self.center:,} "
            f"매수[{buy_prices[-1]:,}~{buy_prices[0]:,}] "
            f"매도[{sell_prices[0]:,}~{sell_prices[-1]:,}]"
        )

        for k, bp in enumerate(buy_prices, start=1):
            if bp < self.current_price:
                await self._place("BUY", self.base_qty, bp, k)
        for k, sp in enumerate(sell_prices, start=1):
            if sp > self.current_price:
                await self._place("SELL", self.base_qty, sp, k)

        self._grid_placed = True

    # ── 체결 확인 ─────────────────────────────────────────
    async def _check_fills(self):
        if not self.orders:
            return
        for oid, o in list(self.orders.items()):
            r = await asyncio.to_thread(self.api.get_order_status, self.ticker, oid)
            if not r.get("ok") or r.get("status") == "unknown":
                continue
            delta = r["filled_qty"] - o.get("filled", 0)
            if delta <= 0:
                continue
            o["filled"] = r["filled_qty"]
            await self._on_fill(o, delta)
            if r["remaining_qty"] == 0:
                self.orders.pop(oid, None)
                for lk, v in list(self.grid.items()):
                    if v == oid:
                        del self.grid[lk]
                        break

    async def _on_fill(self, order: dict, qty: int):
        side  = order["side"]
        price = order["price"]
        level = order["level"]

        self.last_fill_price = price
        self.last_fill_side  = side

        if side == "BUY":
            self.total_buy_qty += qty
            self.total_buy_amt += qty * price
            if self.holding_qty + qty > 0:
                self.holding_avg = (
                    (self.holding_avg * self.holding_qty + price * qty)
                    // (self.holding_qty + qty)
                )
            self.holding_qty += qty
            self._log(f"✅ 매수 {qty}주@{price:,}원 | 보유 {self.holding_qty}주 | 평단 {self.holding_avg:,}원")
        else:
            self.total_sell_qty += qty
            self.total_sell_amt += qty * price
            realized = qty * (price - self.holding_avg) if self.holding_avg else 0
            self.grid_pnl   += realized
            self.grid_trades += 1
            self.holding_qty = max(0, self.holding_qty - qty)
            self._log(
                f"✅ 매도 {qty}주@{price:,}원 | 실현 {realized:+,}원 | 누적 {self.grid_pnl:+,}원"
            )

        # 체결 내역
        self.fills.append({
            "time":      datetime.now().strftime("%H:%M:%S"),
            "side":      side,
            "price":     price,
            "qty":       qty,
            "krw_chg":   -(qty * price) if side == "BUY" else qty * price,
            "cash_cum":  self.total_sell_amt - self.total_buy_amt,
            "stock_cum": self.holding_qty - self.init_qty,
            "grid_pnl":  self.grid_pnl,
        })
        self.fills = self.fills[-1000:]
        self.save_state()

        # 카운터 주문 배치
        await self._place_counter(side, price, level)

        # 종료 조건 확인
        await self._check_end_condition()

    # ── 카운터 주문: 바로 마주한 1칸에만 ─────────────────
    async def _place_counter(self, filled_side: str, filled_price: int, level: int):
        """
        매수 체결 → sell_gap×1 위 매도 1칸
        매도 체결 → buy_gap×1 아래 매수 1칸
        """
        if filled_side == "BUY":
            counter_price = ceil_tick(self.center + self.sell_gap * 1)
            counter_lk    = 1   # sell level 1
            # 이미 해당 칸에 미체결 주문 있으면 스킵
            if counter_lk in self.grid and self.grid[counter_lk] in self.orders:
                return
            await self._place("SELL", self.base_qty, counter_price, 1)
        else:
            counter_price = floor_tick(self.center - self.buy_gap * 1)
            counter_lk    = -1  # buy level 1
            if counter_lk in self.grid and self.grid[counter_lk] in self.orders:
                return
            await self._place("BUY", self.base_qty, counter_price, 1)

    # ── 종료 조건 ─────────────────────────────────────────
    async def _check_end_condition(self):
        """
        매수 전칸 소진: 보유량이 init_qty + base_qty × max_levels 이상
        매도 전칸 소진: 보유량이 0 이하 (또는 init_qty - base_qty × max_levels)
        → 미체결 전량 취소 후 종료
        """
        max_hold = self.init_qty + self.base_qty * self.max_levels
        min_hold = max(0, self.init_qty - self.base_qty * self.max_levels)

        if self.holding_qty >= max_hold:
            self.end_reason = f"매수 전칸 체결 완료 (보유 {self.holding_qty}주 ≥ 최대 {max_hold}주)"
            self._log(f"🔴 그리드 종료: {self.end_reason}")
            await self._cancel_all()
            self.running = False
            self.save_state()
            if self.broadcast_fn:
                await self.broadcast_fn({"type": "stock_update", "data": self.get_state()})

        elif self.holding_qty <= min_hold and self.grid_trades > 0:
            self.end_reason = f"매도 전칸 체결 완료 (보유 {self.holding_qty}주 ≤ 최소 {min_hold}주)"
            self._log(f"🔴 그리드 종료: {self.end_reason}")
            await self._cancel_all()
            self.running = False
            self.save_state()
            if self.broadcast_fn:
                await self.broadcast_fn({"type": "stock_update", "data": self.get_state()})

    # ── 메인 루프 ─────────────────────────────────────────
    async def _loop(self):
        self._log("엔진 시작")
        self.load_state()
        last_fill_check = last_save = 0.0

        # 수동 시작 (장중) 또는 center가 없으면 현재가로 초기화
        try:
            r = await asyncio.to_thread(self.api.get_price, self.ticker)
            if r.get("ok"):
                self.current_price = abs(int(r["price"]))
                self.current_name  = r.get("name", "")
        except Exception as e:
            self._log(f"시세 오류: {e}")

        if self._init_center > 0:
            self.center = self._init_center
        if not self.center and self.current_price:
            self.center = nearest_tick(self.current_price)

        if self.center and is_market_open():
            await self._place_grid()
            self._was_open = True

        while self.running:
            now = time.time()
            market_open = is_market_open()

            # 장 시작 감지
            if market_open and not self._was_open:
                self._log("장 재개 — 그리드 재배치")
                try:
                    r = await asyncio.to_thread(self.api.get_price, self.ticker)
                    if r.get("ok"):
                        self.current_price = abs(int(r["price"]))
                        self.current_name  = r.get("name", "")
                except Exception as e:
                    self._log(f"시세 오류: {e}")
                if self.center:
                    await self._place_grid()

            # 장 마감 감지
            elif not market_open and self._was_open:
                self._log("장 마감 — 전량 취소")
                await self._cancel_all()
                self.save_state()

            self._was_open = market_open

            if not market_open:
                secs = seconds_to_open()
                await asyncio.sleep(60 if secs > 600 else 10)
                continue

            # 현재가 갱신
            try:
                r = await asyncio.to_thread(self.api.get_price, self.ticker)
                if r.get("ok"):
                    self.current_price = abs(int(r["price"]))
                    self.current_name  = r.get("name", "")
            except Exception as e:
                self._log(f"시세 오류: {e}")
                await asyncio.sleep(self.POLL_SEC)
                continue

            # 체결 확인
            if now - last_fill_check >= self.FILL_CHECK_SEC:
                try:
                    await self._check_fills()
                except Exception as e:
                    self._log(f"체결 확인 오류: {e}")
                last_fill_check = now

            # 주기 저장
            if now - last_save >= self.STATE_SAVE_SEC:
                self.save_state()
                last_save = now

            # broadcast
            if self.broadcast_fn:
                try:
                    await self.broadcast_fn({"type": "stock_update", "data": self.get_state()})
                except Exception:
                    pass

            await asyncio.sleep(self.POLL_SEC)

        self._log("엔진 정지")

    # ── 시작 / 정지 ───────────────────────────────────────
    async def start(self, broadcast_fn=None):
        if self.running:
            return
        self.broadcast_fn  = broadcast_fn
        self.running       = True
        self.end_reason    = ""
        import time
        self._started_ts   = time.time()
        self._task         = asyncio.create_task(self._loop())

    async def stop(self):
        self.running = False
        await self._cancel_all()
        self.save_state()
        if self._task:
            self._task.cancel()

    # ── 수동 재배치 (center 변경) ─────────────────────────
    async def manual_rebalance(self, new_center: Optional[int] = None):
        self.center = nearest_tick(new_center or self.current_price)
        self._log(f"수동 재배치 center={self.center:,}")
        await self._place_grid()

    # ── 상태 반환 ─────────────────────────────────────────
    def get_state(self) -> dict:
        unreal = (
            (self.current_price - self.holding_avg) * self.holding_qty
            if self.holding_avg and self.holding_qty else 0
        )
        # 그리드 범위
        buy_range  = (self._buy_price(self.max_levels),  self._buy_price(1))  if self.center else (0, 0)
        sell_range = (self._sell_price(1), self._sell_price(self.max_levels)) if self.center else (0, 0)

        orders_list = sorted(
            [{"order_id": oid, **o} for oid, o in self.orders.items()],
            key=lambda x: x["price"], reverse=True,
        )
        return {
            "ticker":         self.ticker,
            "name":           self.current_name,
            "running":        self.running,
            "end_reason":     self.end_reason,
            "market_open":    is_market_open(),
            "current_price":  self.current_price,
            "center":         self.center,
            "holding_qty":    self.holding_qty,
            "holding_avg":    self.holding_avg,
            "init_qty":       self.init_qty,
            "last_fill_price":self.last_fill_price,
            "last_fill_side": self.last_fill_side,
            "grid_pnl":       self.grid_pnl,
            "unrealized_pnl": unreal,
            "total_pnl":      self.grid_pnl + unreal,
            "grid_trades":    self.grid_trades,
            "buy_range":      buy_range,
            "sell_range":     sell_range,
            "orders":         orders_list,
            "fills":          self.fills[-50:],
            "log":            self.log[:30],
            "config": {
                "base_qty":   self.base_qty,
                "max_levels": self.max_levels,
                "buy_gap":    self.buy_gap,
                "sell_gap":   self.sell_gap,
                "init_qty":   self.init_qty,
            },
        }
