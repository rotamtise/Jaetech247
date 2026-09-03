"""
app/services/stats.py
인메모리 실시간 통계 취합 헬퍼

순환참조 방지 설계:
  - auth.py → app.services.stats  (단방향)
  - 각 라우터 모듈은 import 하지 않음
  - 런타임에 importlib 없이, 이미 sys.modules에 올라온
    라우터 모듈에서 저장소 변수를 직접 참조(getattr)
  - 모듈이 아직 로드되지 않았으면 조용히 스킵
"""
from __future__ import annotations
import sys
import time
from dataclasses import dataclass


@dataclass
class PlatformStats:
    active_count: int    = 0
    total_profit: int    = 0    # KRW
    avg_hours:    float  = 0.0
    show_banner:  bool   = False   # 12h 환산 10,000원 이상




def _iso_to_hours(ts_str, now_ts: float) -> float:
    """ISO datetime 문자열 → 현재까지 경과 시간(h). 실패 시 0."""
    if not ts_str:
        return 0.0
    try:
        from datetime import datetime
        # tzinfo가 없으면 local로 처리 (session_start는 datetime.now().isoformat()으로 저장됨)
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            ts = dt.timestamp()
        else:
            ts = dt.timestamp()
        return max(0.0, (now_ts - ts) / 3600)
    except Exception:
        return 0.0


def collect() -> PlatformStats:
    """
    현재 메모리에 올라온 모든 봇 인스턴스를 순회해
    active_count / total_profit / avg_hours 를 반환한다.
    """
    profits: list[float] = []
    hours:   list[float] = []
    now = time.time()

    # ── 코인 기본형 채널 (crypto_basic._runners) ─────────────
    crypto_mod = sys.modules.get("app.routers.crypto_basic")
    if crypto_mod:
        runners_dict = getattr(crypto_mod, "_runners", {})
        for ch_slots in runners_dict.values():
            for runner in ch_slots.values():
                if runner and getattr(runner, "running", False):
                    s = runner.get_status()
                    cur   = s.get("cur_price") or s.get("center", 0)
                    mavg  = ((s.get("start_center") or cur) + cur) / 2
                    pnl   = s.get("pnl_krw", 0) + s.get("coin_delta", 0) * mavg
                    eh    = s.get("elapsed_h", 0)
                    profits.append(pnl)
                    hours.append(eh)

    # ── 주식 채널 (stock._grids) ─────────────────────────────
    stock_mod = sys.modules.get("app.routers.stock")
    if stock_mod:
        grids_dict = getattr(stock_mod, "_grids", {})
        for ch_grids in grids_dict.values():
            for engine in ch_grids.values():
                if getattr(engine, "running", False):
                    s   = engine.get_state()
                    pnl = s.get("total_pnl", 0)
                    # 가동 시간: loop 시작 시각이 없으면 0
                    started = getattr(engine, "_started_ts", 0)
                    eh  = (now - started) / 3600 if started else 0
                    profits.append(pnl)
                    hours.append(eh)

    # ── 프리미엄 채널 (premium.grid_runners) ───────────────
    prem_mod = sys.modules.get("app.routers.premium")
    if prem_mod:
        for runner in getattr(prem_mod, "grid_runners", {}).values():
            if getattr(runner, "running", False):
                s   = runner.get_status()
                cur  = s.get("cur_price") or s.get("center", 0)
                ctr  = s.get("start_center") or cur
                mavg = (ctr + cur) / 2
                pnl  = s.get("pnl_krw", 0) + s.get("coin_delta", 0) * mavg
                # session_start 필드
                ts_str = getattr(runner, "session_start", None)
                eh = _iso_to_hours(ts_str, now)
                profits.append(pnl)
                hours.append(eh)

        for runner in getattr(prem_mod, "binance_grid_runners", {}).values():
            if getattr(runner, "running", False):
                s   = runner.get_status()
                pnl = s.get("pnl_usdt", 0) * 1380   # USDT → KRW 근사
                ts_str = getattr(runner, "session_start", None)
                eh = _iso_to_hours(ts_str, now)
                profits.append(pnl)
                hours.append(eh)

    # ── 집계 ────────────────────────────────────────────────
    count = len(profits)
    if count == 0:
        return PlatformStats()

    total_profit = int(sum(profits))
    avg_hours    = round(sum(hours) / count, 1)

    # 전광판 노출 조건: 12h 환산 수익 ≥ 10,000원
    # total_profit / avg_hours * 12 >= 10000  (avg_hours > 0 guard)
    show = False
    if avg_hours > 0:
        rate_12h = (total_profit / avg_hours) * 12
        show = rate_12h >= 10_000

    return PlatformStats(
        active_count = count,
        total_profit = total_profit,
        avg_hours    = avg_hours,
        show_banner  = show,
    )
