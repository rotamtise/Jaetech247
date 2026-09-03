"""
app/services/discord_alert.py
채널별 독립 Discord Webhook 알림 서비스

- 코인 기본형(CryptoGrid2Runner), 주식(GridEngine) 모두 지원
- 채널마다 독립 webhook URL + 주기 설정
- 메시지: 빗썸 잔고 + 그리드 현황 (바이낸스 제외)
"""

import asyncio
import time
from datetime import datetime
from typing import Callable, Optional

import requests


# ── 채널별 알림 인스턴스 저장소 ───────────────────────────────────
# { channel_id: ChannelAlert }
_alerts: dict[int, "ChannelAlert"] = {}


def get_or_create(channel_id: int) -> "ChannelAlert":
    if channel_id not in _alerts:
        _alerts[channel_id] = ChannelAlert(channel_id)
    return _alerts[channel_id]


def get(channel_id: int) -> Optional["ChannelAlert"]:
    return _alerts.get(channel_id)


async def stop_all():
    for alert in _alerts.values():
        await alert.stop()


# ── 주기 선택지 (초) ──────────────────────────────────────────────
INTERVAL_OPTIONS = {
    "30m":  30 * 60,
    "1h":   60 * 60,
    "2h":  120 * 60,
    "4h":  240 * 60,
}


class ChannelAlert:
    """채널 하나에 대한 Discord 알림 인스턴스."""

    def __init__(self, channel_id: int):
        self.channel_id = channel_id
        self.webhook:   str  = ""
        self.interval:  int  = 3600     # 기본 1시간
        self.enabled:   bool = False

        self._task: Optional[asyncio.Task] = None
        # 상태 provider: 외부에서 주입 (get_state 콜백)
        self._state_provider: Optional[Callable] = None
        # 빗썸 API provider (잔고 조회용)
        self._bithumb_provider: Optional[Callable] = None

    # ── 설정 ────────────────────────────────────────────────────────
    def configure(self, webhook: str, interval_key: str,
                  state_provider: Callable,
                  bithumb_provider: Optional[Callable] = None):
        self.webhook  = webhook.strip()
        self.interval = INTERVAL_OPTIONS.get(interval_key, 3600)
        self._state_provider   = state_provider
        self._bithumb_provider = bithumb_provider

    # ── 시작/정지 ───────────────────────────────────────────────────
    async def start(self):
        if self.enabled:
            return
        if not self.webhook:
            raise ValueError("Webhook URL이 설정되지 않았습니다.")
        self.enabled = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self.enabled = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=3)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

    # ── 즉시 전송 ───────────────────────────────────────────────────
    async def send_now(self) -> bool:
        try:
            msg = await self._build_message()
            await self._send(msg)
            return True
        except Exception as e:
            print(f"[DC CH{self.channel_id:02d}] 즉시 전송 실패: {e}")
            return False

    # ── 상태 반환 ────────────────────────────────────────────────────
    def get_status(self) -> dict:
        # 현재 주기 키 역조회
        interval_key = next(
            (k for k, v in INTERVAL_OPTIONS.items() if v == self.interval),
            "1h"
        )
        return {
            "enabled":      self.enabled,
            "webhook":      ("*" * 8 + self.webhook[-10:]) if len(self.webhook) > 10 else self.webhook,
            "interval_key": interval_key,
            "interval_sec": self.interval,
        }

    # ── 루프 ────────────────────────────────────────────────────────
    async def _loop(self):
        # 시작 즉시 1회 전송
        try:
            msg = await self._build_message()
            await self._send(msg)
            print(f"[DC CH{self.channel_id:02d}] 시작 알림 전송")
        except Exception as e:
            print(f"[DC CH{self.channel_id:02d}] 시작 알림 실패: {e}")

        while self.enabled:
            # interval 동안 1초씩 체크 (즉시 중단 가능)
            for _ in range(self.interval):
                if not self.enabled:
                    return
                await asyncio.sleep(1)
            try:
                msg = await self._build_message()
                await self._send(msg)
                print(f"[DC CH{self.channel_id:02d}] 정기 전송 완료 — {datetime.now().strftime('%H:%M')}")
            except Exception as e:
                print(f"[DC CH{self.channel_id:02d}] 정기 전송 실패: {e}")

    # ── 메시지 빌드 ─────────────────────────────────────────────────
    async def _build_message(self) -> str:
        lines = []
        now = datetime.now().strftime("%m/%d %H:%M")

        lines.append(f"📊 **채널 {self.channel_id:02d} 운용 현황** — {now}")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━")

        # 그리드 현황
        if self._state_provider:
            try:
                states = self._state_provider()   # dict 또는 list[dict]
                if isinstance(states, dict):
                    states = list(states.values())

                running = [s for s in states if s.get("running")]

                if not running:
                    lines.append("⏸ 현재 실행 중인 그리드 없음")
                else:
                    for s in running:
                        lines.append(_format_grid_state(s))
            except Exception as e:
                lines.append(f"⚠ 그리드 상태 조회 실패: {e}")

        # 빗썸 잔고 (API 키가 있는 경우)
        if self._bithumb_provider:
            try:
                bt = await asyncio.to_thread(self._bithumb_provider)
                if bt and bt.get("ok"):
                    lines.append("")
                    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
                    lines.append(f"🇰🇷 **빗썸 잔고: ₩{bt['total_krw']:,.0f}**")
                    lines.append(f"  현금 ₩{bt['cash_krw']:,.0f} / 코인 ₩{bt['coin_krw']:,.0f}")
                    for item in bt.get("items", [])[:5]:
                        lines.append(
                            f"  {item['symbol']}: {item['qty']:.4f}개 "
                            f"× ₩{item['price']:,.0f} = **₩{item['krw']:,.0f}**"
                        )
            except Exception as e:
                lines.append(f"  빗썸 잔고 조회 실패: {e}")

        return "\n".join(lines)

    async def _send(self, text: str):
        if not self.webhook:
            return
        chunks = [text[i:i+1900] for i in range(0, len(text), 1900)]
        for chunk in chunks:
            await asyncio.to_thread(
                requests.post, self.webhook,
                json={"content": chunk}, timeout=8
            )


# ── 그리드 상태 포맷 (채널 타입별) ───────────────────────────────
def _format_grid_state(s: dict) -> str:
    """CryptoGrid2Runner 또는 GridEngine의 get_state() 결과를 디스코드 메시지로 포맷."""
    lines = []

    # ── 코인 기본형 (CryptoGrid2Runner) ──
    if "slot" in s:
        sym     = s.get("symbol", "?")
        slot    = s.get("slot", "?")
        cur     = s.get("cur_price") or s.get("center", 0)
        ctr     = s.get("start_center") or s.get("center", 0)
        pnl_krw = s.get("pnl_krw", 0)
        cd      = s.get("coin_delta", 0)
        mavg    = (ctr + cur) / 2 if cur and ctr else cur or ctr
        grid_pnl  = pnl_krw + cd * mavg
        eval_pnl  = cd * (cur - mavg) if cur and mavg else 0
        total_pnl = grid_pnl + eval_pnl
        elapsed   = s.get("elapsed_h", 0)
        fills     = s.get("fill_count", 0)
        round_idx = s.get("round_idx", 1)
        resting   = s.get("resting", False)

        s_grid  = "+" if grid_pnl  >= 0 else ""
        s_total = "+" if total_pnl >= 0 else ""

        status = "⏸ 쉬어감" if resting else "▶ 실행중"
        lines.append(
            f"\n**[슬롯 {slot}] {sym}** {status}\n"
            f"  현재가: ₩{cur:,.0f}  (기준: ₩{ctr:,.0f})\n"
            f"  그리드 손익: **{s_grid}₩{grid_pnl:,.0f}**\n"
            f"  평가 손익:   {'+' if eval_pnl>=0 else ''}₩{eval_pnl:,.0f}\n"
            f"  통산 손익:   **{s_total}₩{total_pnl:,.0f}**\n"
            f"  회차 {round_idx} / 체결 {fills}회 / ⏱{elapsed:.1f}h\n"
            f"  코인Δ: {'+' if cd>=0 else ''}{cd:.4f}개"
        )

    # ── 주식 기본형 (GridEngine) ──
    elif "ticker" in s:
        ticker   = s.get("ticker", "?")
        name     = s.get("name", "")
        cur      = s.get("current_price", 0)
        center   = s.get("center", 0)
        grid_pnl = s.get("grid_pnl", 0)
        unreal   = s.get("unrealized_pnl", 0)
        total    = s.get("total_pnl", 0)
        holding  = s.get("holding_qty", 0)
        avg_p    = s.get("holding_avg", 0)
        trades   = s.get("grid_trades", 0)
        dev      = s.get("deviation", 0)
        gap_pct  = s.get("gap_pct", 0)

        s_grid  = "+" if grid_pnl >= 0 else ""
        s_total = "+" if total    >= 0 else ""
        gap_str = f"{'+'if gap_pct>=0 else ''}{gap_pct*100:.2f}%" if gap_pct else "—"

        lines.append(
            f"\n**{ticker}** ({name})\n"
            f"  현재가: {cur:,}원  (center: {center:,}원)\n"
            f"  당일갭: {gap_str}\n"
            f"  그리드 손익: **{s_grid}{grid_pnl:,.0f}원**\n"
            f"  평가 손익:   {'+' if unreal>=0 else ''}{unreal:,.0f}원\n"
            f"  통산 손익:   **{s_total}{total:,.0f}원**\n"
            f"  보유: {holding}주 @ {avg_p:,}원 / 체결 {trades}회\n"
            f"  편중(dev): {'+' if dev>=0 else ''}{dev:.3f}"
        )

    # ── 프리미엄 Grid2Runner ──
    elif "round_idx" in s and "pnl_krw" in s:
        sym     = s.get("symbol", "?")
        cur     = s.get("cur_price") or s.get("center", 0)
        ctr     = s.get("start_center") or s.get("center", 0)
        pnl_krw = s.get("pnl_krw", 0)
        cd      = s.get("coin_delta", 0)
        mavg    = (ctr + cur) / 2 if cur and ctr else 0
        grid_pnl  = pnl_krw + cd * mavg
        eval_pnl  = cd * (cur - mavg) if mavg else 0
        total_pnl = grid_pnl + eval_pnl
        fills     = s.get("fill_count", 0)
        round_idx = s.get("round_idx", 1)
        resting   = s.get("resting", False)

        s_total = "+" if total_pnl >= 0 else ""
        status  = "⏸ 쉬어감" if resting else "▶ 실행중"

        lines.append(
            f"\n**{sym}** {status}\n"
            f"  현재가: ₩{cur:,.0f}  (기준: ₩{ctr:,.0f})\n"
            f"  그리드: {'+' if grid_pnl>=0 else ''}₩{grid_pnl:,.0f} | "
            f"평가: {'+' if eval_pnl>=0 else ''}₩{eval_pnl:,.0f}\n"
            f"  통산: **{s_total}₩{total_pnl:,.0f}**\n"
            f"  회차 {round_idx} / 체결 {fills}회 / 코인Δ {'+' if cd>=0 else ''}{cd:.4f}"
        )

    return "\n".join(lines)
