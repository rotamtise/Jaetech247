"""
trading/engine.py
Core async trading engine.

Key design:
  - Each channel runs as an independent asyncio Task
  - Time-staggered scheduling (Modulo offset):
      offset_sec = channel_id % 6
      Channel fires at UTC seconds where second % 6 == offset_sec
  - WebSocket registry for real-time push to connected browsers
  - No synchronous loops; all I/O via httpx AsyncClient
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from fastapi import WebSocket
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("trading.engine")


# ── Channel State ─────────────────────────────────────────────────────────────
class ChannelState:
    def __init__(self, channel_id: int):
        self.channel_id = channel_id
        self.running = False
        self.symbol: Optional[str] = None
        self.owner_id: Optional[str] = None
        self.channel_type: Optional[str] = None

        # Realtime data
        self.current_price: float = 0.0
        self.position_qty: float = 0.0
        self.position_avg_price: float = 0.0
        self.unrealized_pnl: float = 0.0
        self.realized_pnl: float = 0.0
        self.trade_count: int = 0
        self.order_book: dict = {}
        self.last_order: Optional[dict] = None
        self.logs: List[dict] = []   # rolling 100 log lines

        # Task handle
        self._task: Optional[asyncio.Task] = None

    def to_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "running": self.running,
            "symbol": self.symbol,
            "owner_id": self.owner_id,
            "channel_type": self.channel_type,
            "current_price": self.current_price,
            "position_qty": self.position_qty,
            "position_avg_price": self.position_avg_price,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "trade_count": self.trade_count,
            "last_order": self.last_order,
            "logs": self.logs[-50:],
        }

    def add_log(self, msg: str, level: str = "INFO"):
        self.logs.append({
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "level": level,
            "msg": msg,
        })
        if len(self.logs) > 100:
            self.logs = self.logs[-100:]


# ── Trading Engine Singleton ──────────────────────────────────────────────────
class TradingEngine:
    def __init__(self):
        self._states: Dict[int, ChannelState] = {}
        self._ws_registry: Dict[int, Set[WebSocket]] = {}   # channel_id -> set of WS
        self._cycle_seconds = 6   # base period

    def _get_or_create_state(self, channel_id: int) -> ChannelState:
        if channel_id not in self._states:
            self._states[channel_id] = ChannelState(channel_id)
        return self._states[channel_id]

    def get_channel_state(self, channel_id: int) -> Optional[dict]:
        st = self._states.get(channel_id)
        return st.to_dict() if st else None

    # ── WebSocket registry ────────────────────────────────────────────────────
    async def register_ws(self, channel_id: int, ws: WebSocket):
        if channel_id not in self._ws_registry:
            self._ws_registry[channel_id] = set()
        self._ws_registry[channel_id].add(ws)

    async def unregister_ws(self, channel_id: int, ws: WebSocket):
        if channel_id in self._ws_registry:
            self._ws_registry[channel_id].discard(ws)

    async def _broadcast(self, channel_id: int, payload: dict):
        """Push state update to all connected WebSocket clients."""
        dead = set()
        for ws in self._ws_registry.get(channel_id, set()):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)
        if dead and channel_id in self._ws_registry:
            self._ws_registry[channel_id] -= dead

    # ── Time-staggered tick wait ──────────────────────────────────────────────
    async def _wait_for_next_tick(self, channel_id: int):
        """
        Wait until the next UTC second where:
            utc_second % 6 == channel_id % 6

        This guarantees channels are spread evenly across the 6-second window,
        regardless of when start() is called.
        """
        offset = channel_id % self._cycle_seconds

        while True:
            now = datetime.now(timezone.utc)
            current_sec = now.second
            current_microsec = now.microsecond

            # How many seconds until next aligned tick?
            secs_past_cycle = current_sec % self._cycle_seconds
            wait = (offset - secs_past_cycle) % self._cycle_seconds
            if wait == 0 and current_microsec < 100_000:
                # We're right at the tick — fire now
                break
            if wait == 0:
                wait = self._cycle_seconds

            # Sleep to just before the target second, then fine-tune
            sleep_target = wait - current_microsec / 1_000_000
            if sleep_target > 0.01:
                await asyncio.sleep(sleep_target - 0.01)

            # Busy-wait the last 10ms for precision
            while True:
                now2 = datetime.now(timezone.utc)
                if now2.second % self._cycle_seconds == offset:
                    return
                await asyncio.sleep(0.001)

    # ── Channel task runner ───────────────────────────────────────────────────
    async def _run_channel(self, channel_id: int, db_session_factory):
        """Main loop for a single trading channel."""
        from trading.channels.crypto_basic import CryptoBasicChannel
        from trading.channels.stock_basic import StockBasicChannel
        from trading.channels.premium import PremiumChannel

        state = self._states[channel_id]
        state.add_log(f"채널 {channel_id:02d} 시작 (오프셋 {channel_id % 6}초)", "INFO")

        # Select channel implementation
        if 1 <= channel_id <= 6:
            runner = PremiumChannel(channel_id, state)
        elif 7 <= channel_id <= 30:
            runner = CryptoBasicChannel(channel_id, state)
        else:
            runner = StockBasicChannel(channel_id, state)

        # Load API credentials from DB
        async with db_session_factory() as session:
            from sqlalchemy import select as sa_select
            from app.models.database import User, Channel
            from app.core.security import decrypt_api_key

            ch_res = await session.execute(
                sa_select(Channel).where(Channel.channel_id == channel_id)
            )
            ch = ch_res.scalar_one_or_none()
            if not ch:
                state.add_log("채널 DB 레코드 없음", "ERROR")
                state.running = False
                return

            u_res = await session.execute(
                sa_select(User).where(User.id == ch.owner_id)
            )
            user = u_res.scalar_one_or_none()
            if not user:
                state.add_log("소유자 계정 없음", "ERROR")
                state.running = False
                return

            creds = {}
            if 1 <= channel_id <= 30:  # crypto or premium
                creds["api_key"] = decrypt_api_key(user.bithumb_api_key_enc or "")
                creds["api_secret"] = decrypt_api_key(user.bithumb_api_secret_enc or "")
            else:  # stock
                creds["api_key"] = decrypt_api_key(user.kis_api_key_enc or "")
                creds["api_secret"] = decrypt_api_key(user.kis_api_secret_enc or "")
                creds["account_no"] = user.kis_account_no or ""

            runner.set_credentials(creds)
            state.symbol = ch.symbol
            state.channel_type = ch.channel_type
            try:
                state.position_qty = json.loads(ch.strategy_params or "{}").get("qty", 0)
            except Exception:
                pass

        # Main tick loop
        try:
            await runner.on_start()
            while state.running:
                await self._wait_for_next_tick(channel_id)
                if not state.running:
                    break
                try:
                    await runner.tick()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    state.add_log(f"틱 오류: {e}", "ERROR")
                    logger.exception(f"Channel {channel_id} tick error")
                    await asyncio.sleep(1)

                # Push to all connected WebSocket clients
                await self._broadcast(channel_id, {
                    "type": "state",
                    "data": state.to_dict(),
                })

                # Persist stats to DB
                await self._persist_stats(channel_id, state, db_session_factory)

        except asyncio.CancelledError:
            pass
        finally:
            await runner.on_stop()
            state.running = False
            state.add_log(f"채널 {channel_id:02d} 정지", "INFO")
            await self._broadcast(channel_id, {"type": "state", "data": state.to_dict()})

    async def _persist_stats(self, channel_id: int, state: ChannelState, db_factory):
        """Persist trade stats back to DB (non-blocking)."""
        try:
            async with db_factory() as session:
                from sqlalchemy import select as sa_select, update
                from app.models.database import Channel
                await session.execute(
                    update(Channel)
                    .where(Channel.channel_id == channel_id)
                    .values(
                        total_profit=state.realized_pnl,
                        trade_count=state.trade_count,
                        last_tick_at=datetime.now(timezone.utc),
                        is_running=state.running,
                    )
                )
                await session.commit()
        except Exception as e:
            logger.warning(f"Stats persist error ch{channel_id}: {e}")

    # ── Public API ─────────────────────────────────────────────────────────────
    async def start_channel(self, channel_id: int, user_id: str, db_session) -> bool:
        state = self._get_or_create_state(channel_id)
        if state.running:
            return False

        state.running = True
        state.owner_id = user_id

        from app.models.database import AsyncSessionLocal
        task = asyncio.create_task(
            self._run_channel(channel_id, AsyncSessionLocal),
            name=f"channel-{channel_id:02d}"
        )
        state._task = task

        # Update DB is_running flag
        try:
            from sqlalchemy import update
            from app.models.database import Channel
            await db_session.execute(
                update(Channel).where(Channel.channel_id == channel_id)
                .values(is_running=True)
            )
            await db_session.commit()
        except Exception:
            pass

        logger.info(f"Channel {channel_id:02d} started by {user_id}")
        return True

    async def stop_channel(self, channel_id: int) -> bool:
        state = self._states.get(channel_id)
        if not state or not state.running:
            return False

        state.running = False
        if state._task and not state._task.done():
            state._task.cancel()
            try:
                await asyncio.wait_for(state._task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        logger.info(f"Channel {channel_id:02d} stopped")
        return True

    async def stop_all(self):
        """Graceful shutdown — called on app shutdown."""
        tasks = []
        for ch_id, state in self._states.items():
            if state.running:
                state.running = False
                if state._task and not state._task.done():
                    state._task.cancel()
                    tasks.append(state._task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


# Singleton instance
trading_engine = TradingEngine()
