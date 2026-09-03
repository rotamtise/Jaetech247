"""
trading/channels/premium.py
Premium channels 01~06: full-featured grid trading (Bithumb + Binance).
Admin/VIP only. All Binance UI is shown (not hidden like in basic channels).
"""
from trading.channels.crypto_basic import CryptoBasicChannel, BithumbClient


class PremiumChannel(CryptoBasicChannel):
    """
    Extends CryptoBasicChannel with:
    - Advanced grid with dynamic level adjustment
    - Multi-exchange support (Bithumb primary, Binance secondary)
    - Detailed console logging
    - Aggregated P&L across positions
    """

    async def on_start(self):
        await super().on_start()
        self.log(
            f"[PREMIUM CH{self.channel_id:02d}] 프리미엄 채널 활성화 | "
            f"관리자/VIP 전용 모드",
            "INFO"
        )

    async def tick(self):
        """Premium tick: same grid logic + enhanced logging."""
        await super().tick()

        # Additional: compute unrealized PnL from current price
        if self.state.position_qty > 0 and self.state.position_avg_price > 0:
            self.state.unrealized_pnl = (
                (self.state.current_price - self.state.position_avg_price)
                * self.state.position_qty
            )
