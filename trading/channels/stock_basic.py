"""
trading/channels/stock_basic.py
Korea Investment Securities (KIS) REST API trading — channels 31~35.

All Windows/Kiwoom OCX code has been removed.
Pure aarch64 Linux compatible via REST API.

KIS OAuth2 token flow:
  POST /oauth2/tokenP → access_token (valid 24h)

Grid/momentum strategy (adapted from original logic):
  - Morning: buy momentum stocks if condition met
  - Afternoon: trailing stop-loss sell
"""
import asyncio
from datetime import datetime, timezone, time as dtime
from typing import Optional

import httpx

from app.core.config import settings
from trading.channels.base import BaseChannel


KIS_BASE = "https://openapi.koreainvestment.com:9443"

# Korean stock market hours (KST = UTC+9)
MARKET_OPEN_KST = dtime(9, 0)
MARKET_CLOSE_KST = dtime(15, 30)


class KISClient:
    """Korea Investment Securities async REST client."""

    def __init__(self, api_key: str, api_secret: str, account_no: str, client: httpx.AsyncClient):
        self._key = api_key
        self._secret = api_secret
        self._account_no = account_no   # e.g. "50124567-01"
        self._client = client
        self._token: Optional[str] = None
        self._token_expires: Optional[datetime] = None

    async def _ensure_token(self):
        """Get or refresh OAuth2 access token."""
        now = datetime.now(timezone.utc)
        if self._token and self._token_expires and now < self._token_expires:
            return

        resp = await self._client.post(
            f"{KIS_BASE}/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": self._key,
                "appsecret": self._secret,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        # KIS token expires in 86400s
        from datetime import timedelta
        self._token_expires = now + timedelta(seconds=data.get("expires_in", 86400) - 60)

    def _headers(self, tr_id: str) -> dict:
        acct_parts = self._account_no.split("-")
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self._token}",
            "appkey": self._key,
            "appsecret": self._secret,
            "tr_id": tr_id,
            "custtype": "P",
            "CANO": acct_parts[0] if acct_parts else self._account_no,
            "ACNT_PRDT_CD": acct_parts[1] if len(acct_parts) > 1 else "01",
        }

    async def get_current_price(self, stock_code: str) -> dict:
        await self._ensure_token()
        resp = await self._client.get(
            f"{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers={**self._headers("FHKST01010100"), "tr_cont": "N"},
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code},
        )
        resp.raise_for_status()
        return resp.json()

    async def get_balance(self) -> dict:
        await self._ensure_token()
        acct_parts = self._account_no.split("-")
        resp = await self._client.get(
            f"{KIS_BASE}/uapi/domestic-stock/v1/trading/inquire-balance",
            headers={**self._headers("TTTC8434R"), "tr_cont": "N"},
            params={
                "CANO": acct_parts[0],
                "ACNT_PRDT_CD": acct_parts[1] if len(acct_parts) > 1 else "01",
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "01",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def buy_market(self, stock_code: str, qty: int) -> dict:
        """Market order buy."""
        await self._ensure_token()
        acct_parts = self._account_no.split("-")
        resp = await self._client.post(
            f"{KIS_BASE}/uapi/domestic-stock/v1/trading/order-cash",
            headers=self._headers("TTTC0802U"),
            json={
                "CANO": acct_parts[0],
                "ACNT_PRDT_CD": acct_parts[1] if len(acct_parts) > 1 else "01",
                "PDNO": stock_code,
                "ORD_DVSN": "01",      # 시장가
                "ORD_QTY": str(qty),
                "ORD_UNPR": "0",
                "CTAC_TLNO": "",
                "SLL_TYPE": "000",
                "ALGO_NO": "",
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def sell_market(self, stock_code: str, qty: int) -> dict:
        """Market order sell."""
        await self._ensure_token()
        acct_parts = self._account_no.split("-")
        resp = await self._client.post(
            f"{KIS_BASE}/uapi/domestic-stock/v1/trading/order-cash",
            headers=self._headers("TTTC0801U"),
            json={
                "CANO": acct_parts[0],
                "ACNT_PRDT_CD": acct_parts[1] if len(acct_parts) > 1 else "01",
                "PDNO": stock_code,
                "ORD_DVSN": "01",
                "ORD_QTY": str(qty),
                "ORD_UNPR": "0",
                "CTAC_TLNO": "",
                "ALGO_NO": "",
            },
        )
        resp.raise_for_status()
        return resp.json()


def is_market_open() -> bool:
    """Check if Korean stock market is currently open (KST)."""
    import zoneinfo
    kst = zoneinfo.ZoneInfo("Asia/Seoul")
    now_kst = datetime.now(kst)
    if now_kst.weekday() >= 5:   # Sat/Sun
        return False
    t = now_kst.time()
    return MARKET_OPEN_KST <= t <= MARKET_CLOSE_KST


class StockBasicChannel(BaseChannel):
    """
    Channels 31~35: KIS REST API momentum + trailing stop strategy.

    Strategy params:
      - stock_code: 6-digit KRX code (e.g. "005930" = Samsung)
      - buy_qty: number of shares per trade
      - momentum_threshold: % gain from day open to trigger buy (default 1.5%)
      - stop_loss_pct: trailing stop from peak (default -2.0%)
      - take_profit_pct: take profit target (default 3.0%)
    """

    def __init__(self, channel_id: int, state):
        super().__init__(channel_id, state)
        self._kis: Optional[KISClient] = None
        self._params: dict = {}
        self._tick_count = 0
        self._in_position = False
        self._entry_price: float = 0.0
        self._peak_price: float = 0.0
        self._bought_today = False

    async def on_start(self):
        await super().on_start()
        client = await self._get_client()
        self._kis = KISClient(
            self.creds.get("api_key", ""),
            self.creds.get("api_secret", ""),
            self.creds.get("account_no", ""),
            client,
        )
        # Load params
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
                    self.state.symbol = self._params.get("stock_code", ch.symbol)
        except Exception as e:
            self.log(f"파라미터 로드 실패: {e}", "WARN")

        self.log(
            f"[CH{self.channel_id:02d}] KIS 주식 채널 초기화 | 종목: {self.state.symbol}", "INFO"
        )

    async def tick(self):
        self._tick_count += 1
        stock_code = self._params.get("stock_code", self.state.symbol)
        if not stock_code:
            return

        if not is_market_open():
            if self._tick_count % 60 == 0:
                self.log("장외 시간 대기 중…", "INFO")
            # Reset daily flags at midnight KST
            import zoneinfo
            kst_hour = datetime.now(zoneinfo.ZoneInfo("Asia/Seoul")).hour
            if kst_hour == 0:
                self._bought_today = False
            return

        try:
            price_data = await self._kis.get_current_price(stock_code)
            output = price_data.get("output", {})
            current = float(output.get("stck_prpr", 0))
            open_price = float(output.get("stck_oprc", current))

            self.state.current_price = current

            buy_qty = int(self._params.get("buy_qty", 1))
            momentum_thr = float(self._params.get("momentum_threshold", 1.5))
            stop_loss = float(self._params.get("stop_loss_pct", -2.0))
            take_profit = float(self._params.get("take_profit_pct", 3.0))

            change_pct = ((current - open_price) / open_price * 100) if open_price else 0

            if self._in_position:
                # Trailing stop
                if current > self._peak_price:
                    self._peak_price = current
                trail_pct = ((current - self._peak_price) / self._peak_price * 100)
                profit_pct = ((current - self._entry_price) / self._entry_price * 100)

                should_sell = trail_pct <= stop_loss or profit_pct >= take_profit

                if self._tick_count % 5 == 0:
                    self.log(
                        f"{stock_code} 보유중 | 현재가 {current:,}원 | "
                        f"수익률 {profit_pct:+.2f}% | 트레일링 {trail_pct:+.2f}%",
                        "INFO"
                    )

                if should_sell:
                    resp = await self._kis.sell_market(stock_code, buy_qty)
                    if resp.get("rt_cd") == "0":
                        profit = (current - self._entry_price) * buy_qty
                        self.state.realized_pnl += profit
                        self.state.trade_count += 1
                        self._in_position = False
                        reason = "익절" if profit_pct >= take_profit else "손절(트레일링)"
                        self.log(
                            f"✅ {reason} 매도 | {current:,}원 | "
                            f"수익 {profit:+,.0f}원 | 누적 {self.state.realized_pnl:+,.0f}원",
                            "TRADE"
                        )
                        self.state.last_order = {
                            "side": "SELL", "price": current, "qty": buy_qty,
                            "profit": profit, "reason": reason
                        }

            else:
                # Entry condition: momentum
                if not self._bought_today and change_pct >= momentum_thr:
                    resp = await self._kis.buy_market(stock_code, buy_qty)
                    if resp.get("rt_cd") == "0":
                        self._in_position = True
                        self._entry_price = current
                        self._peak_price = current
                        self._bought_today = True
                        self.state.position_qty += buy_qty
                        self.state.trade_count += 1
                        self.log(
                            f"✅ 모멘텀 매수 | {stock_code} {current:,}원 × {buy_qty}주 | "
                            f"등락 +{change_pct:.2f}%",
                            "TRADE"
                        )
                        self.state.last_order = {
                            "side": "BUY", "price": current, "qty": buy_qty
                        }

        except httpx.HTTPError as e:
            self.log(f"KIS API 오류: {e}", "ERROR")
        except Exception as e:
            self.log(f"틱 오류: {e}", "ERROR")
