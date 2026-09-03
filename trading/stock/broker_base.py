"""
trading/stock/broker_base.py
KRX 호가단위, 장 시간, 추상 브로커 인터페이스
broker_base.py 원본 100% 이식 + SaaS 구조용 경로 조정
"""
from abc import ABC, abstractmethod
from datetime import datetime, timedelta


# ─── 호가단위 (KRX 공통) ──────────────────────────────────────────────
def krx_tick(price: int) -> int:
    if   price <    2_000: return 1
    elif price <    5_000: return 5
    elif price <   20_000: return 10
    elif price <   50_000: return 50
    elif price <  200_000: return 100
    elif price <  500_000: return 500
    else:                  return 1_000

def floor_tick(price: float) -> int:
    p = max(1, int(price))
    t = krx_tick(p)
    return max(t, (p // t) * t)

def ceil_tick(price: float) -> int:
    p = max(1, int(price))
    t = krx_tick(p)
    q, r = divmod(p, t)
    return (q + (1 if r else 0)) * t

def nearest_tick(price: float) -> int:
    p = max(1, int(round(price)))
    t = krx_tick(p)
    return max(t, round(p / t) * t)


# ─── 장 시간 (KST 기준) ──────────────────────────────────────────────
MARKET_OPEN  = (9,  0)
MARKET_CLOSE = (15, 30)

def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN <= t < MARKET_CLOSE

def seconds_to_open() -> int:
    if is_market_open():
        return 0
    now = datetime.now()
    if now.weekday() >= 5:
        days = 7 - now.weekday()
        nxt = now.replace(hour=9, minute=0, second=0, microsecond=0)
        return int((nxt - now).total_seconds()) + days * 86400
    h, m = now.hour, now.minute
    if (h, m) < MARKET_OPEN:
        nxt = now.replace(hour=9, minute=0, second=0, microsecond=0)
    else:
        nxt = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        while nxt.weekday() >= 5:
            nxt += timedelta(days=1)
    return max(0, int((nxt - now).total_seconds()))


# ─── 추상 브로커 ─────────────────────────────────────────────────────
class BrokerBase(ABC):
    """모든 브로커가 구현해야 하는 공통 인터페이스"""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def get_price(self, ticker: str) -> dict:
        """반환: {ok, price, name, open, high, low, volume, close_prev}"""
        ...

    @abstractmethod
    def get_orderbook(self, ticker: str) -> dict:
        """반환: {ok, asks:[(price,qty)×5], bids:[(price,qty)×5], current}"""
        ...

    @abstractmethod
    def place_order(self, ticker: str, side: str, qty: int, price: int) -> dict:
        """반환: {ok, order_id, ticker, side, qty, price}"""
        ...

    @abstractmethod
    def cancel_order(self, ticker: str, order_id: str, qty: int) -> dict:
        """반환: {ok, error}"""
        ...

    @abstractmethod
    def get_order_status(self, ticker: str, order_id: str) -> dict:
        """반환: {ok, filled_qty, remaining_qty, status('filled'|'pending'|'unknown')}"""
        ...

    @abstractmethod
    def get_balance(self) -> dict:
        """반환: {ok, stocks:[{ticker,name,qty,avg_price,eval_amt}], cash}"""
        ...
