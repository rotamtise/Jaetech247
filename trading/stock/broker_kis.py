"""
trading/stock/broker_kis.py
한국투자증권 KIS Open API 브로커 (aarch64 Linux 순수 REST)
broker_kis.py 원본 100% 이식 — Kiwoom OCX 관련 코드 완전 제거
"""
import requests, time, random
from trading.stock.broker_base import BrokerBase, krx_tick, floor_tick, ceil_tick, nearest_tick


# ── MockMarket (개발/테스트용) ────────────────────────────────────────
class MockMarket:
    def __init__(self, ticker: str, base: int = 50_000):
        self.ticker = ticker
        self.price  = base
        self.prev_close = base
        self._orders: dict[str, dict] = {}

    def tick(self) -> int:
        t = krx_tick(self.price)
        self.price = max(t, self.price + random.choice([-2,-1,-1,0,0,0,1,1,2]) * t)
        return self.price

    def orderbook(self) -> dict:
        t = krx_tick(self.price)
        return {
            "asks": [(self.price + t*i, random.randint(50,500)) for i in range(1,6)],
            "bids": [(self.price - t*i, random.randint(50,500)) for i in range(1,6)],
            "current": self.price,
        }

    def place(self, oid, side, qty, price):
        self._orders[oid] = {"side":side,"qty":qty,"price":price,"filled":0}

    def check_fill(self, oid) -> dict:
        o = self._orders.get(oid)
        if not o:
            return {"filled_qty":0,"remaining_qty":0,"status":"unknown"}
        can = (o["side"]=="BUY" and self.price<=o["price"]) or \
              (o["side"]=="SELL" and self.price>=o["price"])
        if can and random.random() < 0.8:
            o["filled"] = o["qty"]
            return {"filled_qty":o["qty"],"remaining_qty":0,"status":"filled"}
        return {"filled_qty":o["filled"],"remaining_qty":o["qty"]-o["filled"],"status":"pending"}


# ── KISBroker ─────────────────────────────────────────────────────────
class KISBroker(BrokerBase):
    BASE = "https://openapi.koreainvestment.com:9443"

    def __init__(self, app_key: str = "", app_secret: str = "",
                 account_no: str = "", mock: bool = True):
        self.app_key    = app_key.strip()
        self.app_secret = app_secret.strip()
        self.account_no = account_no.strip()
        self.mock       = mock
        self._token     = ""
        self._token_exp = 0
        self._mocks: dict[str, MockMarket] = {}

    @property
    def name(self): return "KIS"

    def _ensure_token(self):
        if time.time() < self._token_exp - 300:
            return
        if self.mock:
            self._token = "MOCK"; self._token_exp = time.time() + 86400; return
        r = requests.post(f"{self.BASE}/oauth2/tokenP", json={
            "grant_type": "client_credentials",
            "appkey": self.app_key, "appsecret": self.app_secret,
        }, timeout=10)
        if r.ok:
            d = r.json()
            self._token = d["access_token"]
            self._token_exp = time.time() + int(d.get("expires_in", 86400))
        else:
            raise RuntimeError(f"KIS 토큰 실패: {r.status_code} {r.text[:100]}")

    def _hdr(self, tr_id: str) -> dict:
        return {
            "authorization": f"Bearer {self._token}",
            "appkey": self.app_key, "appsecret": self.app_secret,
            "tr_id": tr_id, "custtype": "P",
            "Content-Type": "application/json; charset=utf-8",
        }

    def _get(self, path: str, params: dict, tr_id: str) -> dict:
        self._ensure_token()
        try:
            r = requests.get(f"{self.BASE}{path}", params=params,
                             headers=self._hdr(tr_id), timeout=10)
            return r.json() if r.ok else {"rt_cd":"-1","msg1":f"HTTP {r.status_code}"}
        except Exception as e:
            return {"rt_cd":"-1","msg1":str(e)}

    def _post(self, path: str, body: dict, tr_id: str) -> dict:
        self._ensure_token()
        try:
            r = requests.post(f"{self.BASE}{path}", json=body,
                              headers=self._hdr(tr_id), timeout=10)
            return r.json() if r.ok else {"rt_cd":"-1","msg1":f"HTTP {r.status_code}"}
        except Exception as e:
            return {"rt_cd":"-1","msg1":str(e)}

    def _m(self, ticker: str) -> MockMarket:
        if ticker not in self._mocks:
            self._mocks[ticker] = MockMarket(ticker)
        return self._mocks[ticker]

    def _split(self) -> tuple[str, str]:
        if "-" in self.account_no:
            return self.account_no.split("-", 1)
        return self.account_no[:8], self.account_no[8:]

    # ── Public API ─────────────────────────────────────────────────
    def get_price(self, ticker: str) -> dict:
        if self.mock:
            m = self._m(ticker); p = m.tick()
            return {"ok":True,"price":p,"name":f"[MOCK]{ticker}",
                    "open":p,"high":p,"low":p,"volume":0,"close_prev":m.prev_close}
        r = self._get("/uapi/domestic-stock/v1/quotations/inquire-price",
                      {"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":ticker},
                      "FHKST01010100")
        if r.get("rt_cd") != "0":
            return {"ok":False,"error":r.get("msg1","??")}
        o = r["output"]
        return {"ok":True,
                "price":      int(o["stck_prpr"]),
                "open":       int(o["stck_oprc"]),
                "high":       int(o["stck_hgpr"]),
                "low":        int(o["stck_lwpr"]),
                "volume":     int(o["acml_vol"]),
                "name":       o.get("hts_kor_isnm", ticker),
                "close_prev": int(o.get("stck_sdpr", o["stck_prpr"])),
                }

    def get_orderbook(self, ticker: str) -> dict:
        if self.mock:
            return {"ok":True, **self._m(ticker).orderbook()}
        r = self._get("/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                      {"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":ticker},
                      "FHKST01010200")
        if r.get("rt_cd") != "0":
            return {"ok":False,"error":r.get("msg1","??")}
        o = r["output1"]
        asks = [(int(o[f"askp{i}"]),int(o[f"askp_rsqn{i}"])) for i in range(1,6)]
        bids = [(int(o[f"bidp{i}"]),int(o[f"bidp_rsqn{i}"])) for i in range(1,6)]
        return {"ok":True,"asks":asks,"bids":bids,
                "current":int(o.get("stck_prpr", asks[0][0]))}

    # ── Private API ────────────────────────────────────────────────
    def place_order(self, ticker: str, side: str, qty: int, price: int) -> dict:
        if qty <= 0:
            return {"ok":False,"error":"수량 0"}
        price = floor_tick(price) if side=="BUY" else ceil_tick(price)
        if self.mock:
            oid = f"KIS-{ticker}-{side}-{int(time.time()*1000)}"
            self._m(ticker).place(oid, side, qty, price)
            print(f"[KIS MOCK] {side} {ticker} {qty}주@{price:,}")
            return {"ok":True,"order_id":oid,"ticker":ticker,"side":side,"qty":qty,"price":price}
        acct, suf = self._split()
        body = {"CANO":acct,"ACNT_PRDT_CD":suf,"PDNO":ticker,
                "ORD_DVSN":"00","ORD_QTY":str(qty),"ORD_UNPR":str(price)}
        tr_id = "TTTC0802U" if side=="BUY" else "TTTC0801U"
        r = self._post("/uapi/domestic-stock/v1/trading/order-cash", body, tr_id)
        if r.get("rt_cd") != "0":
            return {"ok":False,"error":r.get("msg1","??")}
        return {"ok":True,"order_id":r["output"]["ODNO"],
                "ticker":ticker,"side":side,"qty":qty,"price":price}

    def cancel_order(self, ticker: str, order_id: str, qty: int) -> dict:
        if self.mock:
            return {"ok":True}
        acct, suf = self._split()
        body = {"CANO":acct,"ACNT_PRDT_CD":suf,"KRX_FWDG_ORD_ORGNO":"",
                "ORGN_ODNO":order_id,"ORD_DVSN":"00","RVSE_CNCL_DVSN_CD":"02",
                "ORD_QTY":str(qty),"ORD_UNPR":"0","QTY_ALL_ORD_YN":"Y"}
        r = self._post("/uapi/domestic-stock/v1/trading/order-rvsecncl", body, "TTTC0803U")
        return {"ok":r.get("rt_cd")=="0","error":r.get("msg1","")}

    def get_order_status(self, ticker: str, order_id: str) -> dict:
        if self.mock:
            return {"ok":True, **self._m(ticker).check_fill(order_id)}
        acct, suf = self._split()
        r = self._get("/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
                      {"CANO":acct,"ACNT_PRDT_CD":suf,
                       "CTX_AREA_FK100":"","CTX_AREA_NK100":"",
                       "INQR_DVSN_1":"0","INQR_DVSN_2":"0"}, "TTTC8036R")
        if r.get("rt_cd") != "0":
            return {"ok":False,"error":r.get("msg1","??")}
        for o in r.get("output",[]):
            if o.get("odno") == order_id:
                tot = int(o.get("ord_qty",0)); rem = int(o.get("rmn_qty",0))
                return {"ok":True,"filled_qty":tot-rem,"remaining_qty":rem,
                        "status":"filled" if rem==0 else "pending"}
        return {"ok":True,"filled_qty":0,"remaining_qty":0,"status":"unknown"}

    def get_balance(self) -> dict:
        if self.mock:
            return {"ok":True,"stocks":[],"cash":10_000_000}
        acct, suf = self._split()
        r = self._get("/uapi/domestic-stock/v1/trading/inquire-balance",
                      {"CANO":acct,"ACNT_PRDT_CD":suf,
                       "AFHR_FLPR_YN":"N","OFL_YN":"","INQR_DVSN":"02",
                       "UNPR_DVSN":"01","FUND_STTL_ICLD_YN":"N",
                       "FNCG_AMT_AUTO_RDPT_YN":"N","PRCS_DVSN":"01",
                       "CTX_AREA_FK100":"","CTX_AREA_NK100":""}, "TTTC8434R")
        if r.get("rt_cd") != "0":
            return {"ok":False,"error":r.get("msg1","??")}
        stocks = [{"ticker":o["pdno"],"name":o.get("prdt_name",""),
                   "qty":int(o.get("hldg_qty",0)),
                   "avg_price":int(float(o.get("pchs_avg_pric",0))),
                   "eval_amt":int(o.get("evlu_amt",0))}
                  for o in r.get("output1",[]) if int(o.get("hldg_qty",0))>0]
        cash = int(r.get("output2",[{}])[0].get("dnca_tot_amt",0))
        return {"ok":True,"stocks":stocks,"cash":cash}
