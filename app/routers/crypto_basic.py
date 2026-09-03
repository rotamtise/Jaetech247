"""
app/routers/crypto_basic.py — 코인 기본형 채널 라우터 (채널 07~30)
슬롯 A / B, open_member.py Grid2Runner 이식, DB API 키 주입
"""
import asyncio, hashlib, time, urllib.parse, uuid as _uuid
from datetime import datetime
from typing import Optional

import jwt as pyjwt, requests
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from app.core.config import settings
from app.services import discord_alert as dc_svc
from app.core.security import decrypt_api_key
from app.models.database import AsyncSessionLocal, Channel, User
from app.routers.auth import get_current_user

router = APIRouter(prefix="/channel", tags=["crypto_basic"])
templates = Jinja2Templates(directory="app/templates")

_runners: dict[int, dict[str, Optional["CryptoGrid2Runner"]]] = {}
_ws_pool: dict[int, set[WebSocket]] = {}

def _get_runners(ch_id):
    if ch_id not in _runners:
        _runners[ch_id] = {"A": None, "B": None}
    return _runners[ch_id]

async def _broadcast_channel(ch_id, msg):
    dead = set()
    for ws in list(_ws_pool.get(ch_id, set())):
        try: await ws.send_json(msg)
        except: dead.add(ws)
    if dead and ch_id in _ws_pool:
        _ws_pool[ch_id] -= dead

# ── 빗썸 API ──────────────────────────────────────────────────────────
class BithumbAPIBasic:
    BASE = "https://api.bithumb.com"
    def __init__(self, api_key, secret_key):
        self.api_key = api_key.strip(); self.secret_key = secret_key.strip()
    def _market(self, sym): return f"KRW-{sym.upper()}"
    def _server_timestamp(self):
        try:
            import email.utils
            r = requests.get(f"{self.BASE}/v1/ticker?markets=KRW-BTC", timeout=3)
            return round(email.utils.parsedate_to_datetime(r.headers["Date"]).timestamp()*1000)
        except: return round(time.time()*1000)
    def _make_jwt(self, params=None):
        p = {"access_key": self.api_key, "nonce": str(_uuid.uuid4()), "timestamp": self._server_timestamp()}
        if params:
            qs = urllib.parse.urlencode(params)
            p["query_hash"] = hashlib.sha512(qs.encode()).hexdigest()
            p["query_hash_alg"] = "SHA512"
        return pyjwt.encode(p, self.secret_key, algorithm="HS256")
    def _get(self, path, params=None):
        r = requests.get(self.BASE+path, params=params, headers={"Authorization": f"Bearer {self._make_jwt(params)}"}, timeout=5)
        return r.json() if r.ok else {"error": r.text}
    def _post(self, path, body):
        r = requests.post(self.BASE+path, json=body, headers={"Authorization": f"Bearer {self._make_jwt(body)}", "Content-Type":"application/json"}, timeout=5)
        return r.json() if r.ok else {"error": r.text}
    def _delete(self, path, params):
        r = requests.delete(self.BASE+path, params=params, headers={"Authorization": f"Bearer {self._make_jwt(params)}"}, timeout=5)
        return r.json() if r.ok else {"error": r.text}
    def ticker(self, sym):
        try:
            r = requests.get(f"{self.BASE}/v1/ticker", params={"markets": self._market(sym)}, timeout=5)
            d = r.json(); d = d[0] if isinstance(d, list) and d else d
            if "trade_price" not in d: return {"status":"error"}
            return {"status":"0000","data":{"closing_price":str(d["trade_price"])}}
        except Exception as e: return {"status":"error","message":str(e)}
    def _private(self, endpoint, params):
        sym = params.get("order_currency",""); market = self._market(sym)
        try:
            if endpoint == "/trade/place":
                side = "bid" if params.get("type")=="bid" else "ask"
                raw = float(params["price"]); ps = str(round(raw,2)) if raw<100 else str(int(raw))
                res = self._post("/v2/orders",{"market":market,"side":side,"order_type":"limit","price":ps,"volume":str(float(params["units"]))})
                oid = res.get("order_id") or res.get("uuid")
                return {"status":"0000","data":{"order_id":str(oid)}} if oid else {"status":"error","message":str(res)}
            elif endpoint == "/trade/cancel":
                res = self._delete("/v1/order",{"uuid":str(params["order_id"])})
                return {"status":"0000"} if ("uuid" in res or "order_id" in res) else {"status":"error","message":str(res)}
        except Exception as e: return {"status":"error","message":str(e)}
        return {"status":"error","message":"unknown"}

# ── 수량 모델 ──────────────────────────────────────────────────────────
TREND_SKEW = {"강":0.045,"중":0.030,"약":0.015}
def _tick_unit(p):
    if p>=2_000_000: return 1000
    if p>=1_000_000: return 500
    if p>=500_000: return 100
    if p>=100_000: return 50
    if p>=10_000: return 10
    if p>=1_000: return 1
    if p>=100: return 0.1
    if p>=10: return 0.01
    return 0.001

# ── Grid2Runner ────────────────────────────────────────────────────────
class CryptoGrid2Runner:
    def __init__(self, slot, channel_id, symbol, center, unit, init_h=1.0, limit=6, sell_adj=0.0, trend="중"):
        self.slot=slot; self.channel_id=channel_id; self.symbol=symbol.upper()
        self.center=center; self.start_center=center; self.unit=unit
        self.init_h=float(init_h); self.limit=int(limit)
        self.sell_adj=float(sell_adj); self.trend=trend if trend in TREND_SKEW else "중"
        self.running=False; self._task=None
        self.round_idx=1; self.round_fill_count=0; self.round_started_at=None
        self.resting=False; self.rest_anchor_price=None; self.rest_anchor_side=None
        self.orders={}; self.filled_uuids=set(); self.pending=set()
        self.fills=[]; self.pnl_krw=0.0; self.coin_delta=0.0
        self.sessions=[]; self.session_start=None; self.log=[]; self.api=None
        self.cur_price=0.0; self.started_ts=0.0

    def _log(self,msg):
        e=f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        self.log.insert(0,e); self.log=self.log[:100]
        print(f"[CH{self.channel_id:02d}-{self.slot}][{self.symbol}] {msg}")

    def _rp(self,p):
        u=_tick_unit(p); return round(round(p/u)*u,2) if p<100 else int(round(p/u)*u)

    def _grid_prices(self):
        buys=[self._rp(self.center-k*self.unit) for k in range(1,self.limit+1)]
        sells=[self._rp(self.center+k*self.unit) for k in range(1,self.limit+1)]
        return buys,sells

    def _calc_units(self, side, price_r, k=1, cu=None, ck_old=None):
        if cu is not None:
            u=cu*(1.0+(ck_old*0.002)/0.045) if ck_old else cu
            return max(0.0001,round(u,8)), price_r*u
        H=max(0.0,self.init_h+self.coin_delta); norm=H/self.init_h if self.init_h>0 else 1.0
        sc=TREND_SKEW.get(self.trend,0.030); skew=sc*(norm-1.0); pen=k*0.002
        ratio=(0.045*1.006-skew-pen) if side=="buy" else (0.045+skew-pen)
        H_eff=self.init_h+0.7*self.coin_delta
        units=max(0.0001,round(ratio*H_eff,8)); return units, price_r*units

    async def _bcast(self):
        await _broadcast_channel(self.channel_id,{"type":"grid_status","slot":self.slot,"data":self.get_status()})

    async def _cancel_all(self):
        targets=[(oid,o) for oid,o in self.orders.items() if not o.get("filled")]
        self._log(f"취소 {len(targets)}건")
        for oid,order in targets:
            try:
                await asyncio.to_thread(self.api._private,"/trade/cancel",{"order_currency":self.symbol,"payment_currency":"KRW","order_id":oid,"type":order["side"]})
                await asyncio.sleep(0.15)
            except Exception as e: self._log(f"취소 예외: {e}")
        try:
            raw=await asyncio.to_thread(self.api._get,"/v1/orders",{"market":f"KRW-{self.symbol}","state":"wait","limit":100})
            remaining={str(o.get("uuid") or "") for o in (raw if isinstance(raw,list) else [])}
            for oid in list(self.orders):
                if oid not in remaining: del self.orders[oid]
        except: pass
        self.filled_uuids=set(); self.pending=set()

    async def _setup_grid(self):
        self._log(f"격자배치 회차{self.round_idx} | 기준:{self.center:,} 간격:{self.unit}")
        try:
            t=await asyncio.wait_for(asyncio.to_thread(self.api.ticker,self.symbol),timeout=10.0)
            self.cur_price=float(t["data"]["closing_price"]) if t.get("status")=="0000" else self.center
        except: self.cur_price=self.center
        buys,sells=self._grid_prices()
        for k,bp in enumerate(buys,1):
            if bp<self.cur_price: await self._place("buy",bp,k); await asyncio.sleep(0.2)
        for k,sp in enumerate(sells,1):
            if sp>self.cur_price: await self._place("sell",sp,k); await asyncio.sleep(0.2)

    async def _place(self, side, price, k, cu=None, ck_old=None):
        raw_p=price if side=="buy" else price+self.sell_adj; price_r=self._rp(raw_p)
        key=(price_r,side)
        if key in self.pending: return
        if any(not o["filled"] and o["side"]==side and o["price"]==price_r for o in self.orders.values()): return
        units,order_krw=self._calc_units(side,price_r,k,cu,ck_old)
        if units<=0: return
        self.pending.add(key)
        try:
            res=await asyncio.to_thread(self.api._private,"/trade/place",{"order_currency":self.symbol,"payment_currency":"KRW","units":round(units,8),"price":price_r,"type":"ask" if side=="sell" else "bid"})
            oid=res.get("data",{}).get("order_id") if res.get("status")=="0000" else None
            if oid:
                self.orders[oid]={"order_id":oid,"side":side,"price":price_r,"units":units,"amount_krw":order_krw,"k":k,"filled":False,"fill_count":0,"partial":False}
                self._log(f"{'매수'if side=='buy'else'매도'} k{k} {price_r:,}원 {units:.4f}개")
            else: self._log(f"배치 실패 {side} k{k}: {res}")
        finally: self.pending.discard(key)

    async def _try_resolve_resting(self):
        if not self.resting or self.rest_anchor_price is None: return False
        cur=self.cur_price; anchor=self.rest_anchor_price; u=self.unit
        new_center=None; mode=None
        if self.rest_anchor_side=="buy":
            if cur<anchor-2*u: new_center,mode=cur,"follow"
            elif cur>anchor-0.8*u: new_center,mode=anchor,"normal"
        else:
            if cur>anchor+2*u: new_center,mode=cur,"follow"
            elif cur<anchor+0.8*u: new_center,mode=anchor,"normal"
        if new_center is None: return True
        self.center=self._rp(new_center); self.resting=False
        self.rest_anchor_price=None; self.rest_anchor_side=None
        self.round_fill_count=0; self.round_started_at=datetime.now().isoformat()
        self._log(f"쉬어감 해소({mode}) center={self.center:,}")
        await self._setup_grid(); return True

    async def _monitor(self):
        try: await asyncio.wait_for(self._monitor_inner(),timeout=8.0)
        except asyncio.TimeoutError: self._log("타임아웃")
        except Exception as e: self._log(f"오류: {e}")

    async def _monitor_inner(self):
        try:
            t=await asyncio.wait_for(asyncio.to_thread(self.api.ticker,self.symbol),timeout=5.0)
            if t.get("status")=="0000": self.cur_price=float(t["data"]["closing_price"])
        except asyncio.TimeoutError: return
        if self.resting:
            if await self._try_resolve_resting(): await self._bcast(); return
        try:
            raw=await asyncio.wait_for(asyncio.to_thread(self.api._get,"/v1/orders",{"market":f"KRW-{self.symbol}","state":"wait","limit":100}),timeout=6.0)
        except asyncio.TimeoutError: return
        if not isinstance(raw,list): return
        open_ids=set(); partial_ids=set()
        for o in raw:
            uid=str(o.get("uuid") or o.get("order_id") or "")
            if not uid: continue
            open_ids.add(uid)
            try:
                if float(o.get("executed_volume") or 0)>0 and float(o.get("remaining_volume") or 0)>0: partial_ids.add(uid)
            except: pass
        if not open_ids and [o for o in self.orders.values() if not o["filled"]]:
            self._log("미체결 빈 결과 스킵"); return
        newly_filled=[]; seen={"buy":set(),"sell":set()}
        for oid,order in list(self.orders.items()):
            sid=str(oid)
            if sid in self.filled_uuids: del self.orders[oid]; continue
            if order["filled"]: continue
            if sid in partial_ids: order["partial"]=True; continue
            order["partial"]=False
            if sid not in open_ids:
                pk=int(round(order["price"]*100)); side=order["side"]
                if pk in seen[side]: self.filled_uuids.add(sid); del self.orders[oid]; continue
                seen[side].add(pk); self.filled_uuids.add(sid)
                order["filled"]=True; newly_filled.append(order)
                self._log(f"체결 {'매도'if side=='sell'else'매수'} {order['price']:,}원 {order['units']:.4f}개")
        for o in newly_filled: self.orders.pop(o["order_id"],None)
        if not newly_filled: await self._bcast(); return
        for order in newly_filled:
            p=order["price"]; u=order["units"]; s=order["side"]; krw=p*u
            if s=="sell": self.pnl_krw+=krw; self.coin_delta-=u
            else: self.pnl_krw-=krw; self.coin_delta+=u
            self.round_fill_count+=1
            diff=round(p-self.start_center)
            self.fills.append({"time":datetime.now().isoformat(),"side":s,"price":p,"units":u,"krw":krw,"k":order.get("k",0),"round":self.round_idx,"round_fill":self.round_fill_count,"pos":f"기준{'+'if diff>=0 else''}{diff}","pnl_cum":round(self.pnl_krw),"coin_cum":round(self.coin_delta,6)})
            self.fills = self.fills[-1000:]
        ratio=(self.coin_delta/self.init_h) if self.init_h>0 else self.coin_delta
        if ratio<-0.95:
            self._log(f"coin_delta 한계 종료({ratio:.4f})")
            await self._cancel_all()
            self.sessions.append({"round":self.round_idx,"started_at":self.round_started_at,"ended_at":datetime.now().isoformat(),"fill_count":self.round_fill_count,"pnl_krw":self.pnl_krw,"coin_delta":self.coin_delta,"reason":f"한계({ratio:.4f})"})
            self.running=False; await self._bcast(); return
        if self.round_fill_count>=self.limit:
            cur=self.cur_price or self.center
            anchor=min(newly_filled,key=lambda o:abs(o["price"]-cur))
            ag=anchor["price"]-self.sell_adj if anchor["side"]=="sell" else anchor["price"]
            u=self.unit; mode=None; nc=None
            if anchor["side"]=="buy":
                if cur<ag-2*u: mode,nc="follow",cur
                elif ag-2*u<=cur<=ag-0.8*u: mode="rest"
                else: mode,nc="normal",ag
            else:
                if cur>ag+2*u: mode,nc="follow",cur
                elif ag+0.8*u<=cur<=ag+2*u: mode="rest"
                else: mode,nc="normal",ag
            self.sessions.append({"round":self.round_idx,"started_at":self.round_started_at,"ended_at":datetime.now().isoformat(),"fill_count":self.round_fill_count,"pnl_krw":self.pnl_krw,"coin_delta":self.coin_delta,"mode":mode,"reason":f"{self.limit}회 체결({mode})"})
            await self._cancel_all()
            if mode=="rest":
                self.resting=True; self.rest_anchor_price=ag; self.rest_anchor_side=anchor["side"]
                self._log(f"쉬어감 앵커:{ag:,}")
            else:
                self.round_idx+=1; self.round_fill_count=0; self.round_started_at=datetime.now().isoformat()
                self.center=self._rp(nc); self.resting=False; self.rest_anchor_price=None; self.rest_anchor_side=None
                self._log(f"회차→{self.round_idx}({mode}) center={self.center:,}")
                await self._setup_grid()
            await self._bcast(); return
        cq={}
        for order in newly_filled:
            p=order["price"]; s=order["side"]; u=order["units"]; ko=order.get("k",0)
            gp=p-self.sell_adj if s=="sell" else p
            if s=="sell":
                np=self._rp(gp-self.unit); gmin=self._rp(self.center-self.limit*self.unit)
                if np<gmin: continue
                nk=max(1,min(self.limit,round((self.center-np)/self.unit)))
                near=abs(np-self.center)<=0.5*self.unit
                cq[int(round(np*100))]=("buy",np,nk,u if near else None,ko if near else None)
            else:
                np=self._rp(gp+self.unit); gmax=self._rp(self.center+self.limit*self.unit)
                if np>gmax: continue
                nk=max(1,min(self.limit,round((np-self.center)/self.unit)))
                near=abs(np-self.center)<=0.5*self.unit
                cq[int(round(np*100))]=("sell",np,nk,u if near else None,ko if near else None)
        for cs,cp,ck,cu,cko in cq.values():
            await asyncio.sleep(0.2); await self._place(cs,cp,ck,cu,cko)
        await self._bcast()

    async def _loop(self):
        self.session_start=datetime.now().isoformat(); self.round_started_at=self.session_start
        await self._setup_grid()
        while self.running:
            await self._wait_next_tick()
            if self.running:
                await self._monitor()

    async def _wait_next_tick(self):
        """
        서버 표준시간 기반 모듈러 연산으로 정확한 타임슬롯 대기.
        offset = channel_id % 6  → 매 6초 주기 중 해당 초에 실행.
        ch07→1초, ch08→2초, ch12→0초, ch13→1초 ...
        """
        import time as _time
        offset = self.channel_id % 6
        cycle  = settings.CHANNEL_CYCLE_SECONDS  # 6

        now     = _time.time()
        # 현재 주기 내 위치 (0.0 ~ 5.999...)
        pos_in_cycle = now % cycle
        # 이번 주기에서 offset까지 남은 시간
        wait = (offset - pos_in_cycle) % cycle
        # 너무 짧으면(< 0.05s) 다음 주기 offset으로
        if wait < 0.05:
            wait += cycle

        await asyncio.sleep(wait)

    async def start(self, api):
        if self.running: return
        self.api=api; self.running=True; self.started_ts=time.time()
        self._task=asyncio.create_task(self._loop()); self._log("시작")

    def stop(self, reason="수동종료"):
        self.running=False
        if self._task: self._task.cancel()
        self.sessions.append({"round":self.round_idx,"started_at":self.round_started_at,"ended_at":datetime.now().isoformat(),"fill_count":self.round_fill_count,"pnl_krw":self.pnl_krw,"coin_delta":self.coin_delta,"reason":reason})
        self.round_fill_count=0; self.round_started_at=None; self.resting=False
        self.rest_anchor_price=None; self.rest_anchor_side=None
        self.filled_uuids=set(); self.pending=set(); self.orders={}
        self._log(f"정지: {reason}")

    def get_status(self):
        elapsed_h=(time.time()-self.started_ts)/3600 if self.started_ts else 0
        cur=self.cur_price or self.start_center; mavg=(self.start_center+cur)/2
        return {"slot":self.slot,"symbol":self.symbol,"running":self.running,"center":self.center,"start_center":self.start_center,"unit":self.unit,"init_h":self.init_h,"limit":self.limit,"sell_adj":self.sell_adj,"trend":self.trend,"round_idx":self.round_idx,"round_fill_count":self.round_fill_count,"resting":self.resting,"rest_anchor_price":self.rest_anchor_price,"rest_anchor_side":self.rest_anchor_side,"cur_price":cur,"pnl_krw":self.pnl_krw,"coin_delta":self.coin_delta,"grid_pnl":round(self.pnl_krw+self.coin_delta*mavg),"eval_pnl":round(self.coin_delta*(cur-mavg)),"fill_count":len(self.fills),"elapsed_h":round(elapsed_h,1),"orders":list(self.orders.values()),"fills":self.fills[-50:],"sessions":self.sessions,"log":self.log[:20]}

# ── 채널 접근 확인 ────────────────────────────────────────────────────
async def _check_access(channel_id, user):
    async with AsyncSessionLocal() as session:
        res=await session.execute(select(Channel).where(Channel.channel_id==channel_id))
        ch=res.scalar_one_or_none()
    if not ch: raise HTTPException(status_code=404,detail="채널 없음")
    if user.role!="admin" and ch.owner_id!=user.id: raise HTTPException(status_code=403,detail="권한 없음")
    return ch

async def _make_api(owner_id):
    async with AsyncSessionLocal() as session:
        res=await session.execute(select(User).where(User.id==owner_id))
        user=res.scalar_one_or_none()
    if not user or not user.bithumb_api_key_enc:
        raise HTTPException(status_code=400,detail="빗썸 API 키 미등록 — 마이페이지에서 먼저 등록하세요.")
    return BithumbAPIBasic(decrypt_api_key(user.bithumb_api_key_enc),decrypt_api_key(user.bithumb_api_secret_enc or ""))

# ── 라우트 ────────────────────────────────────────────────────────────
@router.get("/{channel_id}/grid",response_class=HTMLResponse)
async def channel_grid_page(channel_id:int, request:Request, user:User=Depends(get_current_user)):
    ch=await _check_access(channel_id,user)
    return templates.TemplateResponse("user/channel_crypto_basic.html",{"request":request,"user":user,"channel":ch,"channel_id":channel_id})

@router.websocket("/{channel_id}/grid/ws")
async def channel_grid_ws(channel_id:int, websocket:WebSocket):
    await websocket.accept()
    token=websocket.cookies.get("jt247_token")
    from app.core.security import decode_token
    if not decode_token(token): await websocket.send_json({"error":"Unauthorized"}); await websocket.close(4001); return
    if channel_id not in _ws_pool: _ws_pool[channel_id]=set()
    _ws_pool[channel_id].add(websocket)
    slots=_get_runners(channel_id)
    for sk,runner in slots.items():
        if runner:
            await websocket.send_json({"type":"grid_status","slot":sk,"data":runner.get_status()})
        else:
            # 실행 중인 러너 없음 → 클라이언트에 빈 상태 전달 (폼 초기화)
            await websocket.send_json({"type":"grid_status","slot":sk,
                                       "data":{"slot":sk,"running":False,"symbol":None}})
    try:
        while True: await asyncio.wait_for(websocket.receive_text(),timeout=30.0)
    except: pass
    finally:
        if channel_id in _ws_pool: _ws_pool[channel_id].discard(websocket)

@router.post("/{channel_id}/grid/{slot}/start")
async def start_grid(channel_id:int, slot:str, body:dict, user:User=Depends(get_current_user)):
    slot=slot.upper()
    if slot not in ("A","B"): raise HTTPException(400,"슬롯은 A 또는 B")
    ch=await _check_access(channel_id,user); api=await _make_api(ch.owner_id)
    sym=body.get("symbol","").upper()
    if not sym: return {"ok":False,"error":"코인명 필수"}
    slots=_get_runners(channel_id); old=slots.get(slot)
    if old and old.running: return {"ok":False,"error":f"슬롯 {slot} 이미 실행 중"}
    runner=CryptoGrid2Runner(slot=slot,channel_id=channel_id,symbol=sym,center=float(body["center"]),unit=float(body["unit"]),init_h=float(body.get("init_h",1.0)),limit=int(body.get("limit",6)),sell_adj=float(body.get("sell_adj",0)),trend=body.get("trend","중"))
    if old: runner.sessions=old.sessions; runner.round_idx=old.round_idx; runner.coin_delta=old.coin_delta; runner.pnl_krw=old.pnl_krw
    slots[slot]=runner; await runner.start(api)
    return {"ok":True,"slot":slot,"symbol":sym}

@router.post("/{channel_id}/grid/{slot}/stop")
async def stop_grid(channel_id:int, slot:str, body:dict={}, user:User=Depends(get_current_user)):
    slot=slot.upper(); await _check_access(channel_id,user)
    runner=_get_runners(channel_id).get(slot)
    if not runner: return {"ok":False,"error":"실행기 없음"}
    reason=body.get("reason","수동종료") if body else "수동종료"
    runner.stop(reason)
    await _broadcast_channel(channel_id,{"type":"grid_status","slot":slot,"data":runner.get_status()})
    return {"ok":True}

@router.get("/{channel_id}/grid/state")
async def grid_state(channel_id:int, user:User=Depends(get_current_user)):
    await _check_access(channel_id,user); slots=_get_runners(channel_id)
    return {s:r.get_status() if r else {"slot":s,"running":False} for s,r in slots.items()}



# ── 빗썸 잔고 조회 helper (디스코드 알림용) ──────────────────────
async def _get_bithumb_portfolio(owner_id: str) -> dict:
    """채널 소유자 빗썸 API로 잔고 조회 → {ok, total_krw, cash_krw, coin_krw, items}"""
    from sqlalchemy import select as _select
    async with AsyncSessionLocal() as session:
        res  = await session.execute(_select(User).where(User.id == owner_id))
        user = res.scalar_one_or_none()
    if not user or not user.bithumb_api_key_enc:
        return {"ok": False}
    import time, urllib.parse, hashlib, uuid as _uuid
    import jwt as pyjwt, requests as _req
    ak = decrypt_api_key(user.bithumb_api_key_enc)
    sk = decrypt_api_key(user.bithumb_api_secret_enc or "")
    api = BithumbAPIBasic(ak, sk)

    try:
        raw = api._get("/v1/accounts")
        if not isinstance(raw, list):
            return {"ok": False}
        cash_krw = 0.0; coin_items = []; coin_total = 0.0
        for a in raw:
            cur = a.get("currency","")
            qty = float(a.get("balance") or 0) + float(a.get("locked") or 0)
            if qty <= 0: continue
            if cur == "KRW":
                cash_krw = qty
            else:
                t = api.ticker(cur)
                price = float(t["data"]["closing_price"]) if t.get("status")=="0000" else 0
                if price > 0:
                    krw = qty * price
                    coin_total += krw
                    coin_items.append({"symbol": cur, "qty": qty, "price": price, "krw": round(krw)})
        return {"ok": True, "total_krw": round(cash_krw + coin_total),
                "cash_krw": round(cash_krw), "coin_krw": round(coin_total),
                "items": sorted(coin_items, key=lambda x: -x["krw"])}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@router.get("/{channel_id}/grid/default_tick/{symbol}")
async def default_tick(channel_id:int, symbol:str, user:User=Depends(get_current_user)):
    ch=await _check_access(channel_id,user); api=await _make_api(ch.owner_id)
    try:
        t=await asyncio.to_thread(api.ticker,symbol.upper())
        cur=float(t["data"]["closing_price"]); u=_tick_unit(cur)
        raw=cur*0.006; unit=max(u,round(raw/u)*u)
        return {"cur_price":cur,"unit":round(unit,4)}
    except Exception as e: return {"error":str(e)}


# ── 디스코드 알림 엔드포인트 ──────────────────────────────────────
@router.post("/{channel_id}/alert/save")
async def alert_save(channel_id: int, body: dict,
                     user: User = Depends(get_current_user)):
    """Webhook URL + 주기 저장 (아직 시작하지 않음)."""
    await _check_access(channel_id, user)
    webhook  = body.get("webhook", "").strip()
    interval = body.get("interval", "1h")
    if webhook and not webhook.startswith("https://discord.com/api/webhooks/"):
        return {"ok": False, "error": "Discord Webhook URL 형식이 올바르지 않습니다."}

    ch = await _check_access(channel_id, user)
    alert = dc_svc.get_or_create(channel_id)

    def state_provider():
        slots = _get_runners(channel_id)
        return [r.get_status() for r in slots.values() if r and r.running]

    async def bithumb_provider():
        return await _get_bithumb_portfolio(ch.owner_id)

    alert.configure(webhook, interval, state_provider,
                    bithumb_provider=bithumb_provider)
    return {"ok": True, "interval": interval}


@router.post("/{channel_id}/alert/toggle")
async def alert_toggle(channel_id: int,
                       user: User = Depends(get_current_user)):
    """알림 ON/OFF 토글."""
    await _check_access(channel_id, user)
    alert = dc_svc.get_or_create(channel_id)
    if not alert.webhook:
        return {"ok": False, "error": "Webhook URL을 먼저 저장하세요."}

    if alert.enabled:
        await alert.stop()
        return {"ok": True, "enabled": False}
    else:
        ch = await _check_access(channel_id, user)
        if not alert._state_provider:
            def state_provider():
                slots = _get_runners(channel_id)
                return [r.get_status() for r in slots.values() if r and r.running]
            async def bithumb_provider():
                return await _get_bithumb_portfolio(ch.owner_id)
            alert.configure(alert.webhook, "1h", state_provider,
                             bithumb_provider=bithumb_provider)
        await alert.start()
        return {"ok": True, "enabled": True}


@router.post("/{channel_id}/alert/send_now")
async def alert_send_now(channel_id: int,
                         user: User = Depends(get_current_user)):
    """즉시 1회 전송."""
    await _check_access(channel_id, user)
    alert = dc_svc.get(channel_id)
    if not alert or not alert.webhook:
        return {"ok": False, "error": "Webhook URL을 먼저 저장하세요."}
    ok = await alert.send_now()
    return {"ok": ok}


@router.get("/{channel_id}/alert/status")
async def alert_status(channel_id: int,
                       user: User = Depends(get_current_user)):
    await _check_access(channel_id, user)
    alert = dc_svc.get(channel_id)
    return alert.get_status() if alert else {"enabled": False, "webhook": "", "interval_key": "1h"}


# ── Graceful Shutdown Hook ────────────────────────────────────────────────────
async def stop_all_runners():
    """lifespan 종료 시 호출 — 모든 채널 CryptoGrid2Runner 안전 정지."""
    tasks = []
    for ch_id, slots in _runners.items():
        for slot, runner in slots.items():
            if runner and runner.running:
                runner.stop(reason="서버 종료")
                if runner._task and not runner._task.done():
                    tasks.append(runner._task)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
