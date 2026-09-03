"""
app/routers/trading.py
WebSocket channel for real-time trading UI + REST control endpoints
"""
import asyncio
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.database import Channel, User, get_db
from app.routers.auth import get_current_user
from trading.engine import trading_engine

router = APIRouter(prefix="/channel", tags=["trading"])
templates = Jinja2Templates(directory="app/templates")


# ── Channel Room View ─────────────────────────────────────────────────────────
@router.get("/{channel_id}", response_class=HTMLResponse)
async def channel_room(
    channel_id: int,
    request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Channel).where(Channel.channel_id == channel_id))
    ch = result.scalar_one_or_none()

    if not ch:
        raise HTTPException(status_code=404, detail="채널을 찾을 수 없습니다.")

    # Non-admin users can only view own channels
    if user.role != "admin" and ch.owner_id != user.id:
        raise HTTPException(status_code=403, detail="접근 권한이 없습니다.")

    template_map = {
        "premium": "user/channel_premium.html",
        "crypto_basic": "user/channel_crypto.html",
        "stock_basic": "user/channel_stock.html",
    }
    tmpl = template_map.get(ch.channel_type, "user/channel_crypto.html")

    return templates.TemplateResponse(tmpl, {
        "request": request,
        "user": user,
        "channel": ch,
        "channel_id": channel_id,
    })


# ── WebSocket: Real-time channel feed ─────────────────────────────────────────
@router.websocket("/{channel_id}/ws")
async def channel_ws(
    websocket: WebSocket,
    channel_id: int,
    db: AsyncSession = Depends(get_db),
):
    await websocket.accept()
    try:
        # Auth via cookie
        token = websocket.cookies.get("jt247_token")
        from app.core.security import decode_token
        payload = decode_token(token) if token else None
        if not payload:
            await websocket.send_json({"error": "Unauthorized"})
            await websocket.close(code=4001)
            return

        user_id = payload.get("sub")

        # Register WebSocket with trading engine for push updates
        await trading_engine.register_ws(channel_id, websocket)

        # Send current state immediately
        state = trading_engine.get_channel_state(channel_id)
        await websocket.send_json({"type": "state", "data": state})

        # Keep alive & handle client commands
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                msg = json.loads(raw)
                cmd = msg.get("cmd")

                if cmd == "start":
                    await trading_engine.start_channel(channel_id, user_id, db)
                    await websocket.send_json({"type": "ack", "cmd": "start", "ok": True})
                elif cmd == "stop":
                    await trading_engine.stop_channel(channel_id)
                    await websocket.send_json({"type": "ack", "cmd": "stop", "ok": True})
                elif cmd == "ping":
                    await websocket.send_json({"type": "pong"})

            except asyncio.TimeoutError:
                # Send heartbeat
                await websocket.send_json({"type": "ping"})

    except WebSocketDisconnect:
        pass
    finally:
        await trading_engine.unregister_ws(channel_id, websocket)


# ── REST control endpoints ────────────────────────────────────────────────────
class ChannelStartRequest(BaseModel):
    symbol: str
    params: dict = {}


@router.post("/{channel_id}/start")
async def start_channel(
    channel_id: int,
    body: ChannelStartRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Channel).where(Channel.channel_id == channel_id))
    ch = result.scalar_one_or_none()
    if not ch or (user.role != "admin" and ch.owner_id != user.id):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    ch.symbol = body.symbol
    import json as _json
    ch.strategy_params = _json.dumps(body.params)
    await db.commit()

    ok = await trading_engine.start_channel(channel_id, user.id, db)
    return {"started": ok, "channel_id": channel_id}


@router.post("/{channel_id}/stop")
async def stop_channel(
    channel_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Channel).where(Channel.channel_id == channel_id))
    ch = result.scalar_one_or_none()
    if not ch or (user.role != "admin" and ch.owner_id != user.id):
        raise HTTPException(status_code=403, detail="권한이 없습니다.")

    ok = await trading_engine.stop_channel(channel_id)
    return {"stopped": ok, "channel_id": channel_id}


@router.get("/{channel_id}/state")
async def get_state(
    channel_id: int,
    user: User = Depends(get_current_user),
):
    state = trading_engine.get_channel_state(channel_id)
    return state or {"channel_id": channel_id, "running": False}
