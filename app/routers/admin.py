"""
app/routers/admin.py
Admin dashboard: user management, approval handling, channel overview
"""
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import Approval, Channel, SystemLog, User, get_db
from app.routers.auth import require_admin

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="app/templates")


# ── Admin Dashboard ───────────────────────────────────────────────────────────
@router.get("", response_class=HTMLResponse)
async def admin_dashboard(
    request: Request,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    # Aggregate stats
    total_users = (await db.execute(select(func.count()).select_from(User))).scalar()
    active_users = (await db.execute(
        select(func.count()).select_from(User).where(User.is_approved == True)
    )).scalar()
    pending_approvals = (await db.execute(
        select(func.count()).select_from(Approval).where(Approval.status == "pending")
    )).scalar()

    # Channel overview
    channels_result = await db.execute(
        select(Channel).order_by(Channel.channel_id)
    )
    channels = channels_result.scalars().all()

    # Recent logs
    logs_result = await db.execute(
        select(SystemLog).order_by(SystemLog.created_at.desc()).limit(50)
    )
    logs = logs_result.scalars().all()

    return templates.TemplateResponse("admin/dashboard.html", {
        "request": request,
        "admin": admin,
        "total_users": total_users,
        "active_users": active_users,
        "pending_approvals": pending_approvals,
        "channels": channels,
        "logs": logs,
        "now": datetime.now(timezone.utc),
    })


# ── User Management ───────────────────────────────────────────────────────────
@router.get("/users", response_class=HTMLResponse)
async def user_list(
    request: Request,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    users = result.scalars().all()
    return templates.TemplateResponse("admin/users.html", {
        "request": request, "admin": admin, "users": users,
        "now": datetime.now(timezone.utc),
    })


class ExpireDateUpdate(BaseModel):
    user_id: str
    expire_date: datetime
    extend_days: Optional[int] = None


@router.post("/users/expire")
async def update_expire_date(
    data: ExpireDateUpdate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).where(User.id == data.user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")

    if data.extend_days:
        base = user.expire_date or datetime.now(timezone.utc)
        user.expire_date = base + timedelta(days=data.extend_days)
    else:
        user.expire_date = data.expire_date

    await db.commit()
    return {"message": f"{data.user_id}의 만료일이 업데이트되었습니다.", "expire_date": str(user.expire_date)}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    if user_id == admin.id:
        raise HTTPException(status_code=400, detail="자기 자신을 삭제할 수 없습니다.")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    await db.delete(user)
    await db.commit()
    return {"message": f"사용자 {user_id}가 삭제되었습니다."}


# ── Approval Management ───────────────────────────────────────────────────────
@router.get("/approvals", response_class=HTMLResponse)
async def approvals_list(
    request: Request,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Approval).order_by(Approval.submitted_at.desc()).limit(100)
    )
    approvals = result.scalars().all()
    return templates.TemplateResponse("admin/approvals.html", {
        "request": request, "admin": admin, "approvals": approvals,
    })


class ApprovalDecision(BaseModel):
    approval_id: int
    decision: str = Field(..., pattern=r"^(approved|rejected)$")
    admin_note: str = ""
    expire_date: Optional[datetime] = None   # set subscription end date on approval


@router.post("/approvals/decide")
async def decide_approval(
    data: ApprovalDecision,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Approval).where(Approval.id == data.approval_id))
    approval = result.scalar_one_or_none()
    if not approval:
        raise HTTPException(status_code=404, detail="승인 요청을 찾을 수 없습니다.")

    approval.status = data.decision
    approval.admin_note = data.admin_note
    approval.decided_at = datetime.now(timezone.utc)
    approval.admin_id = admin.id

    # Update user record
    u_result = await db.execute(select(User).where(User.id == approval.user_id))
    user = u_result.scalar_one_or_none()
    if user:
        if data.decision == "approved":
            user.is_approved = True
            user.approved_at = datetime.now(timezone.utc)
            if data.expire_date:
                user.expire_date = data.expire_date
                approval.approved_expire_date = data.expire_date

    await db.commit()
    return {"message": f"승인 요청이 {'승인' if data.decision == 'approved' else '반려'}되었습니다."}


# ── Channel Management ────────────────────────────────────────────────────────
@router.get("/channels")
async def channel_overview(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Channel).order_by(Channel.channel_id)
    )
    channels = result.scalars().all()
    return [
        {
            "channel_id": ch.channel_id,
            "owner_id": ch.owner_id,
            "channel_type": ch.channel_type,
            "is_running": ch.is_running,
            "symbol": ch.symbol,
            "total_profit": ch.total_profit,
            "trade_count": ch.trade_count,
            "last_tick_at": ch.last_tick_at.isoformat() if ch.last_tick_at else None,
        }
        for ch in channels
    ]


@router.post("/channels/assign")
async def assign_channel(
    channel_id: int,
    user_id: str,
    channel_type: str,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    if not (1 <= channel_id <= 35):
        raise HTTPException(status_code=400, detail="채널 번호는 1~35 사이여야 합니다.")

    result = await db.execute(select(Channel).where(Channel.channel_id == channel_id))
    ch = result.scalar_one_or_none()

    if ch:
        ch.owner_id = user_id
        ch.channel_type = channel_type
    else:
        ch = Channel(channel_id=channel_id, owner_id=user_id, channel_type=channel_type)
        db.add(ch)

    await db.commit()
    return {"message": f"채널 {channel_id:02d}이 {user_id}에게 할당되었습니다."}
