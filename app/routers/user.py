"""
app/routers/user.py
User dashboard, mypage, channel view
"""
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import aiofiles
import uuid
from pathlib import Path
from datetime import datetime, timezone

from app.core.config import settings
from app.core.security import decrypt_api_key, encrypt_api_key, hash_password, verify_password
from app.models.database import Approval, Channel, User, get_db
from app.routers.auth import get_current_user, require_active_user

router = APIRouter(prefix="/user", tags=["user"])
templates = Jinja2Templates(directory="app/templates")


# ── Dashboard ─────────────────────────────────────────────────────────────────
@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Channels owned by user
    result = await db.execute(select(Channel).where(Channel.owner_id == user.id))
    channels = result.scalars().all()

    # Latest approval
    result2 = await db.execute(
        select(Approval)
        .where(Approval.user_id == user.id)
        .order_by(Approval.submitted_at.desc())
        .limit(1)
    )
    latest_approval = result2.scalar_one_or_none()

    return templates.TemplateResponse("user/dashboard.html", {
        "request": request,
        "user": user,
        "channels": channels,
        "latest_approval": latest_approval,
        "now": datetime.now(timezone.utc),
        "has_bithumb_key": bool(user.bithumb_api_key_enc),
        "has_kis_key":     bool(user.kis_api_key_enc),
    })


# ── Approval submission ───────────────────────────────────────────────────────
@router.post("/approve/submit")
async def submit_approval(
    request: Request,
    message: str = Form(""),
    screenshot: UploadFile = File(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Check for pending approval already
    result = await db.execute(
        select(Approval).where(
            Approval.user_id == user.id,
            Approval.status == "pending"
        )
    )
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="이미 승인 대기 중인 요청이 있습니다.")

    screenshot_path = None
    if screenshot and screenshot.filename:
        ext = Path(screenshot.filename).suffix.lower()
        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            raise HTTPException(status_code=400, detail="이미지 파일만 업로드 가능합니다.")
        fname = f"{user.id}_{uuid.uuid4().hex}{ext}"
        save_path = settings.UPLOAD_DIR / fname
        save_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(save_path, "wb") as f:
            content = await screenshot.read()
            if len(content) > settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024:
                raise HTTPException(status_code=400, detail="파일 크기가 너무 큽니다.")
            await f.write(content)
        screenshot_path = str(save_path)

    approval = Approval(
        user_id=user.id,
        screenshot_path=screenshot_path,
        user_message=message,
        status="pending",
    )
    db.add(approval)
    await db.commit()
    return {"message": "승인 요청이 제출되었습니다. 관리자 확인 후 처리됩니다."}


# ── MyPage: change password ───────────────────────────────────────────────────
class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=6)
    new_password_confirm: str


@router.post("/mypage/password")
async def change_password(
    data: PasswordChangeRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not verify_password(data.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="현재 비밀번호가 올바르지 않습니다.")
    if data.new_password != data.new_password_confirm:
        raise HTTPException(status_code=400, detail="새 비밀번호가 일치하지 않습니다.")

    result = await db.execute(select(User).where(User.id == user.id))
    u = result.scalar_one()
    u.password_hash = hash_password(data.new_password)
    await db.commit()
    return {"message": "비밀번호가 변경되었습니다."}


# ── MyPage: update API keys ───────────────────────────────────────────────────
class ApiKeyUpdateRequest(BaseModel):
    exchange: str = Field(..., pattern=r"^(bithumb|kis)$")
    api_key: str = Field(..., min_length=1)
    api_secret: str = Field(..., min_length=1)
    account_no: str = ""


@router.post("/mypage/apikey")
async def update_api_key(
    data: ApiKeyUpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).where(User.id == user.id))
    u = result.scalar_one()

    if data.exchange == "bithumb":
        u.bithumb_api_key_enc = encrypt_api_key(data.api_key)
        u.bithumb_api_secret_enc = encrypt_api_key(data.api_secret)
    else:  # kis
        u.kis_api_key_enc = encrypt_api_key(data.api_key)
        u.kis_api_secret_enc = encrypt_api_key(data.api_secret)
        u.kis_account_no = data.account_no

    await db.commit()
    return {"message": f"{data.exchange.upper()} API 키가 저장되었습니다."}


@router.get("/mypage/apikey/status")
async def api_key_status(user: User = Depends(get_current_user)):
    """Returns whether API keys are set (not the keys themselves)."""
    return {
        "bithumb": bool(user.bithumb_api_key_enc),
        "kis": bool(user.kis_api_key_enc),
    }
