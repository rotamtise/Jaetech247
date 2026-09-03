"""
app/routers/auth.py
Login, signup, logout, token refresh
"""
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import (
    create_access_token, create_refresh_token,
    decode_token, hash_password, verify_password
)
from app.models.database import User, get_db

router = APIRouter(tags=["auth"])
templates = Jinja2Templates(directory="app/templates")

COOKIE_KEY = "jt247_token"


# ── Pydantic Schemas ──────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)


class SignupRequest(BaseModel):
    user_id: str = Field(..., min_length=3, max_length=50, pattern=r"^[a-zA-Z0-9_]+$")
    password: str = Field(..., min_length=6, max_length=128)
    password_confirm: str


# ── Auth dependency ───────────────────────────────────────────────────────────
async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> User:
    token = request.cookies.get(COOKIE_KEY)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    result = await db.execute(select(User).where(User.id == payload.get("sub")))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    return user


async def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="관리자 권한이 필요합니다.")
    return user


async def require_active_user(user: User = Depends(get_current_user)) -> User:
    if user.role == "admin":
        return user
    if user.is_expired:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="서비스 이용 기간이 만료되었습니다.")
    return user


# ── Routes ────────────────────────────────────────────────────────────────────
@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, db: AsyncSession = Depends(get_db)):
    from app.services.stats import collect as collect_stats
    stats = collect_stats()

    return templates.TemplateResponse("auth/login.html", {
        "request":       request,
        "active_channels": stats.active_count,
        "stats":           stats,
    })


@router.post("/login")
async def login(
    data: LoginRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).where(User.id == data.user_id))
    user = result.scalar_one_or_none()

    if not user or not verify_password(data.password, user.password_hash):
        raise HTTPException(status_code=400, detail="아이디 또는 비밀번호가 올바르지 않습니다.")

    token = create_access_token({"sub": user.id, "role": user.role})
    response.set_cookie(
        key=COOKIE_KEY, value=token,
        httponly=True, secure=True, samesite="lax", max_age=60 * 480
    )

    redirect_url = "/admin" if user.role == "admin" else "/"
    return {"redirect": redirect_url, "role": user.role}


@router.post("/signup")
async def signup(data: SignupRequest, db: AsyncSession = Depends(get_db)):
    if data.password != data.password_confirm:
        raise HTTPException(status_code=400, detail="비밀번호가 일치하지 않습니다.")

    result = await db.execute(select(User).where(User.id == data.user_id))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="이미 사용 중인 아이디입니다.")

    user = User(
        id=data.user_id,
        password_hash=hash_password(data.password),
        role="user",
        is_approved=False,
    )
    db.add(user)
    await db.commit()
    return {"message": "계정이 생성되었습니다. 7일 이내에 승인 요청을 완료해 주세요.", "user_id": data.user_id}


@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie(COOKIE_KEY)
    return {"redirect": "/login"}


@router.get("/", response_class=RedirectResponse)
async def root(request: Request):
    if request.cookies.get(COOKIE_KEY):
        return RedirectResponse(url="/user/dashboard")
    return RedirectResponse(url="/login")
