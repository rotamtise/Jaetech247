"""
app/services/scheduler.py
APScheduler async jobs:
  - Remove unverified accounts older than UNVERIFIED_ACCOUNT_TTL_DAYS
  - Log expired user warnings (grace period tracking)
"""
import logging
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, delete

from app.core.config import settings
from app.models.database import AsyncSessionLocal, User, SystemLog

logger = logging.getLogger("scheduler")

scheduler = AsyncIOScheduler(timezone="UTC")


@scheduler.scheduled_job("interval", hours=6, id="cleanup_unverified")
async def cleanup_unverified_accounts():
    """Delete accounts created >7 days ago that never submitted an approval."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.UNVERIFIED_ACCOUNT_TTL_DAYS)
    async with AsyncSessionLocal() as session:
        # Find unapproved non-admin users older than cutoff
        result = await session.execute(
            select(User).where(
                User.is_approved == False,
                User.role == "user",
                User.created_at < cutoff,
            )
        )
        stale = result.scalars().all()
        for u in stale:
            log = SystemLog(
                level="WARN",
                message=f"미승인 계정 자동 삭제: {u.id} (생성: {u.created_at.date()})",
            )
            session.add(log)
            await session.delete(u)
        if stale:
            await session.commit()
            logger.info(f"Cleaned up {len(stale)} unverified accounts.")


@scheduler.scheduled_job("interval", hours=1, id="check_expiry")
async def check_expiry_warnings():
    """Log warnings for accounts within 24h of expiry (inside grace period)."""
    now = datetime.now(timezone.utc)
    warning_threshold = now + timedelta(hours=settings.GRACE_PERIOD_HOURS)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(
                User.is_approved == True,
                User.role == "user",
                User.expire_date.between(now, warning_threshold),
            )
        )
        expiring = result.scalars().all()
        for u in expiring:
            hours_left = (u.expire_date.replace(tzinfo=timezone.utc) - now).total_seconds() / 3600
            log = SystemLog(
                user_id=u.id,
                level="WARN",
                message=f"만료 임박: {u.id} ({hours_left:.1f}시간 후 만료)",
            )
            session.add(log)
        if expiring:
            await session.commit()


def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        logger.info("APScheduler started.")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
