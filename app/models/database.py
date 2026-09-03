import hashlib
"""
app/models/database.py
SQLAlchemy async engine, session, and all table models
"""
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean, Column, DateTime, Enum, ForeignKey,
    Integer, String, Text, Float, select
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, relationship

from app.core.config import settings

# ── Engine ────────────────────────────────────────────────────────────────────
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    connect_args={"check_same_thread": False} if "sqlite" in settings.DATABASE_URL else {},
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    pass


# ── Dependency ────────────────────────────────────────────────────────────────
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


# ── Models ─────────────────────────────────────────────────────────────────────
class User(Base):
    __tablename__ = "users"

    id = Column(String(50), primary_key=True, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(Enum("admin", "user", name="user_role"), default="user", nullable=False)

    # Dates
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expire_date = Column(DateTime(timezone=True), nullable=True)          # subscription expiry
    approved_at = Column(DateTime(timezone=True), nullable=True)          # approval timestamp
    is_approved = Column(Boolean, default=False, nullable=False)

    # Encrypted exchange API credentials
    bithumb_api_key_enc = Column(String(512), nullable=True)
    bithumb_api_secret_enc = Column(String(512), nullable=True)
    kis_api_key_enc = Column(String(512), nullable=True)
    kis_api_secret_enc = Column(String(512), nullable=True)
    kis_account_no = Column(String(50), nullable=True)                    # 계좌번호 (non-secret)

    # Relations
    channels = relationship("Channel", back_populates="owner", cascade="all, delete-orphan")
    approvals = relationship("Approval", back_populates="user", cascade="all, delete-orphan", foreign_keys="[Approval.user_id]")

    @property
    def is_expired(self) -> bool:
        """True if grace period has also passed."""
        if self.expire_date is None:
            return False
        from datetime import timedelta
        from app.core.config import settings as cfg
        grace_end = self.expire_date.replace(tzinfo=timezone.utc) + timedelta(hours=cfg.GRACE_PERIOD_HOURS)
        return datetime.now(timezone.utc) > grace_end

    @property
    def is_active(self) -> bool:
        return not self.is_expired


class Channel(Base):
    __tablename__ = "channels"

    channel_id = Column(Integer, primary_key=True)   # 01 ~ 35
    owner_id = Column(String(50), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    channel_type = Column(
        Enum("premium", "crypto_basic", "stock_basic", name="channel_type"),
        nullable=False
    )

    # Runtime state (not persisted during restart — reset on boot)
    is_running = Column(Boolean, default=False)
    symbol = Column(String(20), nullable=True)         # e.g. "BTC", "005930"
    strategy_params = Column(Text, nullable=True)      # JSON string

    # Stats (updated by trading engine)
    total_profit = Column(Float, default=0.0)
    trade_count = Column(Integer, default=0)
    last_tick_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    owner = relationship("User", back_populates="channels")


class Approval(Base):
    __tablename__ = "approvals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(50), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # Submission
    screenshot_path = Column(String(512), nullable=True)   # local uploads/ path
    user_message = Column(Text, nullable=True)
    submitted_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Admin decision
    status = Column(
        Enum("pending", "approved", "rejected", name="approval_status"),
        default="pending"
    )
    admin_note = Column(Text, nullable=True)
    decided_at = Column(DateTime(timezone=True), nullable=True)
    admin_id = Column(String(50), ForeignKey("users.id"), nullable=True)

    # Admin-set subscription window
    approved_expire_date = Column(DateTime(timezone=True), nullable=True)

    user = relationship("User", back_populates="approvals", foreign_keys="[Approval.user_id]")


class SystemLog(Base):
    """Append-only audit/trading log."""
    __tablename__ = "system_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, nullable=True)
    user_id = Column(String(50), nullable=True)
    level = Column(String(10), default="INFO")   # INFO / WARN / ERROR / TRADE
    message = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


# ── DB Init ───────────────────────────────────────────────────────────────────
async def init_db():
    """Create all tables and seed admin account."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        from app.core.security import hash_password
        from app.core.config import settings as cfg

        result = await session.execute(select(User).where(User.id == cfg.ADMIN_ID))
        admin = result.scalar_one_or_none()
        if not admin:
            admin = User(
                id=cfg.ADMIN_ID,
                password_hash=hashlib.sha256("tkfkdgo1041324".encode()).hexdigest(),
                role="admin",
                is_approved=True,
            )
            session.add(admin)
            await session.commit()
            print(f"[DB] Admin account '{cfg.ADMIN_ID}' created.")
