"""
main.py — JaeTech247 AutoTrading Platform entry point
Oracle Cloud Ubuntu 22.04 aarch64 | FastAPI + Uvicorn
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.core.config import settings
from app.models.database import init_db
from app.routers import auth, user, admin
from app.routers.premium import router as premium_router
from app.routers.crypto_basic import router as crypto_basic_router
from app.routers.stock import router as stock_router
from app.services.scheduler import start_scheduler, stop_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=== JaeTech247 Platform Starting ===")
    await init_db()
    start_scheduler()
    # Ensure state directories exist
    import os
    os.makedirs("./data/stock_states", exist_ok=True)
    os.makedirs("./uploads", exist_ok=True)
    yield
    logger.info("=== JaeTech247 Platform Shutting Down ===")
    from trading.engine import trading_engine
    await trading_engine.stop_all()
    from app.routers.crypto_basic import stop_all_runners as crypto_stop
    await crypto_stop()
    from app.routers.stock import stop_all_runners as stock_stop
    await stock_stop()
    from app.services import discord_alert as dc_svc
    await dc_svc.stop_all()
    stop_scheduler()


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="JaeTech247 AutoTrading Platform",
    version="1.0.0",
    docs_url="/api/docs" if settings.DEBUG else None,
    redoc_url=None,
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.BASE_URL, "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files & uploads
import os
os.makedirs("app/static", exist_ok=True)
os.makedirs("uploads", exist_ok=True)
app.mount("/static",  StaticFiles(directory="app/static"), name="static")
app.mount("/uploads", StaticFiles(directory="uploads"),    name="uploads")

# Templates
templates = Jinja2Templates(directory="app/templates")

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(user.router)
app.include_router(admin.router)
app.include_router(premium_router)
app.include_router(crypto_basic_router)
app.include_router(stock_router)


# ── Root redirect ─────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/login")


# ── Exception handlers ────────────────────────────────────────────────────────
@app.exception_handler(401)
async def unauthorized_handler(request: Request, exc):
    if request.url.path.startswith(("/api/", "/premium/api/", "/stock/", "/channel/")):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/login")


@app.exception_handler(403)
async def forbidden_handler(request: Request, exc):
    return templates.TemplateResponse(
        "partials/error.html",
        {"request": request, "code": 403, "message": str(exc.detail)},
        status_code=403,
    )


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    return templates.TemplateResponse(
        "partials/error.html",
        {"request": request, "code": 404, "message": "페이지를 찾을 수 없습니다."},
        status_code=404,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        workers=1,       # Single worker — asyncio handles concurrency
        loop="uvloop",
        access_log=True,
        reload=settings.DEBUG,
    )
