import json
from pathlib import Path

from aiohttp import web

from .config import CONFIG
from .db import Database
from .report import PerformanceReport


BASE_DIR = Path(__file__).resolve().parent

database = Database(
    CONFIG.database_path
)

report_generator = PerformanceReport(
    config=CONFIG,
    database=database
)


# --------------------------------------------------
# Dashboard page
# --------------------------------------------------

async def dashboard(request):
    html_path = BASE_DIR / "dashboard.html"

    if not html_path.exists():
        return web.Response(
            text="dashboard.html not found",
            status=404,
            content_type="text/plain"
        )

    return web.FileResponse(
        html_path
    )


# --------------------------------------------------
# API
# --------------------------------------------------

async def api_data(request):

    report = report_generator.generate()

    open_trades = database.get_open_trades()

    recent_trades = database.get_closed_trades(
        limit=50
    )

    recent_signals = database.get_recent_signals(
        limit=100
    )

    data = {
        "report": report,

        "open_trades": [
            dict(row)
            for row in open_trades
        ],

        "recent_trades": [
            dict(row)
            for row in recent_trades
        ],

        "recent_signals": [
            dict(row)
            for row in recent_signals
        ]
    }

    return web.json_response(
        data
    )


# --------------------------------------------------
# Health check
# --------------------------------------------------

async def health(request):

    return web.json_response(
        {
            "status": "ok",
            "service": "binance-precision-sniper-dashboard"
        }
    )


# --------------------------------------------------
# App
# --------------------------------------------------

def create_app():

    app = web.Application()

    app.router.add_get(
        "/",
        dashboard
    )

    app.router.add_get(
        "/api",
        api_data
    )

    app.router.add_get(
        "/health",
        health
    )

    return app


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    app = create_app()

    web.run_app(
        app,
        host=CONFIG.dashboard_host,
        port=CONFIG.dashboard_port
    )


if __name__ == "__main__":
    main()
