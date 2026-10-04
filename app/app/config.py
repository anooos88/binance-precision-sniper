import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)

    if value is None or value == "":
        return default

    return float(value)


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None or value == "":
        return default

    return int(value)


@dataclass
class Config:
    # Binance
    rest_url: str = os.getenv(
        "BINANCE_REST_URL",
        "https://api.binance.com"
    )

    ws_url: str = os.getenv(
        "BINANCE_WS_URL",
        "wss://stream.binance.com:9443/stream"
    )

    # Starting paper balance
    starting_balance: float = env_float(
        "STARTING_BALANCE",
        1000.0
    )

    # Market filters
    min_24h_quote_volume: float = env_float(
        "MIN_24H_QUOTE_VOLUME",
        20_000_000
    )

    max_spread_pct: float = env_float(
        "MAX_SPREAD_PCT",
        0.05
    )

    max_entry_spread_pct: float = env_float(
        "MAX_ENTRY_SPREAD_PCT",
        0.03
    )

    # Buy pressure
    min_buy_pressure_10s: float = env_float(
        "MIN_BUY_PRESSURE_10S",
        0.68
    )

    min_buy_pressure_30s: float = env_float(
        "MIN_BUY_PRESSURE_30S",
        0.60
    )

    # Order book
    min_orderbook_imbalance: float = env_float(
        "MIN_ORDERBOOK_IMBALANCE",
        0.25
    )

    # Volume velocity
    min_volume_velocity: float = env_float(
        "MIN_VOLUME_VELOCITY",
        2.5
    )

    # BTC protection
    max_btc_move_10s_pct: float = env_float(
        "MAX_BTC_MOVE_10S_PCT",
        0.20
    )

    # Anti-chasing
    max_chase_5s_pct: float = env_float(
        "MAX_CHASE_5S_PCT",
        0.15
    )

    # Strategy score
    min_score: int = env_int(
        "MIN_SCORE",
        80
    )

    # Trade management
    stop_loss_pct: float = env_float(
        "STOP_LOSS_PCT",
        0.18
    )

    take_profit_pct: float = env_float(
        "TAKE_PROFIT_PCT",
        0.28
    )

    max_hold_seconds: int = env_int(
        "MAX_HOLD_SECONDS",
        20
    )

    # Simulated trading costs
    fee_rate: float = env_float(
        "FEE_RATE",
        0.001
    )

    slippage_bps: float = env_float(
        "SLIPPAGE_BPS",
        2.0
    )

    # Risk management
    risk_per_trade_pct: float = env_float(
        "RISK_PER_TRADE_PCT",
        0.10
    )

    max_position_pct: float = env_float(
        "MAX_POSITION_PCT",
        20.0
    )

    max_open_trades: int = env_int(
        "MAX_OPEN_TRADES",
        3
    )

    daily_loss_limit_pct: float = env_float(
        "DAILY_LOSS_LIMIT_PCT",
        1.0
    )

    # Dashboard
    dashboard_host: str = os.getenv(
        "DASHBOARD_HOST",
        "0.0.0.0"
    )

    dashboard_port: int = env_int(
        "DASHBOARD_PORT",
        8080
    )

    # Database
    database_path: str = os.getenv(
        "DATABASE_PATH",
        "data/sniper.db"
    )


CONFIG = Config()
