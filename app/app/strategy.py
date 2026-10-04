from dataclasses import dataclass
from typing import Tuple

from .config import Config
from .models import MarketState


@dataclass
class Signal:
    accepted: bool
    score: int
    reason: str


class PrecisionStrategy:
    def __init__(self, config: Config):
        self.config = config

    def evaluate(
        self,
        state: MarketState
    ) -> Signal:

        score = 0
        reasons = []

        # --------------------------------------------------
        # Basic market data
        # --------------------------------------------------

        if state.last_price <= 0:
            return Signal(
                False,
                0,
                "NO_PRICE"
            )

        if state.bid <= 0 or state.ask <= 0:
            return Signal(
                False,
                0,
                "NO_BOOK"
            )

        # --------------------------------------------------
        # 24h volume
        # --------------------------------------------------

        if (
            state.volume_24h_quote
            < self.config.min_24h_quote_volume
        ):
            return Signal(
                False,
                0,
                "LOW_24H_VOLUME"
            )

        score += 10

        # --------------------------------------------------
        # Spread
        # --------------------------------------------------

        spread = state.spread_pct

        if spread > self.config.max_spread_pct:
            return Signal(
                False,
                score,
                f"SPREAD_TOO_HIGH:{spread:.4f}"
            )

        score += 10

        if spread <= self.config.max_entry_spread_pct:
            score += 5

        # --------------------------------------------------
        # Buy pressure
        # --------------------------------------------------

        pressure_10s = state.buy_pressure(10)
        pressure_30s = state.buy_pressure(30)

        if (
            pressure_10s
            < self.config.min_buy_pressure_10s
        ):
            return Signal(
                False,
                score,
                f"LOW_BUY_PRESSURE_10S:{pressure_10s:.3f}"
            )

        score += 15

        if (
            pressure_30s
            < self.config.min_buy_pressure_30s
        ):
            return Signal(
                False,
                score,
                f"LOW_BUY_PRESSURE_30S:{pressure_30s:.3f}"
            )

        score += 10

        # --------------------------------------------------
        # Order book imbalance
        # --------------------------------------------------

        imbalance = state.orderbook_imbalance

        if (
            imbalance
            < self.config.min_orderbook_imbalance
        ):
            return Signal(
                False,
                score,
                f"WEAK_ORDERBOOK:{imbalance:.3f}"
            )

        score += 15

        # --------------------------------------------------
        # EMA trend
        # --------------------------------------------------

        if (
            state.ema20_1m is None
            or state.ema50_1m is None
            or state.ema50_5m is None
            or state.ema200_5m is None
        ):
            return Signal(
                False,
                score,
                "INSUFFICIENT_EMA_DATA"
            )

        if state.ema20_1m <= state.ema50_1m:
            return Signal(
                False,
                score,
                "1M_TREND_NOT_BULLISH"
            )

        score += 10

        if state.ema50_5m <= state.ema200_5m:
            return Signal(
                False,
                score,
                "5M_TREND_NOT_BULLISH"
            )

        score += 10

        # --------------------------------------------------
        # BTC protection
        # --------------------------------------------------

        if (
            abs(state.btc_move_10s_pct)
            > self.config.max_btc_move_10s_pct
        ):
            return Signal(
                False,
                score,
                f"BTC_VOLATILITY:{state.btc_move_10s_pct:.3f}"
            )

        score += 5

        # --------------------------------------------------
        # Anti-chasing
        # --------------------------------------------------

        move_5s = state.price_change_pct(5)

        if (
            move_5s
            > self.config.max_chase_5s_pct
        ):
            return Signal(
                False,
                score,
                f"CHASE_PROTECTION:{move_5s:.3f}"
            )

        score += 5

        # --------------------------------------------------
        # Short-term volume activity
        # --------------------------------------------------

        volume_10s = state.window_quote_volume(10)

        if volume_10s <= 0:
            return Signal(
                False,
                score,
                "NO_RECENT_VOLUME"
            )

        score += 5

        # --------------------------------------------------
        # Final score
        # --------------------------------------------------

        if score < self.config.min_score:
            return Signal(
                False,
                score,
                f"SCORE_TOO_LOW:{score}"
            )

        return Signal(
            True,
            score,
            (
                "ENTRY:"
                f"pressure10={pressure_10s:.3f},"
                f"pressure30={pressure_30s:.3f},"
                f"imbalance={imbalance:.3f},"
                f"spread={spread:.4f},"
                f"score={score}"
            )
  )
