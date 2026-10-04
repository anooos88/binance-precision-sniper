from dataclasses import dataclass, field
from collections import deque
from typing import Optional
import time


@dataclass
class TradeTick:
    symbol: str
    price: float
    quantity: float
    timestamp: float
    is_buyer_maker: bool

    @property
    def quote_volume(self) -> float:
        return self.price * self.quantity


@dataclass
class MarketState:
    symbol: str

    last_price: float = 0.0

    bid: float = 0.0
    ask: float = 0.0
    bid_qty: float = 0.0
    ask_qty: float = 0.0

    volume_24h_quote: float = 0.0

    ema20_1m: Optional[float] = None
    ema50_1m: Optional[float] = None

    ema50_5m: Optional[float] = None
    ema200_5m: Optional[float] = None

    btc_move_10s_pct: float = 0.0

    trades: deque = field(
        default_factory=lambda: deque(maxlen=5000)
    )

    prices: deque = field(
        default_factory=lambda: deque(maxlen=5000)
    )

    candles_1m: deque = field(
        default_factory=lambda: deque(maxlen=300)
    )

    candles_5m: deque = field(
        default_factory=lambda: deque(maxlen=300)
    )

    last_update: float = field(
        default_factory=time.time
    )

    @property
    def spread_pct(self) -> float:
        if self.bid <= 0 or self.ask <= 0:
            return 999.0

        mid = (self.bid + self.ask) / 2

        if mid <= 0:
            return 999.0

        return ((self.ask - self.bid) / mid) * 100

    @property
    def orderbook_imbalance(self) -> float:
        total = self.bid_qty + self.ask_qty

        if total <= 0:
            return 0.0

        return (self.bid_qty - self.ask_qty) / total

    def add_trade(self, tick: TradeTick) -> None:
        self.trades.append(tick)
        self.last_price = tick.price
        self.prices.append(
            (tick.timestamp, tick.price)
        )
        self.last_update = tick.timestamp

    def window_trades(
        self,
        seconds: float,
        now: Optional[float] = None
    ):
        if now is None:
            now = time.time()

        cutoff = now - seconds

        return [
            tick
            for tick in self.trades
            if tick.timestamp >= cutoff
        ]

    def buy_pressure(
        self,
        seconds: float,
        now: Optional[float] = None
    ) -> float:
        trades = self.window_trades(
            seconds,
            now
        )

        if not trades:
            return 0.0

        total = 0.0
        buy = 0.0

        for tick in trades:
            quote = tick.quote_volume

            total += quote

            # Binance is_buyer_maker=True means
            # the buyer was the maker, therefore
            # the aggressive side was the seller.
            if not tick.is_buyer_maker:
                buy += quote

        if total <= 0:
            return 0.0

        return buy / total

    def window_quote_volume(
        self,
        seconds: float,
        now: Optional[float] = None
    ) -> float:
        trades = self.window_trades(
            seconds,
            now
        )

        return sum(
            tick.quote_volume
            for tick in trades
        )

    def price_change_pct(
        self,
        seconds: float,
        now: Optional[float] = None
    ) -> float:
        if now is None:
            now = time.time()

        if self.last_price <= 0:
            return 0.0

        cutoff = now - seconds

        old_price = None

        for timestamp, price in reversed(self.prices):
            if timestamp <= cutoff:
                old_price = price
                break

        if old_price is None or old_price <= 0:
            return 0.0

        return (
            (self.last_price - old_price)
            / old_price
        ) * 100


@dataclass
class PaperPosition:
    symbol: str

    entry_price: float
    quantity: float

    entry_time: float

    stop_loss_price: float
    take_profit_price: float

    entry_fee: float = 0.0

    score: int = 0

    reason: str = ""

    @property
    def notional(self) -> float:
        return self.entry_price * self.quantity

    def unrealized_pnl(
        self,
        current_price: float
    ) -> float:
        return (
            current_price - self.entry_price
        ) * self.quantity
