import asyncio
import json
import logging
import time
from typing import Dict, List

import aiohttp
import websockets

from .config import Config
from .models import MarketState, TradeTick


logger = logging.getLogger(__name__)


class BinanceMarketData:
    """
    Binance Spot market-data collector.

    Paper-trading only.
    No order execution is performed here.
    """

    # We use 5 streams per symbol:
    # trade + bookTicker + depth + 1m kline + 5m kline
    #
    # Binance documents a maximum of 1024 streams per connection.
    # Keep a safety margin instead of using the absolute maximum.
    MAX_STREAMS_PER_CONNECTION = 900

    STREAMS_PER_SYMBOL = 5

    MAX_SYMBOLS_PER_CONNECTION = (
        MAX_STREAMS_PER_CONNECTION
        // STREAMS_PER_SYMBOL
    )

    # Minimum 24h quote volume used when selecting
    # symbols for the live WebSocket.
    MIN_LIQUIDITY_QUOTE_VOLUME = 20_000_000

    # Historical candles loaded before WebSocket startup.
    HISTORICAL_1M_LIMIT = 300
    HISTORICAL_5M_LIMIT = 300

    def __init__(
        self,
        config: Config,
        states: Dict[str, MarketState]
    ):
        self.config = config
        self.states = states

        self.session = None

        self.symbols: List[str] = []

        self.running = True

        self.last_volume_refresh = 0.0

    # --------------------------------------------------
    # HTTP session
    # --------------------------------------------------

    async def create_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session is not None:
            await self.session.close()
            self.session = None

    # --------------------------------------------------
    # Get active USDT Spot symbols
    # --------------------------------------------------

    async def load_symbols(self):
        await self.create_session()

        url = (
            self.config.rest_url
            + "/api/v3/exchangeInfo"
        )

        async with self.session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:

            response.raise_for_status()

            data = await response.json()

        symbols = []

        for item in data.get("symbols", []):

            symbol = item.get("symbol")
            status = item.get("status")
            quote_asset = item.get("quoteAsset")

            if not symbol:
                continue

            if status != "TRADING":
                continue

            if quote_asset != "USDT":
                continue

            # /api/v3/exchangeInfo is the Spot exchange-info
            # endpoint. Do not require the old "SPOT" permission
            # field because its presence/shape can vary.
            #
            # If Binance explicitly reports that Spot trading is
            # not allowed, reject the symbol.
            spot_allowed = item.get(
                "isSpotTradingAllowed"
            )

            if spot_allowed is False:
                continue

            symbols.append(
                symbol.lower()
            )

        self.symbols = sorted(
            set(symbols)
        )

        logger.info(
            "Loaded %d active USDT Spot symbols.",
            len(self.symbols)
        )

        for symbol in self.symbols:

            if symbol not in self.states:

                self.states[symbol] = MarketState(
                    symbol=symbol.upper()
                )

    # --------------------------------------------------
    # Refresh 24h volume
    # --------------------------------------------------

    async def refresh_24h_volume(self):

        await self.create_session()

        url = (
            self.config.rest_url
            + "/api/v3/ticker/24hr"
        )

        async with self.session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:

            response.raise_for_status()

            data = await response.json()

        for item in data:

            symbol = item.get(
                "symbol",
                ""
            ).lower()

            if symbol not in self.states:
                continue

            try:

                quote_volume = float(
                    item.get(
                        "quoteVolume",
                        0
                    )
                )

                self.states[
                    symbol
                ].volume_24h_quote = quote_volume

            except (
                TypeError,
                ValueError
            ):
                continue

        self.last_volume_refresh = time.time()

        logger.info(
            "24h volume data refreshed."
        )

    # --------------------------------------------------
    # Historical klines
    # --------------------------------------------------

    async def fetch_historical_klines(
        self,
        symbol: str,
        interval: str,
        limit: int
    ):
        """
        Load historical Binance Spot klines.

        Used during startup to initialize EMA history
        before the live WebSocket begins.
        """

        await self.create_session()

        url = (
            self.config.rest_url
            + "/api/v3/klines"
        )

        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }

        async with self.session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:

            response.raise_for_status()

            data = await response.json()

        candles = []

        for row in data:

            try:

                candle = {
                    "open_time": int(row[0]),
                    "close_time": int(row[6]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "closed": True,
                }

                candles.append(candle)

            except (
                TypeError,
                ValueError,
                IndexError
            ):
                continue

        return candles

    # --------------------------------------------------
    # Load historical klines for all selected symbols
    # --------------------------------------------------

    async def load_historical_klines(self):

        logger.info(
            "Loading historical klines for %d symbols...",
            len(self.symbols)
        )

        ready_count = 0

        for symbol in self.symbols:

            state = self.states.get(symbol)

            if state is None:
                continue

            try:

                candles_1m = (
                    await self.fetch_historical_klines(
                        symbol,
                        "1m",
                        self.HISTORICAL_1M_LIMIT
                    )
                )

                candles_5m = (
                    await self.fetch_historical_klines(
                        symbol,
                        "5m",
                        self.HISTORICAL_5M_LIMIT
                    )
                )

                if len(candles_1m) < 50:

                    logger.warning(
                        "%s: only %d 1m candles received",
                        symbol.upper(),
                        len(candles_1m)
                    )

                    continue

                if len(candles_5m) < 200:

                    logger.warning(
                        "%s: only %d 5m candles received",
                        symbol.upper(),
                        len(candles_5m)
                    )

                    continue

                state.candles_1m.clear()
                state.candles_5m.clear()

                state.candles_1m.extend(
                    candles_1m
                )

                state.candles_5m.extend(
                    candles_5m
                )

                # Calculate initial EMA values
                # before starting the live WebSocket.
                self.update_ema(
                    state,
                    "1m"
                )

                self.update_ema(
                    state,
                    "5m"
                )

                if (
                    state.ema20_1m is None
                    or state.ema50_1m is None
                    or state.ema50_5m is None
                    or state.ema200_5m is None
                ):

                    logger.warning(
                        "%s: EMA calculation not ready",
                        symbol.upper()
                    )

                    continue

                ready_count += 1

                logger.info(
                    "Historical EMA ready: %s | "
                    "1m=%d | 5m=%d | "
                    "EMA20_1m=%.8f | "
                    "EMA50_1m=%.8f | "
                    "EMA50_5m=%.8f | "
                    "EMA200_5m=%.8f",
                    symbol.upper(),
                    len(candles_1m),
                    len(candles_5m),
                    state.ema20_1m,
                    state.ema50_1m,
                    state.ema50_5m,
                    state.ema200_5m
                )

                # Small delay to avoid firing the entire
                # startup batch at exactly the same moment.
                await asyncio.sleep(0.05)

            except Exception as exc:

                logger.exception(
                    "Historical klines failed for %s: %s",
                    symbol.upper(),
                    exc
                )

        logger.info(
            "Historical EMA data loaded for %d/%d symbols.",
            ready_count,
            len(self.symbols)
        )

    # --------------------------------------------------
    # Select liquid symbols for WebSocket
    # --------------------------------------------------

    def select_stream_symbols(self):

        candidates = []

        for symbol in self.symbols:

            state = self.states.get(symbol)

            if state is None:
                continue

            volume = float(
                getattr(
                    state,
                    "volume_24h_quote",
                    0.0
                )
                or 0.0
            )

            if volume >= self.MIN_LIQUIDITY_QUOTE_VOLUME:

                candidates.append(
                    (symbol, volume)
                )

        # Highest-volume markets first.
        candidates.sort(
            key=lambda item: item[1],
            reverse=True
        )

        selected = [
            symbol
            for symbol, _ in candidates[
                :self.MAX_SYMBOLS_PER_CONNECTION
            ]
        ]

        # BTCUSDT is required for the BTC market-protection
        # filter used by the strategy.
        if "btcusdt" in self.symbols:

            if "btcusdt" not in selected:

                if len(selected) >= self.MAX_SYMBOLS_PER_CONNECTION:

                    selected = selected[
                        :self.MAX_SYMBOLS_PER_CONNECTION - 1
                    ]

                selected.append(
                    "btcusdt"
                )

        self.symbols = selected

        logger.info(
            "Selected %d liquid USDT symbols for WebSocket.",
            len(self.symbols)
        )

        if not self.symbols:

            logger.warning(
                "No USDT symbols passed the liquidity filter."
            )

    # --------------------------------------------------
    # Handle trade
    # --------------------------------------------------

    def handle_trade(
        self,
        data: dict
    ):

        symbol = data.get(
            "s"
        )

        if not symbol:
            return

        symbol_key = symbol.lower()

        state = self.states.get(
            symbol_key
        )

        if state is None:
            return

        try:

            price = float(
                data["p"]
            )

            quantity = float(
                data["q"]
            )

            timestamp = (
                float(data["T"])
                / 1000
            )

            is_buyer_maker = bool(
                data["m"]
            )

        except (
            KeyError,
            TypeError,
            ValueError
        ):
            return

        tick = TradeTick(
            symbol=symbol.upper(),
            price=price,
            quantity=quantity,
            timestamp=timestamp,
            is_buyer_maker=is_buyer_maker
        )

        state.add_trade(
            tick
        )

    # --------------------------------------------------
    # Handle book ticker
    # --------------------------------------------------

    def handle_book_ticker(
        self,
        data: dict
    ):

        symbol = data.get(
            "s"
        )

        if not symbol:
            return

        symbol_key = symbol.lower()

        state = self.states.get(
            symbol_key
        )

        if state is None:
            return

        try:

            state.bid = float(
                data["b"]
            )

            state.bid_qty = float(
                data["B"]
            )

            state.ask = float(
                data["a"]
            )

            state.ask_qty = float(
                data["A"]
            )

        except (
            KeyError,
            TypeError,
            ValueError
        ):
            return

    # --------------------------------------------------
    # Handle depth
    # --------------------------------------------------

    def handle_depth(
        self,
        data: dict
    ):

        symbol = data.get(
            "s"
        )

        if not symbol:
            return

        symbol_key = symbol.lower()

        state = self.states.get(
            symbol_key
        )

        if state is None:
            return

        bids = data.get(
            "b",
            []
        )

        asks = data.get(
            "a",
            []
        )

        try:

            bid_qty = sum(
                float(level[1])
                for level in bids[:5]
            )

            ask_qty = sum(
                float(level[1])
                for level in asks[:5]
            )

            state.bid_qty = bid_qty
            state.ask_qty = ask_qty

        except (
            TypeError,
            ValueError,
            IndexError
        ):
            return

    # --------------------------------------------------
    # Handle kline
    # --------------------------------------------------

    def handle_kline(
        self,
        data: dict
    ):

        kline = data.get(
            "k"
        )

        if not kline:
            return

        symbol = kline.get(
            "s"
        )

        if not symbol:
            return

        symbol_key = symbol.lower()

        state = self.states.get(
            symbol_key
        )

        if state is None:
            return

        interval = kline.get(
            "i"
        )

        try:

            candle = {
                "open_time": int(
                    kline["t"]
                ),
                "close_time": int(
                    kline["T"]
                ),
                "open": float(
                    kline["o"]
                ),
                "high": float(
                    kline["h"]
                ),
                "low": float(
                    kline["l"]
                ),
                "close": float(
                    kline["c"]
                ),
                "volume": float(
                    kline["v"]
                ),
                "closed": bool(
                    kline["x"]
                )
            }

        except (
            KeyError,
            TypeError,
            ValueError
        ):
            return

        if interval == "1m":

            self.update_candle(
                state.candles_1m,
                candle
            )

            self.update_ema(
                state,
                "1m"
            )

        elif interval == "5m":

            self.update_candle(
                state.candles_5m,
                candle
            )

            self.update_ema(
                state,
                "5m"
            )

    # --------------------------------------------------
    # Update candle history
    # --------------------------------------------------

    @staticmethod
    def update_candle(
        candles,
        candle
    ):

        if candles:

            last = candles[-1]

            if (
                last["open_time"]
                == candle["open_time"]
            ):

                candles[-1] = candle

                return

        candles.append(
            candle
        )

        # Prevent unlimited in-memory growth.
        # 1m history needs at least 50 candles.
        # 5m history needs at least 200 candles.
        if len(candles) > 300:

            del candles[:-300]

    # --------------------------------------------------
    # EMA
    # --------------------------------------------------

    @staticmethod
    def calculate_ema(
        candles,
        period: int
    ):

        if len(candles) < period:
            return None

        closes = [
            candle["close"]
            for candle in candles
        ]

        multiplier = (
            2
            / (period + 1)
        )

        ema = closes[0]

        for price in closes[1:]:

            ema = (
                (
                    price - ema
                )
                * multiplier
            ) + ema

        return ema

    def update_ema(
        self,
        state: MarketState,
        timeframe: str
    ):

        if timeframe == "1m":

            state.ema20_1m = (
                self.calculate_ema(
                    state.candles_1m,
                    20
                )
            )

            state.ema50_1m = (
                self.calculate_ema(
                    state.candles_1m,
                    50
                )
            )

        elif timeframe == "5m":

            state.ema50_5m = (
                self.calculate_ema(
                    state.candles_5m,
                    50
                )
            )

            state.ema200_5m = (
                self.calculate_ema(
                    state.candles_5m,
                    200
                )
            )

    # --------------------------------------------------
    # Handle incoming WebSocket message
    # --------------------------------------------------

    def handle_message(
        self,
        raw_message: str
    ):

        try:

            message = json.loads(
                raw_message
            )

        except json.JSONDecodeError:

            return

        data = message.get(
            "data"
        )

        if not isinstance(
            data,
            dict
        ):
            return

        event_type = data.get(
            "e"
        )

        if event_type == "trade":

            self.handle_trade(
                data
            )

        elif event_type == "bookTicker":

            self.handle_book_ticker(
                data
            )

        elif event_type == "depthUpdate":

            self.handle_depth(
                data
            )

        elif event_type == "kline":

            self.handle_kline(
                data
            )

    # --------------------------------------------------
    # Build stream list
    # --------------------------------------------------

    def build_stre
