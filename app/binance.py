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

    # Binance stream safety limit.
    MAX_STREAMS_PER_CONNECTION = 900

    # Streams used per symbol:
    # trade + bookTicker + depth + 1m kline + 5m kline
    STREAMS_PER_SYMBOL = 5

    MAX_SYMBOLS_PER_CONNECTION = (
        MAX_STREAMS_PER_CONNECTION
        // STREAMS_PER_SYMBOL
    )

    # Minimum 24h quote volume for liquid symbols.
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

    # ==================================================
    # HTTP SESSION
    # ==================================================

    async def create_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session is not None:
            await self.session.close()
            self.session = None

    # ==================================================
    # LOAD ACTIVE USDT SPOT SYMBOLS
    # ==================================================

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

    # ==================================================
    # REFRESH 24H VOLUME
    # ==================================================

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

    # ==================================================
    # FETCH HISTORICAL KLINES
    # ==================================================

    async def fetch_historical_klines(
        self,
        symbol: str,
        interval: str,
        limit: int
    ):

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

    # ==================================================
    # LOAD HISTORICAL DATA
    # ==================================================

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

                logger.info(
                    "Loading historical data: %s",
                    symbol.upper()
                )

                # ------------------------------------------
                # 300 x 1m candles
                # ------------------------------------------

                candles_1m = (
                    await self.fetch_historical_klines(
                        symbol,
                        "1m",
                        self.HISTORICAL_1M_LIMIT
                    )
                )

                # ------------------------------------------
                # 300 x 5m candles
                # ------------------------------------------

                candles_5m = (
                    await self.fetch_historical_klines(
                        symbol,
                        "5m",
                        self.HISTORICAL_5M_LIMIT
                    )
                )

                # ------------------------------------------
                # Validate 1m history
                # ------------------------------------------

                if len(candles_1m) < 50:

                    logger.warning(
                        "%s: only %d 1m candles received",
                        symbol.upper(),
                        len(candles_1m)
                    )

                    continue

                # ------------------------------------------
                # Validate 5m history
                # ------------------------------------------

                if len(candles_5m) < 200:

                    logger.warning(
                        "%s: only %d 5m candles received",
                        symbol.upper(),
                        len(candles_5m)
                    )

                    continue

                # ------------------------------------------
                # Replace current history
                # ------------------------------------------

                state.candles_1m.clear()

                state.candles_5m.clear()

                state.candles_1m.extend(
                    candles_1m
                )

                state.candles_5m.extend(
                    candles_5m
                )

                # ------------------------------------------
                # Calculate 1m EMA
                # ------------------------------------------

                self.update_ema(
                    state,
                    "1m"
                )

                # ------------------------------------------
                # Calculate 5m EMA
                # ------------------------------------------

                self.update_ema(
                    state,
                    "5m"
                )

                # ------------------------------------------
                # Verify EMA readiness
                # ------------------------------------------

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
                    "1m=%d | "
                    "5m=%d | "
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

                # Small delay between symbols.
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

    # ==================================================
    # SELECT LIQUID SYMBOLS
    # ==================================================

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

        # BTCUSDT is required for BTC protection.
        if "btcusdt" in self.symbols:

            if "btcusdt" not in selected:

                if (
                    len(selected)
                    >= self.MAX_SYMBOLS_PER_CONNECTION
                ):

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

    # ==================================================
    # HANDLE TRADE
    # ==================================================

    def handle_trade(
        self,
        data: dict
    ):

        symbol = data.get("s")

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

    # ==================================================
    # HANDLE BOOK TICKER
    # ==================================================

    def handle_book_ticker(
        self,
        data: dict
    ):

        symbol = data.get("s")

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

    # ==================================================
    # HANDLE DEPTH
    # ==================================================

    def handle_depth(
        self,
        data: dict
    ):

        symbol = data.get("s")

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

    # ==================================================
    # HANDLE KLINE
    # ==================================================

    def handle_kline(
        self,
        data: dict
    ):

        kline = data.get("k")

        if not kline:
            return

        symbol = kline.get("s")

        if not symbol:
            return

        symbol_key = symbol.lower()

        state = self.states.get(
            symbol_key
        )

        if state is None:
            return

        interval = kline.get("i")

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

    # ==================================================
    # UPDATE CANDLE HISTORY
    # ==================================================

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

        # Keep only the latest 300 candles.
        if len(candles) > 300:

            del candles[:-300]

    # ==================================================
    # EMA CALCULATION
    # ==================================================

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

    # ==================================================
    # HANDLE WEBSOCKET MESSAGE
    # ==================================================

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

        data = message.get("data")

        if not isinstance(
            data,
            dict
        ):
            return

        event_type = data.get("e")

        if event_type == "trade":

            self.handle_trade(
                data
            )

        elif event_type == "bookTicker":

            self.handle_book_ticker(
                data
            )

     elif event_type == "bookTicker":
        self.handle_book_ticker(data)

    elif event_type == "depthUpdate":
        self.handle_depth(data)

    elif event_type == "kline":
        self.handle_kline(data)

    # --------------------------------------------------
    # Build stream list
    # --------------------------------------------------

    def build_streams(self) -> list[str]:
        streams = []

        for symbol in self.selected_symbols:
            s = symbol.lower()

            streams.append(f"{s}@trade")
            streams.append(f"{s}@bookTicker")
            streams.append(f"{s}@depth5@100ms")
            streams.append(f"{s}@kline_1m")
            streams.append(f"{s}@kline_5m")

        return streams

    # --------------------------------------------------
    # WebSocket
    # --------------------------------------------------

    async def websocket_loop(self):
        streams = self.build_streams()

        if not streams:
            raise RuntimeError("No Binance streams available.")

        url = self.config.ws_url + "?streams=" + "/".join(streams)

        logger.info("Prepared %d Binance WebSocket streams.", len(streams))
        logger.info("Connecting to Binance WebSocket...")

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=10_000_000,
        ) as websocket:

            logger.info("Binance WebSocket connected.")

            async for message in websocket:
                if not self.running:
                    break

                try:
                    payload = json.loads(message)
                    await self.handle_message(payload)

                except Exception:
                    logger.exception("Error processing Binance WebSocket message.")

    # --------------------------------------------------
    # Main loop
    # --------------------------------------------------

    async def run(self):
        self.running = True

        await self.load_symbols()

        await self.refresh_24h_volume()

        self.select_stream_symbols()

        # Load historical candles before starting the live stream.
        # This provides enough data for EMA20/EMA50/EMA200.
        await self.load_historical_klines()

        backoff = 1

        while self.running:
            try:
                if time.time() - self.last_volume_refresh >= 300:
                    await self.refresh_24h_volume()
                    self.select_stream_symbols()

                await self.websocket_loop()

                backoff = 1

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                logger.error("Binance connection error: %s", exc)

                if not self.running:
                    break

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    # --------------------------------------------------
    # Stop
    # --------------------------------------------------

    async def stop(self):
        self.running = False

        if self.session is not None:
            await self.session.close()
            self.session = None
