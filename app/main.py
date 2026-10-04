import asyncio
import logging
import signal
from typing import Dict

from .binance import BinanceMarketData
from .config import CONFIG
from .db import Database
from .models import MarketState
from .paper import PaperEngine
from .strategy import PrecisionStrategy


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


class SniperApp:
    def __init__(self):
        self.config = CONFIG

        self.database = Database(
            self.config.database_path
        )

        self.strategy = PrecisionStrategy(
            self.config
        )

        self.paper = PaperEngine(
            config=self.config,
            database=self.database,
            strategy=self.strategy
        )

        self.states: Dict[str, MarketState] = {}

        self.market_data = BinanceMarketData(
            config=self.config,
            states=self.states
        )

        self.running = True

    # --------------------------------------------------
    # Shutdown
    # --------------------------------------------------

    def stop(self):
        if not self.running:
            return

        logger.info("Shutdown requested...")

        self.running = False

    # --------------------------------------------------
    # Main processing loop
    # --------------------------------------------------

    async def processing_loop(self):
        logger.info("Paper trading engine started.")

        while self.running:

            try:
                self.paper.process(
                    self.states
                )

                await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Error in processing loop"
                )

                await asyncio.sleep(1)

    # --------------------------------------------------
    # Status loop
    # --------------------------------------------------

    async def status_loop(self):
        while self.running:

            try:
                equity = self.paper.mark_to_market(
                    self.states
                )

                open_positions = len(
                    self.paper.positions
                )

                logger.info(
                    "Equity=%.2f | Balance=%.2f | "
                    "OpenPositions=%d | Markets=%d",
                    equity,
                    self.paper.balance,
                    open_positions,
                    len(self.states)
                )

                await asyncio.sleep(10)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Error in status loop"
                )

                await asyncio.sleep(5)

    # --------------------------------------------------
    # Run
    # --------------------------------------------------

    async def run(self):

        logger.info(
            "======================================"
        )

        logger.info(
            "Binance Precision Sniper"
        )

        logger.info(
            "MODE: PAPER TRADING ONLY"
        )

        logger.info(
            "Starting balance: %.2f",
            self.config.starting_balance
        )

        logger.info(
            "======================================"
        )

        market_task = asyncio.create_task(
            self.market_data.run()
        )

        processing_task = asyncio.create_task(
            self.processing_loop()
        )

        status_task = asyncio.create_task(
            self.status_loop()
        )

        try:

            while self.running:
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            self.running = False

        finally:

            logger.info(
                "Stopping application..."
            )

            self.running = False

            market_task.cancel()
            processing_task.cancel()
            status_task.cancel()

            await asyncio.gather(
                market_task,
                processing_task,
                status_task,
                return_exceptions=True
            )

            self.database.close()

            logger.info(
                "Application stopped."
            )


async def async_main():

    app = SniperApp()

    loop = asyncio.get_running_loop()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM
    ):
        try:
            loop.add_signal_handler(
                sig,
                app.stop
            )
        except NotImplementedError:
            pass

    await app.run()


def main():
    try:
        asyncio.run(
            async_main()
        )

    except KeyboardInterrupt:
        logger.info(
            "Interrupted by user."
        )


if __name__ == "__main__":
    main()
