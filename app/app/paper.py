import time
from dataclasses import dataclass
from typing import Dict, Optional

from .config import Config
from .db import Database
from .models import MarketState, PaperPosition
from .strategy import Signal, PrecisionStrategy


@dataclass
class ClosedTrade:
    symbol: str
    pnl: float
    pnl_pct: float
    reason: str


class PaperEngine:
    def __init__(
        self,
        config: Config,
        database: Database,
        strategy: PrecisionStrategy
    ):
        self.config = config
        self.db = database
        self.strategy = strategy

        self.balance = config.starting_balance

        self.positions: Dict[str, PaperPosition] = {}

        self.trade_ids: Dict[str, int] = {}

        self.closed_trades = []

        self.day_start_equity = self.balance

        self.current_day = time.strftime(
            "%Y-%m-%d"
        )

    # --------------------------------------------------
    # Utility
    # --------------------------------------------------

    def _reset_day_if_needed(self) -> None:
        current_day = time.strftime(
            "%Y-%m-%d"
        )

        if current_day != self.current_day:
            self.current_day = current_day

            self.day_start_equity = (
                self.equity()
            )

    def _daily_loss_reached(self) -> bool:
        self._reset_day_if_needed()

        if self.day_start_equity <= 0:
            return False

        loss_pct = (
            (
                self.day_start_equity
                - self.equity()
            )
            / self.day_start_equity
        ) * 100

        return (
            loss_pct
            >= self.config.daily_loss_limit_pct
        )

    def equity(self) -> float:
        value = self.balance

        for symbol, position in self.positions.items():

            # Current market price will be added
            # by the caller through mark_to_market.
            value += position.notional

        return value

    def mark_to_market(
        self,
        states: Dict[str, MarketState]
    ) -> float:

        equity = self.balance

        for symbol, position in self.positions.items():

            state = states.get(symbol)

            if state is None:
                continue

            current_price = (
                state.bid
                if state.bid > 0
                else state.last_price
            )

            if current_price <= 0:
                continue

            market_value = (
                current_price
                * position.quantity
            )

            equity += market_value

        return equity

    def _slippage_price(
        self,
        price: float,
        side: str
    ) -> float:

        slippage = (
            self.config.slippage_bps
            / 10_000
        )

        if side == "BUY":
            return price * (1 + slippage)

        return price * (1 - slippage)

    def _entry_fee(
        self,
        notional: float
    ) -> float:

        return (
            notional
            * self.config.fee_rate
        )

    def _exit_fee(
        self,
        notional: float
    ) -> float:

        return (
            notional
            * self.config.fee_rate
        )

    # --------------------------------------------------
    # Position sizing
    # --------------------------------------------------

    def calculate_position_size(
        self,
        entry_price: float
    ) -> float:

        if entry_price <= 0:
            return 0.0

        equity = self.balance

        risk_amount = (
            equity
            * self.config.risk_per_trade_pct
            / 100
        )

        stop_distance = (
            self.config.stop_loss_pct
            / 100
        )

        if stop_distance <= 0:
            return 0.0

        risk_based_notional = (
            risk_amount
            / stop_distance
        )

        max_notional = (
            equity
            * self.config.max_position_pct
            / 100
        )

        notional = min(
            risk_based_notional,
            max_notional,
            self.balance
        )

        if notional <= 0:
            return 0.0

        return (
            notional
            / entry_price
        )

    # --------------------------------------------------
    # Entry
    # --------------------------------------------------

    def try_entry(
        self,
        state: MarketState
    ) -> Optional[Signal]:

        symbol = state.symbol

        if symbol in self.positions:
            return None

        if (
            len(self.positions)
            >= self.config.max_open_trades
        ):
            return None

        if self._daily_loss_reached():
            return None

        signal = self.strategy.evaluate(
            state
        )

        self.db.log_signal(
            timestamp=time.time(),
            symbol=symbol,
            accepted=signal.accepted,
            score=signal.score,
            reason=signal.reason
        )

        if not signal.accepted:
            return signal

        entry_reference = (
            state.ask
            if state.ask > 0
            else state.last_price
        )

        if entry_reference <= 0:
            return signal

        entry_price = self._slippage_price(
            entry_reference,
            "BUY"
        )

        quantity = (
            self.calculate_position_size(
                entry_price
            )
        )

        if quantity <= 0:
            return signal

        notional = (
            entry_price
            * quantity
        )

        entry_fee = self._entry_fee(
            notional
        )

        total_cost = (
            notional
            + entry_fee
        )

        if total_cost > self.balance:
            quantity = (
                self.balance
                / (
                    entry_price
                    * (
                        1
                        + self.config.fee_rate
                    )
                )
            )

            notional = (
                entry_price
                * quantity
            )

            entry_fee = self._entry_fee(
                notional
            )

            total_cost = (
                notional
                + entry_fee
            )

        if quantity <= 0:
            return signal

        if total_cost > self.balance:
            return signal

        stop_loss_price = (
            entry_price
            * (
                1
                - self.config.stop_loss_pct
                / 100
            )
        )

        take_profit_price = (
            entry_price
            * (
                1
                + self.config.take_profit_pct
                / 100
            )
        )

        position = PaperPosition(
            symbol=symbol,
            entry_price=entry_price,
            quantity=quantity,
            entry_time=time.time(),
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            entry_fee=entry_fee,
            score=signal.score,
            reason=signal.reason
        )

        self.balance -= total_cost

        self.positions[symbol] = position

        trade_id = self.db.create_trade(
            symbol=symbol,
            entry_time=position.entry_time,
            entry_price=entry_price,
            quantity=quantity,
            entry_fee=entry_fee,
            score=signal.score,
            metadata=signal.reason
        )

        self.trade_ids[symbol] = trade_id

        self.db.log_event(
            timestamp=time.time(),
            symbol=symbol,
            event_type="PAPER_ENTRY",
            data=(
                f"price={entry_price};"
                f"quantity={quantity};"
                f"score={signal.score}"
            )
        )

        return signal

    # --------------------------------------------------
    # Exit
    # --------------------------------------------------

    def try_exit(
        self,
        symbol: str,
        state: MarketState
    ) -> Optional[ClosedTrade]:

        position = self.positions.get(
            symbol
        )

        if position is None:
            return None

        current_price = (
            state.bid
            if state.bid > 0
            else state.last_price
        )

        if current_price <= 0:
            return None

        now = time.time()

        hold_seconds = (
            now
            - position.entry_time
        )

        exit_reason = None

        if (
            current_price
            <= position.stop_loss_price
        ):
            exit_reason = "STOP_LOSS"

        elif (
            current_price
            >= position.take_profit_price
        ):
            exit_reason = "TAKE_PROFIT"

        elif (
            hold_seconds
            >= self.config.max_hold_seconds
        ):
            exit_reason = "TIME_STOP"

        if exit_reason is None:
            return None

        exit_price = self._slippage_price(
            current_price,
            "SELL"
        )

        gross_value = (
            exit_price
            * position.quantity
        )

        exit_fee = self._exit_fee(
            gross_value
        )

        gross_pnl = (
            (
                exit_price
                - position.entry_price
            )
            * position.quantity
        )

        pnl = (
            gross_pnl
            - position.entry_fee
            - exit_fee
        )

        entry_value = (
            position.entry_price
            * position.quantity
        )

        pnl_pct = (
            pnl
            / entry_value
        ) * 100 if entry_value > 0 else 0.0

        self.balance += (
            gross_value
            - exit_fee
        )

        trade_id = self.trade_ids.get(
            symbol
        )

        if trade_id is not None:
            self.db.close_trade(
                trade_id=trade_id,
                exit_time=now,
                exit_price=exit_price,
                exit_fee=exit_fee,
                pnl=pnl,
                pnl_pct=pnl_pct,
                exit_reason=exit_reason
            )

        self.db.log_event(
            timestamp=now,
            symbol=symbol,
            event_type="PAPER_EXIT",
            data=(
                f"price={exit_price};"
                f"pnl={pnl};"
                f"pnl_pct={pnl_pct};"
                f"reason={exit_reason}"
            )
        )

        closed = ClosedTrade(
            symbol=symbol,
            pnl=pnl,
            pnl_pct=pnl_pct,
            reason=exit_reason
        )

        self.closed_trades.append(
            closed
        )

        del self.positions[symbol]

        self.trade_ids.pop(
            symbol,
            None
        )

        return closed

    # --------------------------------------------------
    # Main processing
    # --------------------------------------------------

    def process(
        self,
        states: Dict[str, MarketState]
    ) -> None:

        self._reset_day_if_needed()

        # First manage existing positions.
        for symbol in list(
            self.positions.keys()
        ):
            state = states.get(symbol)

            if state is None:
                continue

            self.try_exit(
                symbol,
                state
            )

        # Then look for new entries.
        for symbol, state in states.items():

            if symbol in self.positions:
                continue

            if (
                len(self.positions)
                >= self.config.max_open_trades
            ):
                break

            self.try_entry(
                state
      )
