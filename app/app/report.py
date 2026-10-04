import math
from collections import defaultdict
from typing import Any, Dict, List

from .config import Config
from .db import Database


class PerformanceReport:
    def __init__(
        self,
        config: Config,
        database: Database
    ):
        self.config = config
        self.db = database

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            if value is None:
                return default

            value = float(value)

            if not math.isfinite(value):
                return default

            return value

        except (TypeError, ValueError):
            return default

    # --------------------------------------------------
    # Main report
    # --------------------------------------------------

    def generate(self) -> Dict[str, Any]:

        trades = self.db.get_closed_trades()

        starting_balance = (
            self.config.starting_balance
        )

        if not trades:

            return {
                "starting_balance": starting_balance,
                "ending_balance": starting_balance,
                "net_pnl": 0.0,
                "net_pnl_pct": 0.0,
                "closed_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "expectancy": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "average_hold_seconds": 0.0,
                "exit_reasons": {},
                "symbols": {}
            }

        total_pnl = 0.0

        wins = 0
        losses = 0

        gross_profit = 0.0
        gross_loss = 0.0

        hold_times: List[float] = []

        exit_reasons = defaultdict(int)

        symbol_stats = defaultdict(
            lambda: {
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl": 0.0,
                "gross_profit": 0.0,
                "gross_loss": 0.0
            }
        )

        equity = starting_balance

        peak_equity = starting_balance

        max_drawdown = 0.0
        max_drawdown_pct = 0.0

        # --------------------------------------------------
        # Process trades in chronological order
        # --------------------------------------------------

        for trade in trades:

            pnl = self._safe_float(
                trade["pnl"]
            )

            total_pnl += pnl

            equity += pnl

            if pnl > 0:

                wins += 1

                gross_profit += pnl

            elif pnl < 0:

                losses += 1

                gross_loss += abs(pnl)

            entry_time = self._safe_float(
                trade["entry_time"]
            )

            exit_time = self._safe_float(
                trade["exit_time"]
            )

            hold_seconds = max(
                0.0,
                exit_time - entry_time
            )

            hold_times.append(
                hold_seconds
            )

            reason = (
                trade["exit_reason"]
                or "UNKNOWN"
            )

            exit_reasons[
                reason
            ] += 1

            symbol = (
                trade["symbol"]
                or "UNKNOWN"
            )

            stats = symbol_stats[
                symbol
            ]

            stats["trades"] += 1

            stats["pnl"] += pnl

            if pnl > 0:

                stats["wins"] += 1

                stats["gross_profit"] += pnl

            elif pnl < 0:

                stats["losses"] += 1

                stats["gross_loss"] += abs(pnl)

            # --------------------------------------------------
            # Drawdown
            # --------------------------------------------------

            if equity > peak_equity:

                peak_equity = equity

            drawdown = (
                peak_equity
                - equity
            )

            drawdown_pct = (
                drawdown
                / peak_equity
                * 100
                if peak_equity > 0
                else 0.0
            )

            max_drawdown = max(
                max_drawdown,
                drawdown
            )

            max_drawdown_pct = max(
                max_drawdown_pct,
                drawdown_pct
            )

        # --------------------------------------------------
        # Summary calculations
        # --------------------------------------------------

        closed_trades = len(trades)

        win_rate = (
            wins
            / closed_trades
            * 100
            if closed_trades > 0
            else 0.0
        )

        if gross_loss > 0:

            profit_factor = (
                gross_profit
                / gross_loss
            )

        elif gross_profit > 0:

            profit_factor = float("inf")

        else:

            profit_factor = 0.0

        expectancy = (
            total_pnl
            / closed_trades
            if closed_trades > 0
            else 0.0
        )

        average_hold_seconds = (
            sum(hold_times)
            / len(hold_times)
            if hold_times
            else 0.0
        )

        ending_balance = (
            starting_balance
            + total_pnl
        )

        net_pnl_pct = (
            total_pnl
            / starting_balance
            * 100
            if starting_balance > 0
            else 0.0
        )

        # --------------------------------------------------
        # Per-symbol statistics
        # --------------------------------------------------

        symbols = {}

        for symbol, stats in symbol_stats.items():

            trade_count = stats["trades"]

            symbol_win_rate = (
                stats["wins"]
                / trade_count
                * 100
                if trade_count > 0
                else 0.0
            )

            if stats["gross_loss"] > 0:

                symbol_profit_factor = (
                    stats["gross_profit"]
                    / stats["gross_loss"]
                )

            elif stats["gross_profit"] > 0:

                symbol_profit_factor = float("inf")

            else:

                symbol_profit_factor = 0.0

            symbols[symbol] = {
                "trades": trade_count,
                "wins": stats["wins"],
                "losses": stats["losses"],
                "win_rate": symbol_win_rate,
                "pnl": stats["pnl"],
                "profit_factor": (
                    symbol_profit_factor
                )
            }

        return {
            "starting_balance": starting_balance,
            "ending_balance": ending_balance,

            "net_pnl": total_pnl,
            "net_pnl_pct": net_pnl_pct,

            "closed_trades": closed_trades,

            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,

            "gross_profit": gross_profit,
            "gross_loss": gross_loss,

            "profit_factor": profit_factor,
            "expectancy": expectancy,

            "max_drawdown": max_drawdown,
            "max_drawdown_pct": max_drawdown_pct,

            "average_hold_seconds": (
                average_hold_seconds
            ),

            "exit_reasons": dict(
                exit_reasons
            ),

            "symbols": symbols
        }


def format_report(
    report: Dict[str, Any]
) -> str:

    profit_factor = report[
        "profit_factor"
    ]

    if math.isinf(profit_factor):

        profit_factor_text = "INF"

    else:

        profit_factor_text = (
            f"{profit_factor:.3f}"
        )

    lines = [
        "",
        "======================================",
        "BINANCE PRECISION SNIPER REPORT",
        "======================================",
        f"Starting balance : "
        f"{report['starting_balance']:.2f}",
        f"Ending balance   : "
        f"{report['ending_balance']:.2f}",
        f"Net PnL          : "
        f"{report['net_pnl']:.4f}",
        f"Net PnL %        : "
        f"{report['net_pnl_pct']:.4f}%",
        f"Closed trades    : "
        f"{report['closed_trades']}",
        f"Wins             : "
        f"{report['wins']}",
        f"Losses           : "
        f"{report['losses']}",
        f"Win rate         : "
        f"{report['win_rate']:.2f}%",
        f"Profit factor    : "
        f"{profit_factor_text}",
        f"Expectancy       : "
        f"{report['expectancy']:.6f}",
        f"Max drawdown     : "
        f"{report['max_drawdown']:.4f}",
        f"Max drawdown %   : "
        f"{report['max_drawdown_pct']:.4f}%",
        f"Avg hold         : "
        f"{report['average_hold_seconds']:.2f}s",
        "",
        "Exit reasons:"
    ]

    for reason, count in sorted(
        report["exit_reasons"].items()
    ):

        lines.append(
            f"  {reason}: {count}"
        )

    lines.append("")

    lines.append(
        "Symbol performance:"
    )

    for symbol, stats in sorted(
        report["symbols"].items(),
        key=lambda item: item[1]["pnl"],
        reverse=True
    ):

        pf = stats["profit_factor"]

        if math.isinf(pf):

            pf_text = "INF"

        else:

            pf_text = f"{pf:.3f}"

        lines.append(
            f"  {symbol}: "
            f"trades={stats['trades']} "
            f"win_rate={stats['win_rate']:.2f}% "
            f"pnl={stats['pnl']:.4f} "
            f"PF={pf_text}"
        )

    lines.append(
        "======================================"
    )

    return "\n".join(lines)


if __name__ == "__main__":

    database = Database(
        CONFIG.database_path
    )

    report_generator = PerformanceReport(
        config=CONFIG,
        database=database
    )

    report = report_generator.generate()

    print(
        format_report(report)
    )

    database.close()
