import sqlite3
import threading
from typing import Optional


class Database:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()

        self.connection = sqlite3.connect(
            self.path,
            check_same_thread=False
        )

        self.connection.row_factory = sqlite3.Row

        self._configure()
        self._create_tables()

    def _configure(self) -> None:
        with self.connection:
            self.connection.execute(
                "PRAGMA journal_mode=WAL"
            )

            self.connection.execute(
                "PRAGMA synchronous=NORMAL"
            )

    def _create_tables(self) -> None:
        with self.connection:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    symbol TEXT,
                    event_type TEXT NOT NULL,
                    data TEXT
                )
                """
            )

            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    symbol TEXT NOT NULL,
                    accepted INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    reason TEXT,
                    data TEXT
                )
                """
            )

            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,

                    entry_time REAL NOT NULL,
                    exit_time REAL,

                    entry_price REAL NOT NULL,
                    exit_price REAL,

                    quantity REAL NOT NULL,

                    entry_fee REAL DEFAULT 0,
                    exit_fee REAL DEFAULT 0,

                    pnl REAL DEFAULT 0,
                    pnl_pct REAL DEFAULT 0,

                    score INTEGER DEFAULT 0,

                    exit_reason TEXT,

                    status TEXT NOT NULL,

                    metadata TEXT
                )
                """
            )

            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_signals_timestamp
                ON signals(timestamp)
                """
            )

            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_signals_symbol
                ON signals(symbol)
                """
            )

            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_trades_entry_time
                ON trades(entry_time)
                """
            )

            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_trades_symbol
                ON trades(symbol)
                """
            )

    def log_event(
        self,
        timestamp: float,
        symbol: Optional[str],
        event_type: str,
        data: Optional[str] = None
    ) -> None:
        with self.lock:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO events (
                        timestamp,
                        symbol,
                        event_type,
                        data
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        timestamp,
                        symbol,
                        event_type,
                        data
                    )
                )

    def log_signal(
        self,
        timestamp: float,
        symbol: str,
        accepted: bool,
        score: int,
        reason: str,
        data: Optional[str] = None
    ) -> None:
        with self.lock:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO signals (
                        timestamp,
                        symbol,
                        accepted,
                        score,
                        reason,
                        data
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        timestamp,
                        symbol,
                        1 if accepted else 0,
                        score,
                        reason,
                        data
                    )
                )

    def create_trade(
        self,
        symbol: str,
        entry_time: float,
        entry_price: float,
        quantity: float,
        entry_fee: float,
        score: int,
        metadata: Optional[str] = None
    ) -> int:
        with self.lock:
            with self.connection:
                cursor = self.connection.execute(
                    """
                    INSERT INTO trades (
                        symbol,
                        entry_time,
                        entry_price,
                        quantity,
                        entry_fee,
                        score,
                        status,
                        metadata
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        symbol,
                        entry_time,
                        entry_price,
                        quantity,
                        entry_fee,
                        score,
                        "OPEN",
                        metadata
                    )
                )

                return int(cursor.lastrowid)

    def close_trade(
        self,
        trade_id: int,
        exit_time: float,
        exit_price: float,
        exit_fee: float,
        pnl: float,
        pnl_pct: float,
        exit_reason: str
    ) -> None:
        with self.lock:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE trades
                    SET
                        exit_time = ?,
                        exit_price = ?,
                        exit_fee = ?,
                        pnl = ?,
                        pnl_pct = ?,
                        exit_reason = ?,
                        status = 'CLOSED'
                    WHERE id = ?
                    """,
                    (
                        exit_time,
                        exit_price,
                        exit_fee,
                        pnl,
                        pnl_pct,
                        exit_reason,
                        trade_id
                    )
                )

    def get_open_trades(self):
        with self.lock:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM trades
                WHERE status = 'OPEN'
                ORDER BY entry_time ASC
                """
            )

            return cursor.fetchall()

    def get_closed_trades(
        self,
        limit: Optional[int] = None
    ):
        with self.lock:
            if limit is None:
                cursor = self.connection.execute(
                    """
                    SELECT *
                    FROM trades
                    WHERE status = 'CLOSED'
                    ORDER BY exit_time ASC
                    """
                )
            else:
                cursor = self.connection.execute(
                    """
                    SELECT *
                    FROM trades
                    WHERE status = 'CLOSED'
                    ORDER BY exit_time DESC
                    LIMIT ?
                    """,
                    (limit,)
                )

            return cursor.fetchall()

    def get_recent_signals(
        self,
        limit: int = 100
    ):
        with self.lock:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM signals
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,)
            )

            return cursor.fetchall()

    def get_recent_events(
        self,
        limit: int = 100
    ):
        with self.lock:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM events
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,)
            )

            return cursor.fetchall()

    def close(self) -> None:
        with self.lock:
            self.connection.close()
