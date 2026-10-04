# Binance Precision Sniper

A Binance Spot paper-trading sniper bot designed for real-time market monitoring, strict entry filters, risk management, trade simulation, and performance tracking.

## Important

This project is currently **Paper Trading only**.

It does not place real Binance orders.

No Binance API trading keys are required for the current version.

## Features

- Binance Spot market data
- Real-time WebSocket streams
- 24h quote-volume filtering
- Bid/ask spread filtering
- Buy-pressure analysis
- Order-book imbalance analysis
- EMA trend filtering
- BTC market protection filter
- Anti-chasing protection
- Rule-based entry score
- Simulated stop loss
- Simulated take profit
- Time-based exit
- Simulated trading fees
- Simulated slippage
- Position sizing
- Maximum open trades
- Daily loss protection
- SQLite trade database
- Performance reporting
- Web dashboard

## Strategy

The current experimental strategy uses multiple market conditions before accepting a paper-trade entry.

Example filters include:

- Minimum 24h quote volume
- Maximum spread
- Minimum short-term buy pressure
- Minimum order-book imbalance
- EMA trend confirmation
- BTC volatility protection
- Anti-chasing protection
- Minimum strategy score

The score is a rule-based score.

It is **not a probability of winning**.

No specific win rate is guaranteed.

## Risk Management

The default configuration includes:

- Risk per trade: 0.10%
- Maximum position: 20%
- Maximum open trades: 3
- Stop loss: 0.18%
- Take profit: 0.28%
- Maximum holding time: 20 seconds
- Daily loss limit: 1%

These values are experimental and should be evaluated using forward paper-trading data.

## Database

Trade and signal information is stored in SQLite.

The database is created automatically in:

```text
data/sniper.db
