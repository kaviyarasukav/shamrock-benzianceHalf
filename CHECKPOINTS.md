# Shamrock Bot Project Checkpoints

This file records major architectural changes and fixes to ensure traceability and prevent regression (Butterfly Effect).

## [CP-2024-04-11-01] Technical Indicators Integration
- **Status**: Implemented
- **Description**: Added RSI and Supertrend indicators to the dashboard.
- **Impact**: Enhanced trading signals for users.
- **Key Logic**: `[INDICATORS:RSI:CALC]`, `[INDICATORS:SUPERTREND:CALC]`.

## [CP-2024-04-14-01] Orderflow Analysis Expansion
- **Status**: Implemented
- **Description**: Added Phase 2.1.4 (Block Trades) and Phase 2.1.5 (Liquidity Shifts).
- **Impact**: Real-time detection of institutional smart money and order book manipulation.
- **Key Logic**: `[ORDERFLOW:BLOCK_TRADES:DETECT]`, `[ORDERFLOW:LIQUIDITY_DELTA:TRACK]`.
