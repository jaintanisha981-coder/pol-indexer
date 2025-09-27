# POL Net-Flow Indexer (Binance)

## Overview
Real-time indexer for POL token on Polygon. Tracks ERC-20 Transfer events for POL and computes cumulative net-flow to/from a list of Binance addresses.

## Quickstart
1. Clone repo.
2. Copy `.env.example` to `.env` and set `POLYGON_RPC` if needed.
3. `cargo build --release`
4. `POLYGON_RPC="https://polygon-rpc.com" cargo run --release`

## DB schema
See `schema.sql`.

## Binance addresses
(6 addresses used — see source file)


