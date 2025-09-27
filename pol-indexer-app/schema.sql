PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS blocks (
  block_number INTEGER PRIMARY KEY,
  block_hash TEXT NOT NULL,
  timestamp INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
  tx_hash TEXT PRIMARY KEY,
  block_number INTEGER NOT NULL,
  from_addr TEXT,
  to_addr TEXT,
  value_text TEXT,
  nonce INTEGER,
  FOREIGN KEY(block_number) REFERENCES blocks(block_number)
);

CREATE TABLE IF NOT EXISTS token_transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tx_hash TEXT NOT NULL,
  block_number INTEGER NOT NULL,
  log_index INTEGER NOT NULL,
  token_address TEXT NOT NULL,
  "from" TEXT NOT NULL,
  "to" TEXT NOT NULL,
  amount TEXT NOT NULL,
  decimals INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  UNIQUE(tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS net_flows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_address TEXT NOT NULL,
  exchange_label TEXT NOT NULL,
  snapshot_time INTEGER NOT NULL,
  cumulative_amount_text TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_token_transfers_token ON token_transfers(token_address);
CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON token_transfers("to");
CREATE INDEX IF NOT EXISTS idx_token_transfers_from ON token_transfers("from");
CREATE INDEX IF NOT EXISTS idx_netflows_token_exchange ON net_flows(token_address, exchange_label);

