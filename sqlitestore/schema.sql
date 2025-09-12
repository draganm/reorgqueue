-- Schema for reorgqueue package
-- This schema manages blockchain blocks with reorg protection

CREATE TABLE IF NOT EXISTS blocks (
    number BIGINT PRIMARY KEY,
    hash BLOB NOT NULL,
    parent BLOB NOT NULL,
    block BLOB,
    receipts BLOB,
    call_traces BLOB,
    prestate_traces BLOB,
    keccak256_preimage_traces BLOB,
    state_access_traces BLOB
);

CREATE TABLE IF NOT EXISTS tail_block (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    number BIGINT NOT NULL,
    hash BLOB NOT NULL
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks (hash);
CREATE INDEX IF NOT EXISTS idx_blocks_parent ON blocks (parent);

-- CRITICAL: Index for efficient highest block queries
CREATE INDEX IF NOT EXISTS idx_blocks_number_desc ON blocks (number DESC);