-- Queries for reorgqueue package

-- name: GetBlockCount :one
SELECT COUNT(*) FROM blocks;

-- name: InsertBlock :exec
INSERT INTO blocks (number, hash, parent, block, receipts, call_traces, prestate_traces, keccak256_preimage_traces, state_access_traces)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetHighestBlock :one
SELECT COALESCE(MAX(number), -1) FROM blocks;

-- name: GetLowestBlock :one
SELECT COALESCE(MIN(number), -1) FROM blocks;

-- name: GetBlocks :many
SELECT number, hash, parent, block, receipts, call_traces, prestate_traces, keccak256_preimage_traces, state_access_traces
FROM blocks
ORDER BY number ASC
LIMIT ?;

-- name: GetTopBlocks :many
SELECT number, hash, parent 
FROM blocks 
ORDER BY number DESC 
LIMIT ?;

-- name: DeleteBlocksRange :exec
DELETE FROM blocks WHERE number >= ? AND number <= ?;

-- name: GetBlockByNumber :one
SELECT number, hash, parent, block, receipts, call_traces, prestate_traces, keccak256_preimage_traces, state_access_traces
FROM blocks
WHERE number = ?;

-- name: GetBlockHashByNumber :one
SELECT hash
FROM blocks
WHERE number = ?;

-- name: GetTailBlock :one
SELECT number, hash FROM tail_block WHERE id = 1;

-- name: SetTailBlock :exec
INSERT INTO tail_block (id, number, hash) VALUES (1, ?, ?)
ON CONFLICT(id) DO UPDATE SET number = excluded.number, hash = excluded.hash;

-- name: CheckBlockExists :one
SELECT EXISTS(SELECT 1 FROM blocks WHERE number = ? AND hash = ?);

-- name: DeleteBlocksFrom :execrows
DELETE FROM blocks WHERE number >= ?;