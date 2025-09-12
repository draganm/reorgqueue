-- Additional queries that might be useful in the future

-- name: GetBlocksRange :many
-- Get blocks in a specific range (inclusive)
SELECT number, hash, parent, block, receipts, call_traces, prestate_traces
FROM blocks
WHERE number >= ? AND number <= ?
ORDER BY number ASC;

-- name: GetBlocksByHash :many
-- Get blocks by their hash (useful for finding forks)
SELECT number, hash, parent, block, receipts, call_traces, prestate_traces
FROM blocks
WHERE hash = ?;

-- name: GetChildrenOfBlock :many
-- Find all blocks that have a specific parent (useful for fork detection)
SELECT number, hash, parent, block, receipts, call_traces, prestate_traces
FROM blocks
WHERE parent = ?
ORDER BY number ASC;

-- name: CountBlocksAbove :one
-- Count blocks above a certain height
SELECT COUNT(*) FROM blocks WHERE number > ?;

-- name: CountBlocksBelow :one
-- Count blocks below a certain height
SELECT COUNT(*) FROM blocks WHERE number < ?;

-- name: GetBlocksWithLimit :many
-- Get oldest blocks with both limit and offset (for pagination)
SELECT number, hash, parent, block, receipts, call_traces, prestate_traces
FROM blocks
ORDER BY number ASC
LIMIT ? OFFSET ?;

-- name: CheckTailExists :one
-- Check if tail block is set
SELECT EXISTS(SELECT 1 FROM tail_block WHERE id = 1);

-- name: ClearTailBlock :exec
-- Clear the tail block (used when all blocks are deleted)
DELETE FROM tail_block WHERE id = 1;

-- name: GetBlocksSize :one
-- Get total size of stored blocks (useful for monitoring)
SELECT COALESCE(SUM(
    LENGTH(hash) + 
    LENGTH(parent) + 
    COALESCE(LENGTH(block), 0) + 
    COALESCE(LENGTH(receipts), 0) + 
    COALESCE(LENGTH(call_traces), 0) + 
    COALESCE(LENGTH(prestate_traces), 0)
), 0) FROM blocks;