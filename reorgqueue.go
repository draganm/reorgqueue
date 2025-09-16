//go:generate sqlc generate -f sqlitestore/sqlc.yaml

package reorgqueue

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/draganm/reorgqueue/sqlitestore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Error definitions
var (
	ErrParentNotFound     = errors.New("parent block not found")
	ErrMaxBufferedReached = errors.New("max buffered blocks reached")
	ErrInvalidBlockchain  = errors.New("blockchain invariant violated")
	ErrBlockTooOld        = errors.New("block number is before or at tail block")
	ErrTailMismatch       = errors.New("first block parent does not match tail block")
)

// Prometheus metrics
var (
	blocksInserted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reorgqueue_blocks_inserted_total",
			Help: "Total number of blocks inserted",
		},
		[]string{"status"},
	)

	blocksDeleted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "reorgqueue_blocks_deleted_total",
			Help: "Total number of blocks deleted",
		},
	)

	backoffEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reorgqueue_backoff_events_total",
			Help: "Total number of backoff events",
		},
		[]string{"reason"},
	)

	reorgEvents = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "reorgqueue_reorg_events_total",
			Help: "Total number of reorg events detected",
		},
	)

	cacheHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "reorgqueue_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	cacheMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "reorgqueue_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	queueSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "reorgqueue_size",
			Help: "Current number of blocks in queue",
		},
	)

	tailBlockNumber = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "reorgqueue_tail_block_number",
			Help: "Current tail block number",
		},
	)

	cacheSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "reorgqueue_cache_size",
			Help: "Current number of blocks in memory cache",
		},
	)

	operationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "reorgqueue_operation_duration_seconds",
			Help:    "Duration of reorgqueue operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
)

// init registers metrics with prometheus
func init() {
	prometheus.MustRegister(
		blocksInserted,
		blocksDeleted,
		backoffEvents,
		reorgEvents,
		cacheHits,
		cacheMisses,
		queueSize,
		tailBlockNumber,
		cacheSize,
		operationDuration,
	)
}

//go:embed sqlitestore/schema.sql
var schemaSQL string

// Writer interface for adding blocks to the queue
type Writer interface {
	AddBlock(ctx context.Context, block Block) error
	AddBlocksBatch(ctx context.Context, blocks []Block) error
}

// Consumer interface for reading and deleting blocks
type Consumer interface {
	GetBlocks(ctx context.Context, maxBlocks uint) ([]Block, error)
	DeleteBlocks(ctx context.Context, startBlock, endBlock uint64) error
	GetHighestBlock(ctx context.Context) (uint64, error)
	GetBlockCount(ctx context.Context) (uint, error)
	GetTailBlock() (number uint64, hash common.Hash, exists bool)
}

// Config holds configuration for the reorg queue
type Config struct {
	// Maximum number of blocks to store (default: 2000)
	MaxBlocks uint

	// Number of recent blocks to keep for reorg protection (default: 64)
	ReorgBuffer uint

	// Initial backoff duration when max blocks reached (default: 100ms)
	InitialBackoff time.Duration

	// Maximum backoff duration (default: 30s)
	MaxBackoff time.Duration

	// Backoff multiplier (default: 2.0)
	BackoffMultiplier float64
}

// DefaultConfig returns a Config with default values
func DefaultConfig() Config {
	return Config{
		MaxBlocks:         2000,
		ReorgBuffer:       64,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 1.5,
	}
}

// Block represents a blockchain block with associated data
type Block struct {
	Number                  uint64
	Hash                    common.Hash
	Parent                  common.Hash
	Block                   []byte
	Receipts                []byte
	CallTraces              []byte
	PrestateTraces          []byte
	Keccak256PreimageTraces []byte
	StateAccessTraces       []byte
}

// BlockMetadata holds lightweight block information for caching
type BlockMetadata struct {
	Number uint64
	Hash   common.Hash
	Parent common.Hash
}

// reorgQueue holds the shared state, not exposed to users
type reorgQueue struct {
	db     *sql.DB
	config Config

	// Single writer assumption - using mutex for memory safety
	mu sync.RWMutex

	// In-memory cache for top N blocks
	recentBlocks  map[uint64]*BlockMetadata
	blocksByHash  map[common.Hash]*BlockMetadata
	highestCached uint64
	lowestCached  uint64

	// Tail block tracking
	tailNumber uint64
	tailHash   common.Hash
	hasTail    bool
}

// writer implements the Writer interface
type writer struct {
	rq *reorgQueue
}

// consumer implements the Consumer interface
type consumer struct {
	rq *reorgQueue
}

// NewReorgQueue creates a new reorg queue and returns Writer and Consumer interfaces
func NewReorgQueue(db *sql.DB, config Config) (Writer, Consumer, error) {
	// Validate config
	err := validateConfig(&config)
	if err != nil {
		return nil, nil, err
	}

	rq := &reorgQueue{
		db:           db,
		config:       config,
		recentBlocks: make(map[uint64]*BlockMetadata),
		blocksByHash: make(map[common.Hash]*BlockMetadata),
	}

	// Initialize schema
	err = rq.initSchema()
	if err != nil {
		return nil, nil, err
	}

	// Load tail block
	err = rq.loadTailBlock()
	if err != nil {
		return nil, nil, err
	}

	// Load recent blocks
	err = rq.loadRecentBlocks()
	if err != nil {
		return nil, nil, err
	}

	// Initialize metrics with current state
	ctx := context.Background()
	queries := sqlitestore.New(db)

	count, err := queries.GetBlockCount(ctx)
	if err == nil {
		queueSize.Set(float64(count))
	}
	if rq.hasTail {
		tailBlockNumber.Set(float64(rq.tailNumber))
	}
	cacheSize.Set(float64(len(rq.recentBlocks)))

	return &writer{rq: rq}, &consumer{rq: rq}, nil
}

// NewReorgQueueWithDefaults creates a new reorg queue with default configuration
func NewReorgQueueWithDefaults(db *sql.DB) (Writer, Consumer, error) {
	return NewReorgQueue(db, DefaultConfig())
}

// validateConfig validates the configuration parameters
func validateConfig(c *Config) error {
	if c.ReorgBuffer < 2 {
		return fmt.Errorf("ReorgBuffer must be at least 2, got %d", c.ReorgBuffer)
	}
	if c.ReorgBuffer >= c.MaxBlocks {
		return fmt.Errorf("ReorgBuffer (%d) must be less than MaxBlocks (%d)",
			c.ReorgBuffer, c.MaxBlocks)
	}
	if c.BackoffMultiplier < 1.0 || c.BackoffMultiplier > 4.0 {
		return fmt.Errorf("BackoffMultiplier must be between 1.0 and 4.0, got %f",
			c.BackoffMultiplier)
	}
	return nil
}

// initSchema checks if the schema is applied and applies it if necessary
func (rq *reorgQueue) initSchema() error {
	// Check if schema is already applied
	var tableName string
	err := rq.db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name='blocks'`,
	).Scan(&tableName)

	if err == sql.ErrNoRows {
		// Schema not applied, execute embedded schema
		_, err = rq.db.Exec(schemaSQL)
		if err != nil {
			return fmt.Errorf("failed to apply schema: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check schema: %w", err)
	}
	return nil
}

// loadTailBlock loads the tail block from the database
func (rq *reorgQueue) loadTailBlock() error {
	queries := sqlitestore.New(rq.db)

	tailBlock, err := queries.GetTailBlock(context.Background())
	if err == sql.ErrNoRows {
		// No tail block exists yet
		rq.hasTail = false
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to load tail block: %w", err)
	}

	rq.tailNumber = uint64(tailBlock.Number)
	rq.tailHash = common.BytesToHash(tailBlock.Hash)
	rq.hasTail = true

	return nil
}

// loadRecentBlocks loads recent blocks into the memory cache
func (rq *reorgQueue) loadRecentBlocks() error {
	queries := sqlitestore.New(rq.db)

	// Load up to ReorgBuffer blocks, but handle partial fills gracefully
	blocks, err := queries.GetTopBlocks(context.Background(), int64(rq.config.ReorgBuffer))
	if err != nil {
		return fmt.Errorf("failed to load recent blocks: %w", err)
	}

	// Clear existing cache
	rq.recentBlocks = make(map[uint64]*BlockMetadata)
	rq.blocksByHash = make(map[common.Hash]*BlockMetadata)
	rq.highestCached = 0
	rq.lowestCached = 0

	// Populate cache with loaded blocks
	for _, block := range blocks {
		meta := &BlockMetadata{
			Number: uint64(block.Number),
			Hash:   common.BytesToHash(block.Hash),
			Parent: common.BytesToHash(block.Parent),
		}

		rq.recentBlocks[meta.Number] = meta
		rq.blocksByHash[meta.Hash] = meta

		// Update boundaries
		if rq.highestCached == 0 || meta.Number > rq.highestCached {
			rq.highestCached = meta.Number
		}
		if rq.lowestCached == 0 || meta.Number < rq.lowestCached {
			rq.lowestCached = meta.Number
		}
	}

	return nil
}

// Writer interface implementations

// AddBlock adds a single block to the queue
func (w *writer) AddBlock(ctx context.Context, block Block) error {
	return w.rq.addBlockWithBackoff(ctx, block)
}

// AddBlocksBatch adds multiple blocks to the queue
func (w *writer) AddBlocksBatch(ctx context.Context, blocks []Block) error {
	return w.rq.addBlocksBatchWithBackoff(ctx, blocks)
}

// addBlockWithBackoff adds a block with exponential backoff retry
func (rq *reorgQueue) addBlockWithBackoff(ctx context.Context, block Block) error {
	backoff := rq.config.InitialBackoff

	for {
		err := rq.tryAddBlock(ctx, block)

		if err == nil {
			return nil
		}

		switch {
		case errors.Is(err, ErrMaxBufferedReached):
			// Retry
		case strings.Contains(err.Error(), "database is locked"):
			// Retry
		default:
			return err
		}

		// Check context cancellation before sleeping
		select {
		case <-ctx.Done():
			return fmt.Errorf("add block cancelled: %w", ctx.Err())
		case <-time.After(backoff):
			// Exponential backoff
			backoff = time.Duration(float64(backoff) * rq.config.BackoffMultiplier)
			if backoff > rq.config.MaxBackoff {
				backoff = rq.config.MaxBackoff
			}
		}
	}
}

// tryAddBlock attempts to add a single block to the queue
func (rq *reorgQueue) tryAddBlock(ctx context.Context, block Block) error {
	timer := prometheus.NewTimer(operationDuration.WithLabelValues("add_block"))
	defer timer.ObserveDuration()

	logger := slog.Default().With(
		"component", "reorgqueue",
		"method", "tryAddBlock",
		"block_number", block.Number,
		"block_hash", block.Hash.Hex(),
	)

	logger.Debug("attempting to add block")

	rq.mu.Lock()
	defer rq.mu.Unlock()

	// Early rejection for blocks before or at tail
	if rq.hasTail && block.Number <= rq.tailNumber {
		logger.Warn("block rejected: too old", "tail_number", rq.tailNumber)
		return nil
	}

	tx, err := rq.db.BeginTx(ctx, nil)
	if err != nil {
		blocksInserted.WithLabelValues("error_transaction").Inc()
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check current block count
	queries := sqlitestore.New(tx)
	count, err := queries.GetBlockCount(ctx)
	if err != nil {
		blocksInserted.WithLabelValues("error_get_count").Inc()
		return fmt.Errorf("failed to get block count: %w", err)
	}

	// Check buffer limit
	if uint(count) >= rq.config.MaxBlocks {
		backoffEvents.WithLabelValues("queue_full").Inc()
		logger.Debug("queue full", "current_count", count, "max_blocks", rq.config.MaxBlocks)
		return fmt.Errorf("%w: current count %d, max %d",
			ErrMaxBufferedReached, count, rq.config.MaxBlocks)
	}

	// Special handling for first block when no tail exists
	if count == 0 && !rq.hasTail {
		// First block ever - its parent becomes the tail
		err = queries.SetTailBlock(ctx, sqlitestore.SetTailBlockParams{
			Number: int64(block.Number - 1),
			Hash:   block.Parent.Bytes(),
		})
		if err != nil {
			return fmt.Errorf("failed to set initial tail block: %w", err)
		}

		// Update in-memory tail
		rq.tailNumber = block.Number - 1
		rq.tailHash = block.Parent
		rq.hasTail = true
	}

	// Validate parent relationship
	if err := rq.validateParent(tx, block); err != nil {
		return err
	}

	// Handle potential reorg - delete blocks at same or higher number
	deletedCount, err := queries.DeleteBlocksFrom(ctx, int64(block.Number))
	if err != nil {
		blocksInserted.WithLabelValues("error_reorg_delete").Inc()
		return fmt.Errorf("failed to delete blocks for reorg: %w", err)
	}

	if deletedCount > 0 {
		reorgEvents.Inc()
		logger.Info("reorg detected", "deleted_blocks", deletedCount, "from_number", block.Number)
	}

	// Clean up cache for reorged blocks
	rq.cleanupCacheForReorg(block.Number)

	// Insert the new block
	err = queries.InsertBlock(ctx, sqlitestore.InsertBlockParams{
		Number:                  int64(block.Number),
		Hash:                    block.Hash.Bytes(),
		Parent:                  block.Parent.Bytes(),
		Block:                   block.Block,
		Receipts:                block.Receipts,
		CallTraces:              block.CallTraces,
		PrestateTraces:          block.PrestateTraces,
		Keccak256PreimageTraces: block.Keccak256PreimageTraces,
		StateAccessTraces:       block.StateAccessTraces,
	})
	if err != nil {
		blocksInserted.WithLabelValues("error_insert").Inc()
		return fmt.Errorf("failed to insert block: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		blocksInserted.WithLabelValues("error_commit").Inc()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update memory cache (no need to lock as we already hold the mutex)
	rq.updateMemoryCacheAfterInsert(block)

	// Update metrics
	blocksInserted.WithLabelValues("success").Inc()
	queueSize.Inc()
	cacheSize.Set(float64(len(rq.recentBlocks)))
	if rq.hasTail {
		tailBlockNumber.Set(float64(rq.tailNumber))
	}

	logger.Info("block added successfully",
		"cache_size", len(rq.recentBlocks),
		"queue_size", count+1,
		"tail_number", rq.tailNumber,
	)

	return nil
}

// cleanupCacheForReorg removes blocks from cache that are at or above the given number
func (rq *reorgQueue) cleanupCacheForReorg(fromNumber uint64) {
	// Remove all blocks at or above fromNumber
	for num := fromNumber; num <= rq.highestCached; num++ {
		if meta, exists := rq.recentBlocks[num]; exists {
			delete(rq.recentBlocks, num)
			delete(rq.blocksByHash, meta.Hash)
		}
	}

	// Update highestCached
	if fromNumber <= rq.highestCached {
		if fromNumber > 1 {
			rq.highestCached = fromNumber - 1
		} else {
			rq.highestCached = 0
		}
	}

	// Recalculate lowestCached and highestCached based on remaining blocks
	if len(rq.recentBlocks) == 0 {
		rq.lowestCached = 0
		rq.highestCached = 0
	} else {
		// Find actual bounds
		first := true
		for num := range rq.recentBlocks {
			if first {
				rq.lowestCached = num
				rq.highestCached = num
				first = false
			} else {
				if num < rq.lowestCached {
					rq.lowestCached = num
				}
				if num > rq.highestCached {
					rq.highestCached = num
				}
			}
		}
	}
}

// updateMemoryCacheAfterInsert updates the memory cache after inserting a block
func (rq *reorgQueue) updateMemoryCacheAfterInsert(block Block) {
	meta := &BlockMetadata{
		Number: block.Number,
		Hash:   block.Hash,
		Parent: block.Parent,
	}

	// Add to cache
	rq.recentBlocks[block.Number] = meta
	rq.blocksByHash[block.Hash] = meta

	// Update boundaries
	if block.Number > rq.highestCached || rq.highestCached == 0 {
		rq.highestCached = block.Number
	}

	// Maintain cache size - remove oldest blocks if we exceed ReorgBuffer
	if uint(len(rq.recentBlocks)) > rq.config.ReorgBuffer {
		// Find the new lowest block we should keep
		newLowest := rq.highestCached - uint64(rq.config.ReorgBuffer) + 1

		// Remove blocks below the new threshold
		for num := rq.lowestCached; num < newLowest; num++ {
			if meta, exists := rq.recentBlocks[num]; exists {
				delete(rq.recentBlocks, num)
				delete(rq.blocksByHash, meta.Hash)
			}
		}

		rq.lowestCached = newLowest
	}

	// Update lowestCached if this is the first block or we need to adjust
	if rq.lowestCached == 0 || (len(rq.recentBlocks) > 0 && block.Number < rq.lowestCached) {
		// Find the actual lowest cached block
		for num := range rq.recentBlocks {
			if rq.lowestCached == 0 || num < rq.lowestCached {
				rq.lowestCached = num
			}
		}
	}
}

// addBlocksBatchWithBackoff adds multiple blocks with exponential backoff retry
func (rq *reorgQueue) addBlocksBatchWithBackoff(ctx context.Context, blocks []Block) error {
	if len(blocks) == 0 {
		return nil
	}

	backoff := rq.config.InitialBackoff

	for {
		err := rq.tryAddBlocksBatch(ctx, blocks)

		if err == nil {
			return nil
		}

		switch {
		case errors.Is(err, ErrMaxBufferedReached):
			// Retry
		case strings.Contains(err.Error(), "database is locked"):
			// Retry
		default:
			return err
		}

		// Check context cancellation before sleeping
		select {
		case <-ctx.Done():
			return fmt.Errorf("add blocks batch cancelled: %w", ctx.Err())
		case <-time.After(backoff):
			// Exponential backoff
			backoff = time.Duration(float64(backoff) * rq.config.BackoffMultiplier)
			if backoff > rq.config.MaxBackoff {
				backoff = rq.config.MaxBackoff
			}
		}
	}
}

// tryAddBlocksBatch attempts to add multiple blocks as a single transaction
func (rq *reorgQueue) tryAddBlocksBatch(ctx context.Context, blocks []Block) error {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	skipBlocks := map[int]bool{}

	// Early validation - check all blocks are after tail
	for i, block := range blocks {
		if rq.hasTail && block.Number <= rq.tailNumber {
			skipBlocks[i] = true
		}
	}

	tx, err := rq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check current block count
	queries := sqlitestore.New(tx)
	count, err := queries.GetBlockCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get block count: %w", err)
	}

	// Check if batch would exceed buffer limit
	if uint(count+int64(len(blocks))) > rq.config.MaxBlocks {
		return fmt.Errorf("%w: current count %d + batch size %d exceeds max %d",
			ErrMaxBufferedReached, count, len(blocks), rq.config.MaxBlocks)
	}

	// Process each block in the batch
	for i, block := range blocks {
		if skipBlocks[i] {
			continue
		}

		// Special handling for first block ever
		if count == 0 && i == 0 && !rq.hasTail {
			err = queries.SetTailBlock(ctx, sqlitestore.SetTailBlockParams{
				Number: int64(block.Number - 1),
				Hash:   block.Parent.Bytes(),
			})
			if err != nil {
				return fmt.Errorf("failed to set initial tail block: %w", err)
			}

			// Update in-memory tail
			rq.tailNumber = block.Number - 1
			rq.tailHash = block.Parent
			rq.hasTail = true
		}

		// Validate parent - checking within batch first
		if err := rq.validateParentBatch(tx, block, i, blocks); err != nil {
			return fmt.Errorf("batch validation failed at block %d: %w", i, err)
		}

		// Handle reorg for this block
		_, err = queries.DeleteBlocksFrom(ctx, int64(block.Number))
		if err != nil {
			return fmt.Errorf("failed to delete blocks for reorg at block %d: %w", block.Number, err)
		}

		// Clean up cache for reorged blocks
		rq.cleanupCacheForReorg(block.Number)

		// Insert the block
		err = queries.InsertBlock(ctx, sqlitestore.InsertBlockParams{
			Number:                  int64(block.Number),
			Hash:                    block.Hash.Bytes(),
			Parent:                  block.Parent.Bytes(),
			Block:                   block.Block,
			Receipts:                block.Receipts,
			CallTraces:              block.CallTraces,
			PrestateTraces:          block.PrestateTraces,
			Keccak256PreimageTraces: block.Keccak256PreimageTraces,
			StateAccessTraces:       block.StateAccessTraces,
		})
		if err != nil {
			return fmt.Errorf("failed to insert block %d: %w", block.Number, err)
		}
	}

	// Commit transaction - all or nothing
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	// Update memory cache for all blocks
	for i, block := range blocks {
		if skipBlocks[i] {
			continue
		}
		rq.updateMemoryCacheAfterInsert(block)
	}

	return nil
}

// validateParentBatch validates parent for a block in a batch, checking the batch first
func (rq *reorgQueue) validateParentBatch(tx *sql.Tx, block Block, index int, batch []Block) error {
	// Genesis block has no parent
	if block.Number == 0 {
		return nil
	}

	// Check if parent is earlier in the same batch
	if index > 0 {
		for j := index - 1; j >= 0; j-- {
			if batch[j].Number == block.Number-1 {
				if batch[j].Hash != block.Parent {
					return fmt.Errorf("%w: block %d expects parent %s but batch contains %s at position %d",
						ErrParentNotFound, block.Number, block.Parent.Hex(), batch[j].Hash.Hex(), j)
				}
				return nil
			}
		}
	}

	// Otherwise use standard validation
	return rq.validateParent(tx, block)
}

// Consumer interface implementations

// GetBlocks returns blocks from the queue
func (c *consumer) GetBlocks(ctx context.Context, maxBlocks uint) ([]Block, error) {
	return c.rq.getBlocks(ctx, maxBlocks)
}

// DeleteBlocks removes blocks from the queue
func (c *consumer) DeleteBlocks(ctx context.Context, startBlock, endBlock uint64) error {
	return c.rq.deleteBlocks(ctx, startBlock, endBlock)
}

// GetHighestBlock returns the highest block number
func (c *consumer) GetHighestBlock(ctx context.Context) (uint64, error) {
	return c.rq.getHighestBlock(ctx)
}

// GetBlockCount returns the number of blocks in the queue
func (c *consumer) GetBlockCount(ctx context.Context) (uint, error) {
	queries := sqlitestore.New(c.rq.db)
	count, err := queries.GetBlockCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get block count: %w", err)
	}
	return uint(count), nil
}

// GetTailBlock returns the tail block information
func (c *consumer) GetTailBlock() (uint64, common.Hash, bool) {
	c.rq.mu.RLock()
	defer c.rq.mu.RUnlock()
	return c.rq.tailNumber, c.rq.tailHash, c.rq.hasTail
}

// validateParent validates that a block's parent exists and matches expectations
func (rq *reorgQueue) validateParent(tx *sql.Tx, block Block) error {
	// Genesis block has no parent
	if block.Number == 0 {
		return nil
	}

	// Check if this is the first block after tail
	if rq.hasTail && block.Number == rq.tailNumber+1 {
		if block.Parent != rq.tailHash {
			return fmt.Errorf("%w: block %d expects parent %s but tail block is %s at height %d",
				ErrTailMismatch, block.Number, block.Parent.Hex(), rq.tailHash.Hex(), rq.tailNumber)
		}
		return nil
	}

	// Check parent in memory cache first (no mutex needed as caller should hold it)
	if parent, exists := rq.recentBlocks[block.Number-1]; exists {
		cacheHits.Inc()
		if parent.Hash != block.Parent {
			return fmt.Errorf("%w: block %d expects parent %s at height %d, but found %s",
				ErrParentNotFound, block.Number, block.Parent.Hex(), block.Number-1, parent.Hash.Hex())
		}
		return nil
	}

	// Fall back to database check
	cacheMisses.Inc()
	queries := sqlitestore.New(tx)
	parentHash, err := queries.GetBlockHashByNumber(context.Background(), int64(block.Number-1))

	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: block %d expects parent %s at height %d",
			ErrParentNotFound, block.Number, block.Parent.Hex(), block.Number-1)
	} else if err != nil {
		return fmt.Errorf("failed to check parent block: %w", err)
	}

	if common.BytesToHash(parentHash) != block.Parent {
		return fmt.Errorf("%w: block %d expects parent %s at height %d, but found %s",
			ErrParentNotFound, block.Number, block.Parent.Hex(), block.Number-1,
			common.BytesToHash(parentHash).Hex())
	}

	return nil
}

// CacheDebugInfo contains debug information about the cache state
type CacheDebugInfo struct {
	CacheSize     int
	LowestCached  uint64
	HighestCached uint64
	TailNumber    uint64
	TailHash      string
	HasTail       bool
	TotalBlocks   uint
}

// DumpCacheState returns debug information about the current cache state
func (rq *reorgQueue) DumpCacheState() CacheDebugInfo {
	rq.mu.RLock()
	defer rq.mu.RUnlock()

	// Get block count
	queries := sqlitestore.New(rq.db)
	count, _ := queries.GetBlockCount(context.Background())

	return CacheDebugInfo{
		CacheSize:     len(rq.recentBlocks),
		LowestCached:  rq.lowestCached,
		HighestCached: rq.highestCached,
		TailNumber:    rq.tailNumber,
		TailHash:      rq.tailHash.Hex(),
		HasTail:       rq.hasTail,
		TotalBlocks:   uint(count),
	}
}

// getBlocks returns blocks from the queue, respecting the reorg buffer
func (rq *reorgQueue) getBlocks(ctx context.Context, maxBlocks uint) ([]Block, error) {
	timer := prometheus.NewTimer(operationDuration.WithLabelValues("get_blocks"))
	defer timer.ObserveDuration()

	logger := slog.Default().With(
		"component", "reorgqueue",
		"method", "getBlocks",
		"max_blocks", maxBlocks,
	)

	queries := sqlitestore.New(rq.db)

	// Get total block count
	totalBlocks, err := queries.GetBlockCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get block count: %w", err)
	}

	// Always keep top N blocks for reorgs (configurable)
	if totalBlocks <= int64(rq.config.ReorgBuffer) {
		logger.Debug("no blocks available", "total_blocks", totalBlocks, "reorg_buffer", rq.config.ReorgBuffer)
		return []Block{}, nil
	}

	// Calculate available blocks (excluding the reorg buffer)
	availableBlocks := totalBlocks - int64(rq.config.ReorgBuffer)
	if availableBlocks <= 0 {
		return []Block{}, nil
	}

	// Limit the number of blocks to return
	limit := uint(availableBlocks)
	if maxBlocks > 0 && maxBlocks < limit {
		limit = maxBlocks
	}

	// Get the blocks (oldest first, excluding the top ReorgBuffer blocks)
	rows, err := queries.GetBlocks(ctx, int64(limit))
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks: %w", err)
	}

	blocks := make([]Block, 0, len(rows))
	for _, row := range rows {
		blocks = append(blocks, Block{
			Number:                  uint64(row.Number),
			Hash:                    common.BytesToHash(row.Hash),
			Parent:                  common.BytesToHash(row.Parent),
			Block:                   row.Block,
			Receipts:                row.Receipts,
			CallTraces:              row.CallTraces,
			PrestateTraces:          row.PrestateTraces,
			Keccak256PreimageTraces: row.Keccak256PreimageTraces,
			StateAccessTraces:       row.StateAccessTraces,
		})
	}

	return blocks, nil
}

// deleteBlocks deletes blocks from the queue with tail validation
func (rq *reorgQueue) deleteBlocks(ctx context.Context, startBlock, endBlock uint64) error {
	timer := prometheus.NewTimer(operationDuration.WithLabelValues("delete_blocks"))
	defer timer.ObserveDuration()

	logger := slog.Default().With(
		"component", "reorgqueue",
		"method", "deleteBlocks",
		"start_block", startBlock,
		"end_block", endBlock,
	)

	logger.Debug("attempting to delete blocks")

	// Validate input
	if startBlock > endBlock {
		return fmt.Errorf("invalid range: startBlock %d > endBlock %d", startBlock, endBlock)
	}

	rq.mu.Lock()
	defer rq.mu.Unlock()

	tx, err := rq.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	queries := sqlitestore.New(tx)

	// Get the lowest block number
	lowestBlockRaw, err := queries.GetLowestBlock(ctx)
	if err == sql.ErrNoRows {
		return nil // No blocks to delete
	} else if err != nil {
		return fmt.Errorf("failed to get lowest block: %w", err)
	}

	// Type assert the interface{} to int64
	lowestBlock, ok := lowestBlockRaw.(int64)
	if !ok {
		return fmt.Errorf("unexpected type for lowest block: %T", lowestBlockRaw)
	}

	// Check if no blocks exist (GetLowestBlock returns -1 when empty)
	if lowestBlock == -1 {
		return nil // No blocks to delete
	}

	// Verify we're not leaving blocks behind the tail
	if startBlock > uint64(lowestBlock) {
		return fmt.Errorf("cannot delete blocks: would leave blocks %d-%d behind new tail %d",
			lowestBlock, startBlock-1, endBlock)
	}

	// Get the highest block's hash before deletion if we're deleting all blocks
	var highestDeletedBlock *sqlitestore.Block
	if endBlock >= uint64(lowestBlock) {
		// Get the highest block
		highestRaw, err := queries.GetHighestBlock(ctx)
		if err == nil {
			highest, ok := highestRaw.(int64)
			if ok && highest >= 0 && endBlock >= uint64(highest) {
				// We're deleting all blocks, get the highest block's info
				block, err := queries.GetBlockByNumber(ctx, int64(endBlock))
				if err != nil {
					return fmt.Errorf("failed to get block %d before deletion: %w", endBlock, err)
				}
				highestDeletedBlock = &block
			}
		}
	}

	// Delete the blocks
	err = queries.DeleteBlocksRange(ctx, sqlitestore.DeleteBlocksRangeParams{
		Number:   int64(startBlock),
		Number_2: int64(endBlock),
	})
	if err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}

	// Update tail block to the parent of the new lowest block
	if endBlock >= uint64(lowestBlock) {
		// Find the new lowest block
		newLowestRaw, err := queries.GetLowestBlock(ctx)
		if err == sql.ErrNoRows {
			// All blocks deleted, set tail to highest deleted block
			if highestDeletedBlock != nil {
				// Set tail to the highest deleted block
				err = queries.SetTailBlock(ctx, sqlitestore.SetTailBlockParams{
					Number: highestDeletedBlock.Number,
					Hash:   highestDeletedBlock.Hash,
				})
				if err != nil {
					return fmt.Errorf("failed to set tail block: %w", err)
				}

				// Update in-memory tail
				rq.tailNumber = uint64(highestDeletedBlock.Number)
				rq.tailHash = common.BytesToHash(highestDeletedBlock.Hash)
				rq.hasTail = true
			} else {
				// Should not happen, but handle gracefully
				rq.hasTail = false
				rq.tailNumber = 0
				rq.tailHash = common.Hash{}
			}
		} else if err != nil {
			return fmt.Errorf("failed to get new lowest block: %w", err)
		} else {
			// Type assert the interface{} to int64
			newLowest, ok := newLowestRaw.(int64)
			if !ok {
				return fmt.Errorf("unexpected type for new lowest block: %T", newLowestRaw)
			}

			// Check if no blocks exist after deletion
			if newLowest == -1 {
				// All blocks deleted, set tail to highest deleted block
				if highestDeletedBlock != nil {
					// Set tail to the highest deleted block
					err = queries.SetTailBlock(ctx, sqlitestore.SetTailBlockParams{
						Number: highestDeletedBlock.Number,
						Hash:   highestDeletedBlock.Hash,
					})
					if err != nil {
						return fmt.Errorf("failed to set tail block: %w", err)
					}

					// Update in-memory tail
					rq.tailNumber = uint64(highestDeletedBlock.Number)
					rq.tailHash = common.BytesToHash(highestDeletedBlock.Hash)
					rq.hasTail = true
				} else {
					// Should not happen, but handle gracefully
					rq.hasTail = false
					rq.tailNumber = 0
					rq.tailHash = common.Hash{}
				}
			} else {
				// Get the block to find its parent
				block, err := queries.GetBlockByNumber(ctx, newLowest)
				if err != nil {
					return fmt.Errorf("failed to get block %d: %w", newLowest, err)
				}

				// Set tail to the parent of the new lowest block
				err = queries.SetTailBlock(ctx, sqlitestore.SetTailBlockParams{
					Number: newLowest - 1,
					Hash:   block.Parent,
				})
				if err != nil {
					return fmt.Errorf("failed to update tail block: %w", err)
				}

				// Update in-memory tail
				rq.tailNumber = uint64(newLowest - 1)
				rq.tailHash = common.BytesToHash(block.Parent)
				rq.hasTail = true
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update metrics
	deletedCount := endBlock - startBlock + 1
	blocksDeleted.Add(float64(deletedCount))
	queueSize.Sub(float64(deletedCount))
	if rq.hasTail {
		tailBlockNumber.Set(float64(rq.tailNumber))
	}

	logger.Info("blocks deleted successfully",
		"deleted_count", deletedCount,
		"tail_number", rq.tailNumber,
		"has_tail", rq.hasTail,
	)

	// Note: Do NOT reload cache after deletion as per plan

	return nil
}

// getHighestBlock returns the highest block number in the queue
func (rq *reorgQueue) getHighestBlock(ctx context.Context) (uint64, error) {
	queries := sqlitestore.New(rq.db)

	highestRaw, err := queries.GetHighestBlock(ctx)
	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to get highest block: %w", err)
	}

	// Type assert the interface{} to int64
	highest, ok := highestRaw.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected type for highest block: %T", highestRaw)
	}

	// Check if no blocks exist (GetHighestBlock returns -1 when empty)
	if highest == -1 {
		return 0, nil
	}

	return uint64(highest), nil
}
