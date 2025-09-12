package reorgqueue

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/draganm/reorgqueue/sqlitestore"
	"github.com/ethereum/go-ethereum/common"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestBasicImplementation tests the basic structure and initialization
func TestBasicImplementation(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-test-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	t.Run("create with default config", func(t *testing.T) {
		writer, consumer, err := NewReorgQueueWithDefaults(db)
		require.NoError(t, err)
		require.NotNil(t, writer)
		require.NotNil(t, consumer)
	})

	t.Run("create with custom config", func(t *testing.T) {
		config := Config{
			MaxBlocks:         5000,
			ReorgBuffer:       100,
			InitialBackoff:    50 * 1000000, // 50ms in nanoseconds
			MaxBackoff:        60000000000,  // 60s in nanoseconds
			BackoffMultiplier: 1.5,
		}

		writer, consumer, err := NewReorgQueue(db, config)
		require.NoError(t, err)
		require.NotNil(t, writer)
		require.NotNil(t, consumer)
	})

	t.Run("invalid config - reorg buffer too small", func(t *testing.T) {
		config := DefaultConfig()
		config.ReorgBuffer = 1

		_, _, err := NewReorgQueue(db, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ReorgBuffer must be at least 2")
	})

	t.Run("invalid config - reorg buffer >= max blocks", func(t *testing.T) {
		config := DefaultConfig()
		config.ReorgBuffer = 2000
		config.MaxBlocks = 2000

		_, _, err := NewReorgQueue(db, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be less than MaxBlocks")
	})

	t.Run("invalid config - backoff multiplier out of range", func(t *testing.T) {
		config := DefaultConfig()
		config.BackoffMultiplier = 0.5

		_, _, err := NewReorgQueue(db, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "BackoffMultiplier must be between 1.0 and 4.0")
	})
}

func TestSchemaInitialization(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-schema-test-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	// Create reorg queue - should initialize schema
	_, _, err = NewReorgQueueWithDefaults(db)
	require.NoError(t, err)

	// Check that tables exist
	var count int
	err = db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='table' AND name IN ('blocks', 'tail_block')
	`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// Check that indexes exist
	err = db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='index' AND name IN ('idx_blocks_hash', 'idx_blocks_parent', 'idx_blocks_number_desc')
	`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Creating again should not error (schema already exists)
	_, _, err = NewReorgQueueWithDefaults(db)
	require.NoError(t, err)
}

// TestLoadTailBlock tests loading tail block from database
func TestLoadTailBlock(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-tailblock-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	t.Run("no tail block initially", func(t *testing.T) {
		rq := &reorgQueue{
			db:           db,
			config:       DefaultConfig(),
			recentBlocks: make(map[uint64]*BlockMetadata),
			blocksByHash: make(map[common.Hash]*BlockMetadata),
		}

		// Initialize schema first
		err := rq.initSchema()
		require.NoError(t, err)

		// Load tail block - should not error when none exists
		err = rq.loadTailBlock()
		require.NoError(t, err)
		require.False(t, rq.hasTail)
		require.Equal(t, uint64(0), rq.tailNumber)
		require.Equal(t, common.Hash{}, rq.tailHash)
	})

	t.Run("load existing tail block", func(t *testing.T) {
		// Insert a tail block
		queries := sqlitestore.New(db)
		hash := common.HexToHash("0x1234567890abcdef")
		err := queries.SetTailBlock(context.Background(), sqlitestore.SetTailBlockParams{
			Number: 100,
			Hash:   hash.Bytes(),
		})
		require.NoError(t, err)

		rq := &reorgQueue{
			db:           db,
			config:       DefaultConfig(),
			recentBlocks: make(map[uint64]*BlockMetadata),
			blocksByHash: make(map[common.Hash]*BlockMetadata),
		}

		// Load tail block
		err = rq.loadTailBlock()
		require.NoError(t, err)
		require.True(t, rq.hasTail)
		require.Equal(t, uint64(100), rq.tailNumber)
		require.Equal(t, hash, rq.tailHash)
	})
}

// TestLoadRecentBlocks tests loading recent blocks into cache
func TestLoadRecentBlocks(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-cache-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	rq := &reorgQueue{
		db:           db,
		config:       DefaultConfig(),
		recentBlocks: make(map[uint64]*BlockMetadata),
		blocksByHash: make(map[common.Hash]*BlockMetadata),
	}

	// Initialize schema
	err = rq.initSchema()
	require.NoError(t, err)

	t.Run("empty database", func(t *testing.T) {
		err := rq.loadRecentBlocks()
		require.NoError(t, err)
		require.Equal(t, 0, len(rq.recentBlocks))
		require.Equal(t, 0, len(rq.blocksByHash))
		require.Equal(t, uint64(0), rq.highestCached)
		require.Equal(t, uint64(0), rq.lowestCached)
	})

	t.Run("partial fill", func(t *testing.T) {
		// Insert 10 blocks (less than ReorgBuffer)
		queries := sqlitestore.New(db)
		for i := uint64(1); i <= 10; i++ {
			hash := common.BytesToHash([]byte{byte(i)})
			parent := common.BytesToHash([]byte{byte(i - 1)})

			err := queries.InsertBlock(context.Background(), sqlitestore.InsertBlockParams{
				Number: int64(i),
				Hash:   hash.Bytes(),
				Parent: parent.Bytes(),
			})
			require.NoError(t, err)
		}

		// Load recent blocks
		err = rq.loadRecentBlocks()
		require.NoError(t, err)

		// Should load all 10 blocks
		require.Equal(t, 10, len(rq.recentBlocks))
		require.Equal(t, 10, len(rq.blocksByHash))
		require.Equal(t, uint64(10), rq.highestCached)
		require.Equal(t, uint64(1), rq.lowestCached)

		// Verify blocks are properly cached
		for i := uint64(1); i <= 10; i++ {
			block, exists := rq.recentBlocks[i]
			require.True(t, exists)
			require.Equal(t, i, block.Number)

			expectedHash := common.BytesToHash([]byte{byte(i)})
			require.Equal(t, expectedHash, block.Hash)

			// Also check hash lookup
			blockByHash, exists := rq.blocksByHash[expectedHash]
			require.True(t, exists)
			require.Equal(t, block, blockByHash)
		}
	})

	t.Run("full cache", func(t *testing.T) {
		// Clear existing data
		_, err := db.Exec("DELETE FROM blocks")
		require.NoError(t, err)

		// Insert 100 blocks (more than ReorgBuffer)
		queries := sqlitestore.New(db)
		for i := uint64(1); i <= 100; i++ {
			hash := common.BytesToHash([]byte{byte(i), byte(i >> 8)})
			parent := common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)})

			err := queries.InsertBlock(context.Background(), sqlitestore.InsertBlockParams{
				Number: int64(i),
				Hash:   hash.Bytes(),
				Parent: parent.Bytes(),
			})
			require.NoError(t, err)
		}

		// Load recent blocks
		rq.config.ReorgBuffer = 64 // Default
		err = rq.loadRecentBlocks()
		require.NoError(t, err)

		// Should only load ReorgBuffer blocks (the most recent ones)
		require.Equal(t, 64, len(rq.recentBlocks))
		require.Equal(t, 64, len(rq.blocksByHash))
		require.Equal(t, uint64(100), rq.highestCached)
		require.Equal(t, uint64(37), rq.lowestCached) // 100 - 64 + 1 = 37

		// Verify we have the most recent blocks
		for i := uint64(37); i <= 100; i++ {
			block, exists := rq.recentBlocks[i]
			require.True(t, exists, "block %d should be cached", i)
			require.Equal(t, i, block.Number)
		}

		// Verify older blocks are not cached
		for i := uint64(1); i < 37; i++ {
			_, exists := rq.recentBlocks[i]
			require.False(t, exists, "block %d should not be cached", i)
		}
	})
}

// TestValidateParent tests parent validation logic
func TestValidateParent(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-validate-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	// Initialize schema
	_, err = db.Exec(schemaSQL)
	require.NoError(t, err)

	t.Run("genesis block", func(t *testing.T) {
		rq := &reorgQueue{
			db:           db,
			config:       DefaultConfig(),
			recentBlocks: make(map[uint64]*BlockMetadata),
			blocksByHash: make(map[common.Hash]*BlockMetadata),
		}
		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		block := Block{
			Number: 0,
			Hash:   common.HexToHash("0xgenesis"),
			Parent: common.Hash{},
		}

		err = rq.validateParent(tx, block)
		require.NoError(t, err)
	})

	t.Run("block after tail", func(t *testing.T) {
		rq := &reorgQueue{
			db:           db,
			config:       DefaultConfig(),
			recentBlocks: make(map[uint64]*BlockMetadata),
			blocksByHash: make(map[common.Hash]*BlockMetadata),
		}

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		// Set tail block
		rq.hasTail = true
		rq.tailNumber = 10
		rq.tailHash = common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

		// Valid block after tail
		block := Block{
			Number: 11,
			Hash:   common.HexToHash("0x2234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			Parent: common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
		}

		err = rq.validateParent(tx, block)
		require.NoError(t, err)

		// Invalid block after tail (wrong parent)
		invalidBlock := Block{
			Number: 11,
			Hash:   common.HexToHash("0x3234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			Parent: common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
		}

		err = rq.validateParent(tx, invalidBlock)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrTailMismatch)
		require.Contains(t, err.Error(), "tail block is")
	})

	t.Run("parent in cache", func(t *testing.T) {
		rq := &reorgQueue{
			db:           db,
			config:       DefaultConfig(),
			recentBlocks: make(map[uint64]*BlockMetadata),
			blocksByHash: make(map[common.Hash]*BlockMetadata),
		}

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		// Add parent to cache
		parentMeta := &BlockMetadata{
			Number: 50,
			Hash:   common.HexToHash("0x5050505050505050505050505050505050505050505050505050505050505050"),
			Parent: common.HexToHash("0x4949494949494949494949494949494949494949494949494949494949494949"),
		}
		rq.recentBlocks[50] = parentMeta

		// Valid block
		block := Block{
			Number: 51,
			Hash:   common.HexToHash("0x5151515151515151515151515151515151515151515151515151515151515151"),
			Parent: common.HexToHash("0x5050505050505050505050505050505050505050505050505050505050505050"),
		}

		err = rq.validateParent(tx, block)
		require.NoError(t, err)

		// Invalid block (wrong parent)
		invalidBlock := Block{
			Number: 51,
			Hash:   common.HexToHash("0x5252525252525252525252525252525252525252525252525252525252525252"),
			Parent: common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
		}
		err = rq.validateParent(tx, invalidBlock)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrParentNotFound)
		require.Contains(t, err.Error(), "but found")
	})

	t.Run("parent in database", func(t *testing.T) {
		// Clear blocks table first
		_, err := db.Exec("DELETE FROM blocks")
		require.NoError(t, err)

		rq := &reorgQueue{
			db:           db,
			config:       DefaultConfig(),
			recentBlocks: make(map[uint64]*BlockMetadata),
			blocksByHash: make(map[common.Hash]*BlockMetadata),
		}
		// Insert parent block
		queries := sqlitestore.New(db)
		parentHash := common.HexToHash("0x7070707070707070707070707070707070707070707070707070707070707070")
		err = queries.InsertBlock(context.Background(), sqlitestore.InsertBlockParams{
			Number: 70,
			Hash:   parentHash.Bytes(),
			Parent: common.HexToHash("0x6969696969696969696969696969696969696969696969696969696969696969").Bytes(),
		})
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		// Valid block
		block := Block{
			Number: 71,
			Hash:   common.HexToHash("0x7171717171717171717171717171717171717171717171717171717171717171"),
			Parent: parentHash,
		}

		err = rq.validateParent(tx, block)
		require.NoError(t, err)

		// Invalid block (wrong parent)
		block.Parent = common.HexToHash("0xbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadb")
		err = rq.validateParent(tx, block)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrParentNotFound)
	})

	t.Run("parent not found", func(t *testing.T) {
		rq := &reorgQueue{
			db:           db,
			config:       DefaultConfig(),
			recentBlocks: make(map[uint64]*BlockMetadata),
			blocksByHash: make(map[common.Hash]*BlockMetadata),
		}
		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		block := Block{
			Number: 99,
			Hash:   common.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999"),
			Parent: common.HexToHash("0x9898989898989898989898989898989898989898989898989898989898989898"),
		}

		err = rq.validateParent(tx, block)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrParentNotFound)
		require.Contains(t, err.Error(), "at height 98")
	})
}

// TestWriterAddBlock tests single block addition
func TestWriterAddBlock(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-addblock-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         10,
		ReorgBuffer:       4,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	w, consumer, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("add first block sets tail", func(t *testing.T) {
		block := Block{
			Number: 100,
			Hash:   common.BytesToHash([]byte{byte(100)}),
			Parent: common.BytesToHash([]byte{byte(99)}),
			Block:  []byte("block 100"),
		}

		err := w.AddBlock(ctx, block)
		require.NoError(t, err)

		// Check tail was set
		tailNum, tailHash, hasTail := consumer.GetTailBlock()
		require.True(t, hasTail)
		require.Equal(t, uint64(99), tailNum)
		require.Equal(t, common.BytesToHash([]byte{byte(99)}), tailHash)

		// Check block was inserted
		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(1), count)
	})

	t.Run("drop block before tail", func(t *testing.T) {
		block := Block{
			Number: 98, // Before tail at 99
			Hash:   common.HexToHash("0x0098"),
			Parent: common.HexToHash("0x0097"),
		}

		err := w.AddBlock(ctx, block)
		require.NoError(t, err)
	})

	t.Run("add sequential blocks", func(t *testing.T) {
		// Add blocks 101-104
		for i := uint64(101); i <= 104; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}

			err := w.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(5), count) // 100-104
	})

	t.Run("handle reorg", func(t *testing.T) {
		// Add alternative block at 103 (reorg)
		block := Block{
			Number: 103,
			Hash:   common.HexToHash("0x0103alt"),
			Parent: common.BytesToHash([]byte{byte(102)}), // Same parent as original 103
			Block:  []byte("alt block 103"),
		}

		err := w.AddBlock(ctx, block)
		require.NoError(t, err)

		// Should have 4 blocks (100-102 + new 103, old 103-104 deleted)
		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(4), count)
	})

	t.Run("buffer limit with backoff", func(t *testing.T) {
		// Create a fresh database for this test
		tmpfile, err := os.CreateTemp("", "reorgqueue-backoff-*.db")
		require.NoError(t, err)
		defer os.Remove(tmpfile.Name())
		tmpfile.Close()

		db, err := sql.Open("sqlite3", tmpfile.Name())
		require.NoError(t, err)
		defer db.Close()

		config := Config{
			MaxBlocks:         10,
			ReorgBuffer:       4,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}

		w, consumer, err := NewReorgQueue(db, config)
		require.NoError(t, err)

		// Fill up to max (10 blocks)
		for i := uint64(1); i <= 10; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}

			err := w.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		// Verify we're at max capacity
		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(10), count)

		// Try to add one more - should fail with backoff
		block := Block{
			Number: 11,
			Hash:   common.HexToHash("0x0011"),
			Parent: common.BytesToHash([]byte{byte(10)}),
		}

		// Use a context with timeout to test backoff
		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		err = w.AddBlock(shortCtx, block)
		duration := time.Since(start)

		require.Error(t, err)
		require.Contains(t, err.Error(), "cancelled")
		// Should have attempted backoff
		require.Greater(t, duration, 10*time.Millisecond)
	})
}
func TestAddBlocksBatch(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-batch-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         20,
		ReorgBuffer:       5,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	w, consumer, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("empty batch", func(t *testing.T) {
		err := w.AddBlocksBatch(ctx, []Block{})
		require.NoError(t, err)
	})

	t.Run("batch sets initial tail", func(t *testing.T) {
		blocks := []Block{
			{
				Number: 200,
				Hash:   common.HexToHash("0x0200"),
				Parent: common.HexToHash("0x0199"),
				Block:  []byte("block 200"),
			},
			{
				Number: 201,
				Hash:   common.HexToHash("0x0201"),
				Parent: common.HexToHash("0x0200"),
				Block:  []byte("block 201"),
			},
		}

		err := w.AddBlocksBatch(ctx, blocks)
		require.NoError(t, err)

		// Check tail was set
		tailNum, tailHash, hasTail := consumer.GetTailBlock()
		require.True(t, hasTail)
		require.Equal(t, uint64(199), tailNum)
		require.Equal(t, common.HexToHash("0x0199"), tailHash)

		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(2), count)
	})

	t.Run("batch with parent in same batch", func(t *testing.T) {
		blocks := []Block{
			{
				Number: 202,
				Hash:   common.HexToHash("0x0202"),
				Parent: common.HexToHash("0x0201"),
				Block:  []byte("block 202"),
			},
			{
				Number: 203,
				Hash:   common.HexToHash("0x0203"),
				Parent: common.HexToHash("0x0202"), // Parent is in same batch
				Block:  []byte("block 203"),
			},
			{
				Number: 204,
				Hash:   common.HexToHash("0x0204"),
				Parent: common.HexToHash("0x0203"), // Parent is in same batch
				Block:  []byte("block 204"),
			},
		}

		err := w.AddBlocksBatch(ctx, blocks)
		require.NoError(t, err)

		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(5), count) // 200-204
	})

	t.Run("batch rollback on error", func(t *testing.T) {
		countBefore, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)

		blocks := []Block{
			{
				Number: 205,
				Hash:   common.HexToHash("0x0205"),
				Parent: common.HexToHash("0x0204"),
				Block:  []byte("block 205"),
			},
			{
				Number: 206,
				Hash:   common.HexToHash("0x0206"),
				Parent: common.HexToHash("0xWRONG"), // Invalid parent
				Block:  []byte("block 206"),
			},
		}

		err = w.AddBlocksBatch(ctx, blocks)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batch validation failed")

		// Count should be unchanged (transaction rolled back)
		countAfter, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, countBefore, countAfter)
	})

	t.Run("batch with block before tail", func(t *testing.T) {
		blocks := []Block{
			{
				Number: 198, // Before tail at 199
				Hash:   common.HexToHash("0x0198"),
				Parent: common.HexToHash("0x0197"),
			},
			{
				Number: 199,
				Hash:   common.HexToHash("0x0199new"),
				Parent: common.HexToHash("0x0198"),
			},
		}

		err := w.AddBlocksBatch(ctx, blocks)
		require.NoError(t, err)
	})

	t.Run("batch exceeds buffer", func(t *testing.T) {
		// Fill up close to limit
		currentCount, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)

		blocksToAdd := int(config.MaxBlocks) - int(currentCount) - 1
		var blocks []Block

		// Need to determine the starting block number based on current state
		// After previous tests we have blocks 200-204
		startNum := uint64(205)

		for i := 0; i < blocksToAdd; i++ {
			num := startNum + uint64(i)
			var parent common.Hash
			if num == 205 {
				// First block connects to 204 from previous test
				parent = common.HexToHash("0x0204")
			} else {
				// Subsequent blocks connect to previous in this batch
				parent = common.BytesToHash([]byte{byte(num - 1), byte((num - 1) >> 8)})
			}

			blocks = append(blocks, Block{
				Number: num,
				Hash:   common.BytesToHash([]byte{byte(num), byte(num >> 8)}),
				Parent: parent,
				Block:  []byte{byte(num)},
			})
		}

		err = w.AddBlocksBatch(ctx, blocks)
		require.NoError(t, err)

		// Get the last block number that was added
		lastNum := uint64(205 + blocksToAdd - 1)

		// Now try to add more than space available
		moreThanSpace := []Block{
			{
				Number: lastNum + 1,
				Hash:   common.BytesToHash([]byte{byte(lastNum + 1), byte((lastNum + 1) >> 8)}),
				Parent: common.BytesToHash([]byte{byte(lastNum), byte(lastNum >> 8)}),
			},
			{
				Number: lastNum + 2,
				Hash:   common.BytesToHash([]byte{byte(lastNum + 2), byte((lastNum + 2) >> 8)}),
				Parent: common.BytesToHash([]byte{byte(lastNum + 1), byte((lastNum + 1) >> 8)}),
			},
		}

		// Use a context with short timeout to avoid infinite retry
		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		err = w.AddBlocksBatch(shortCtx, moreThanSpace)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cancelled")
	})
}

// TestMemoryCache tests memory cache management
func TestMemoryCache(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-cache-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         100,
		ReorgBuffer:       10, // Small buffer for testing
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	w, _, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	// Direct access to reorgQueue for testing cache
	// Type assert to access the internal implementation
	writerImpl, ok := w.(*writer)
	require.True(t, ok, "writer should be of type *writer")
	rq := writerImpl.rq

	ctx := context.Background()

	t.Run("cache maintains reorg buffer size", func(t *testing.T) {
		// Add 20 blocks
		for i := uint64(1); i <= 20; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}

			err := w.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		// Check cache size
		require.LessOrEqual(t, len(rq.recentBlocks), int(config.ReorgBuffer))

		// Cache should contain blocks 11-20 (the most recent)
		for i := uint64(11); i <= 20; i++ {
			_, exists := rq.recentBlocks[i]
			require.True(t, exists, "block %d should be in cache", i)
		}

		// Older blocks should not be in cache
		for i := uint64(1); i <= 10; i++ {
			_, exists := rq.recentBlocks[i]
			require.False(t, exists, "block %d should not be in cache", i)
		}

		require.Equal(t, uint64(20), rq.highestCached)
		require.Equal(t, uint64(11), rq.lowestCached)
	})

	t.Run("cache updated on reorg", func(t *testing.T) {
		// Add alternative block at 18
		block := Block{
			Number: 18,
			Hash:   common.HexToHash("0x0018alt"),
			Parent: common.BytesToHash([]byte{byte(17)}),
			Block:  []byte("alt block 18"),
		}

		err := w.AddBlock(ctx, block)
		require.NoError(t, err)

		// Check cache was updated
		cached, exists := rq.recentBlocks[18]
		require.True(t, exists)
		require.Equal(t, common.HexToHash("0x0018alt"), cached.Hash)

		// Blocks 19-20 should be removed from cache due to reorg
		_, exists = rq.recentBlocks[19]
		require.False(t, exists)
		_, exists = rq.recentBlocks[20]
		require.False(t, exists)

		require.Equal(t, uint64(18), rq.highestCached)
	})

	t.Run("batch updates cache correctly", func(t *testing.T) {
		blocks := []Block{
			{
				Number: 19,
				Hash:   common.HexToHash("0x0019new"),
				Parent: common.HexToHash("0x0018alt"),
				Block:  []byte("block 19"),
			},
			{
				Number: 20,
				Hash:   common.HexToHash("0x0020new"),
				Parent: common.HexToHash("0x0019new"),
				Block:  []byte("block 20"),
			},
			{
				Number: 21,
				Hash:   common.HexToHash("0x0021"),
				Parent: common.HexToHash("0x0020new"),
				Block:  []byte("block 21"),
			},
		}

		err := w.AddBlocksBatch(ctx, blocks)
		require.NoError(t, err)

		// All three should be in cache
		for _, block := range blocks {
			cached, exists := rq.recentBlocks[block.Number]
			require.True(t, exists)
			require.Equal(t, block.Hash, cached.Hash)
		}

		// Cache should still respect size limit
		require.LessOrEqual(t, len(rq.recentBlocks), int(config.ReorgBuffer))
		require.Equal(t, uint64(21), rq.highestCached)
		require.Equal(t, uint64(12), rq.lowestCached) // 21 - 10 + 1
	})
}

// TestWriterParentValidation tests parent validation in writer
func TestWriterParentValidation(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-parent-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	w, _, err := NewReorgQueueWithDefaults(db)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("missing parent", func(t *testing.T) {
		// First add block 500 to establish a tail
		block500 := Block{
			Number: 500,
			Hash:   common.HexToHash("0x0500"),
			Parent: common.HexToHash("0x0499"),
			Block:  []byte("block 500"),
		}
		err := w.AddBlock(ctx, block500)
		require.NoError(t, err)

		// Now try to add block 502 without parent 501
		block502 := Block{
			Number: 502,
			Hash:   common.HexToHash("0x0502"),
			Parent: common.HexToHash("0x0501"),
			Block:  []byte("block 502"),
		}

		err = w.AddBlock(ctx, block502)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrParentNotFound)
	})

	t.Run("wrong parent hash", func(t *testing.T) {
		// First add block 501 (parent of 502 that's missing)
		block501 := Block{
			Number: 501,
			Hash:   common.HexToHash("0x0501"),
			Parent: common.HexToHash("0x0500"),
			Block:  []byte("block 501"),
		}
		err := w.AddBlock(ctx, block501)
		require.NoError(t, err)

		// Add block 502
		block502 := Block{
			Number: 502,
			Hash:   common.HexToHash("0x0502"),
			Parent: common.HexToHash("0x0501"),
			Block:  []byte("block 502"),
		}
		err = w.AddBlock(ctx, block502)
		require.NoError(t, err)

		// Add block 503
		block503 := Block{
			Number: 503,
			Hash:   common.HexToHash("0x0503"),
			Parent: common.HexToHash("0x0502"),
			Block:  []byte("block 503"),
		}
		err = w.AddBlock(ctx, block503)
		require.NoError(t, err)

		// Try to add block 504 with wrong parent
		block504 := Block{
			Number: 504,
			Hash:   common.HexToHash("0x0504"),
			Parent: common.HexToHash("0x0503wrong"),
			Block:  []byte("block 504"),
		}

		err = w.AddBlock(ctx, block504)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrParentNotFound)
		require.Contains(t, err.Error(), "but found")
	})
}

// TestBackoffBehavior tests exponential backoff behavior
func TestBackoffBehavior(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-backoff-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         3, // Very small for testing
		ReorgBuffer:       2, // Minimum allowed value
		InitialBackoff:    20 * time.Millisecond,
		MaxBackoff:        80 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	w, _, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	ctx := context.Background()

	// Fill the queue
	for i := uint64(1); i <= 3; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Parent: common.BytesToHash([]byte{byte(i - 1)}),
		}
		err := w.AddBlock(ctx, block)
		require.NoError(t, err)
	}

	t.Run("backoff timing", func(t *testing.T) {
		block := Block{
			Number: 4,
			Hash:   common.HexToHash("0x0004"),
			Parent: common.BytesToHash([]byte{byte(3)}),
		}

		// Create a context that will timeout after 150ms
		// This should allow for: initial attempt + 20ms + 40ms + partial 80ms
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := w.AddBlock(timeoutCtx, block)
		duration := time.Since(start)

		require.Error(t, err)
		require.Contains(t, err.Error(), "cancelled")

		// Should have waited at least 60ms (20 + 40)
		require.GreaterOrEqual(t, duration, 60*time.Millisecond)
		// But less than 160ms (didn't complete full 80ms backoff + some margin)
		require.Less(t, duration, 160*time.Millisecond)
	})
}
// TestCacheEfficiency tests that cache is used efficiently
func TestCacheEfficiency(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-cache-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         100,
		ReorgBuffer:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	w, _, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	// Get direct access to the internal queue for cache inspection
	writerImpl, ok := w.(*writer)
	require.True(t, ok)
	rq := writerImpl.rq

	ctx := context.Background()

	t.Run("cache populated on startup", func(t *testing.T) {
		// Add some blocks
		for i := uint64(1); i <= 20; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}
			err := w.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		// Create a new queue instance - should load recent blocks into cache
		w2, _, err := NewReorgQueue(db, config)
		require.NoError(t, err)

		writerImpl2, ok := w2.(*writer)
		require.True(t, ok)
		rq2 := writerImpl2.rq

		// Cache should contain the most recent ReorgBuffer blocks
		rq2.mu.RLock()
		defer rq2.mu.RUnlock()

		require.Equal(t, int(config.ReorgBuffer), len(rq2.recentBlocks))
		require.Equal(t, uint64(20), rq2.highestCached)
		require.Equal(t, uint64(11), rq2.lowestCached) // 20 - 10 + 1

		// Verify specific blocks are in cache
		for i := uint64(11); i <= 20; i++ {
			_, exists := rq2.recentBlocks[i]
			require.True(t, exists, "block %d should be in cache", i)
		}
	})

	t.Run("cache sliding window", func(t *testing.T) {
		// Continue adding blocks and verify cache slides
		for i := uint64(21); i <= 25; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}
			err := w.AddBlock(ctx, block)
			require.NoError(t, err)

			// Check cache state after each addition
			rq.mu.RLock()
			require.LessOrEqual(t, len(rq.recentBlocks), int(config.ReorgBuffer))
			require.Equal(t, i, rq.highestCached)
			expectedLowest := i - uint64(config.ReorgBuffer) + 1
			if expectedLowest < 1 {
				expectedLowest = 1
			}
			require.Equal(t, expectedLowest, rq.lowestCached)
			rq.mu.RUnlock()
		}

		// Final cache should contain blocks 16-25
		rq.mu.RLock()
		for i := uint64(16); i <= 25; i++ {
			_, exists := rq.recentBlocks[i]
			require.True(t, exists, "block %d should be in cache", i)
		}
		for i := uint64(1); i <= 15; i++ {
			_, exists := rq.recentBlocks[i]
			require.False(t, exists, "block %d should not be in cache", i)
		}
		rq.mu.RUnlock()
	})

	t.Run("cache consistency after reorg", func(t *testing.T) {
		// Add a reorg at block 23
		reorgBlock := Block{
			Number: 23,
			Hash:   common.HexToHash("0x23alt"),
			Parent: common.BytesToHash([]byte{byte(22)}),
			Block:  []byte("reorg block 23"),
		}
		err := w.AddBlock(ctx, reorgBlock)
		require.NoError(t, err)

		rq.mu.RLock()
		defer rq.mu.RUnlock()

		// Cache should no longer contain blocks 23-25 (original chain)
		// Should contain the new block 23
		cached23, exists := rq.recentBlocks[23]
		require.True(t, exists)
		require.Equal(t, common.HexToHash("0x23alt"), cached23.Hash)

		// Blocks 24-25 should be removed
		_, exists = rq.recentBlocks[24]
		require.False(t, exists)
		_, exists = rq.recentBlocks[25]
		require.False(t, exists)

		// highestCached should be 23
		require.Equal(t, uint64(23), rq.highestCached)
	})
}

// TestCacheLookupPerformance tests that cache lookups are used over DB
func TestCacheLookupPerformance(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-perf-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         1000,
		ReorgBuffer:       64,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	w, _, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	ctx := context.Background()

	// Add many blocks
	for i := uint64(1); i <= 100; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i), byte(i >> 8)}),
			Parent: common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)}),
			Block:  []byte{byte(i)},
		}
		err := w.AddBlock(ctx, block)
		require.NoError(t, err)
	}

	// Now add blocks that reference cached parents - should be fast
	// The parent validation should hit the cache instead of the database
	t.Run("sequential blocks use cache", func(t *testing.T) {
		for i := uint64(101); i <= 110; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i), byte(i >> 8)}),
				Parent: common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)}),
				Block:  []byte{byte(i)},
			}
			// This should be fast because parent is in cache
			err := w.AddBlock(ctx, block)
			require.NoError(t, err)
		}
	})
}

// TestBatchCacheUpdate tests that batch operations update cache correctly
func TestBatchCacheUpdate(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-batch-cache-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         100,
		ReorgBuffer:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	w, _, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	writerImpl, ok := w.(*writer)
	require.True(t, ok)
	rq := writerImpl.rq

	ctx := context.Background()

	t.Run("batch updates cache atomically", func(t *testing.T) {
		// Add initial blocks
		blocks := make([]Block, 15)
		for i := 0; i < 15; i++ {
			blocks[i] = Block{
				Number: uint64(i + 1),
				Hash:   common.BytesToHash([]byte{byte(i + 1)}),
				Parent: common.BytesToHash([]byte{byte(i)}),
				Block:  []byte{byte(i + 1)},
			}
		}

		err := w.AddBlocksBatch(ctx, blocks)
		require.NoError(t, err)

		// Check cache state
		rq.mu.RLock()
		require.Equal(t, int(config.ReorgBuffer), len(rq.recentBlocks))
		require.Equal(t, uint64(15), rq.highestCached)
		require.Equal(t, uint64(6), rq.lowestCached) // 15 - 10 + 1
		rq.mu.RUnlock()

		// Add a batch that causes reorg at block 12
		reorgBatch := []Block{
			{
				Number: 12,
				Hash:   common.HexToHash("0x12alt"),
				Parent: common.BytesToHash([]byte{byte(11)}),
				Block:  []byte("alt 12"),
			},
			{
				Number: 13,
				Hash:   common.HexToHash("0x13alt"),
				Parent: common.HexToHash("0x12alt"),
				Block:  []byte("alt 13"),
			},
			{
				Number: 14,
				Hash:   common.HexToHash("0x14alt"),
				Parent: common.HexToHash("0x13alt"),
				Block:  []byte("alt 14"),
			},
		}

		err = w.AddBlocksBatch(ctx, reorgBatch)
		require.NoError(t, err)

		// Check cache was updated correctly
		rq.mu.RLock()
		defer rq.mu.RUnlock()

		// Should have new versions of blocks 12-14
		block12, exists := rq.recentBlocks[12]
		require.True(t, exists)
		require.Equal(t, common.HexToHash("0x12alt"), block12.Hash)

		block13, exists := rq.recentBlocks[13]
		require.True(t, exists)
		require.Equal(t, common.HexToHash("0x13alt"), block13.Hash)

		block14, exists := rq.recentBlocks[14]
		require.True(t, exists)
		require.Equal(t, common.HexToHash("0x14alt"), block14.Hash)

		// Block 15 should be gone
		_, exists = rq.recentBlocks[15]
		require.False(t, exists)

		require.Equal(t, uint64(14), rq.highestCached)
	})
}

// TestCacheDebugInfo tests the debug info functionality
func TestCacheDebugInfo(t *testing.T) {
	// Create a temporary database
	tmpfile, err := os.CreateTemp("", "reorgqueue-debug-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := DefaultConfig()
	config.ReorgBuffer = 5

	w, consumer, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	writerImpl, ok := w.(*writer)
	require.True(t, ok)
	rq := writerImpl.rq

	ctx := context.Background()

	// Add some blocks
	for i := uint64(1); i <= 10; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Parent: common.BytesToHash([]byte{byte(i - 1)}),
			Block:  []byte{byte(i)},
		}
		err := w.AddBlock(ctx, block)
		require.NoError(t, err)
	}

	// Test debug info
	t.Run("dump cache state", func(t *testing.T) {
		info := rq.DumpCacheState()

		require.Equal(t, 5, info.CacheSize)
		require.Equal(t, uint64(6), info.LowestCached)
		require.Equal(t, uint64(10), info.HighestCached)
		require.Equal(t, uint64(0), info.TailNumber)
		require.Equal(t, common.Hash{}.Hex(), info.TailHash)
		require.True(t, info.HasTail)

		// Get total blocks through consumer
		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, count, info.TotalBlocks)
	})
}
