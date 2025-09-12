package reorgqueue

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestBasicInsertRetrieve tests basic insert and retrieve operations
func TestBasicInsertRetrieve(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-basic-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	writer, consumer, err := NewReorgQueueWithDefaults(db)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("single block insert and retrieve", func(t *testing.T) {
		block := Block{
			Number: 1,
			Hash:   common.HexToHash("0x1"),
			Parent: common.HexToHash("0x0"),
			Block:  []byte("block 1"),
		}

		err := writer.AddBlock(ctx, block)
		require.NoError(t, err)

		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(1), count)

		highest, err := consumer.GetHighestBlock(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(1), highest)
	})

	t.Run("sequential blocks", func(t *testing.T) {
		// Add more blocks
		for i := uint64(2); i <= 10; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}
			err := writer.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(10), count)
	})

	t.Run("retrieve with reorg buffer", func(t *testing.T) {
		// With default reorg buffer of 64, no blocks should be available yet
		blocks, err := consumer.GetBlocks(ctx, 100)
		require.NoError(t, err)
		require.Empty(t, blocks)

		// Add more blocks to exceed reorg buffer
		for i := uint64(11); i <= 80; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}
			err := writer.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		// Now we should have blocks available (80 - 64 = 16)
		blocks, err = consumer.GetBlocks(ctx, 10)
		require.NoError(t, err)
		require.Len(t, blocks, 10)

		// Verify they are the oldest blocks
		for i, block := range blocks {
			require.Equal(t, uint64(i+1), block.Number)
		}
	})
}

// TestTailBlockManagement tests tail block tracking
func TestTailBlockManagement(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-tail-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	writer, consumer, err := NewReorgQueueWithDefaults(db)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("initial state has no tail", func(t *testing.T) {
		tailNum, tailHash, hasTail := consumer.GetTailBlock()
		require.False(t, hasTail)
		require.Equal(t, uint64(0), tailNum)
		require.Equal(t, common.Hash{}, tailHash)
	})

	t.Run("first block sets tail", func(t *testing.T) {
		block := Block{
			Number: 100,
			Hash:   common.HexToHash("0x100"),
			Parent: common.HexToHash("0x99"),
			Block:  []byte("block 100"),
		}

		err := writer.AddBlock(ctx, block)
		require.NoError(t, err)

		tailNum, tailHash, hasTail := consumer.GetTailBlock()
		require.True(t, hasTail)
		require.Equal(t, uint64(99), tailNum)
		require.Equal(t, common.HexToHash("0x99"), tailHash)
	})

	t.Run("tail survives restart", func(t *testing.T) {
		// Create new queue instance with same DB
		writer2, consumer2, err := NewReorgQueueWithDefaults(db)
		require.NoError(t, err)

		tailNum, tailHash, hasTail := consumer2.GetTailBlock()
		require.True(t, hasTail)
		require.Equal(t, uint64(99), tailNum)
		require.Equal(t, common.HexToHash("0x99"), tailHash)

		// Can continue adding blocks
		block := Block{
			Number: 101,
			Hash:   common.HexToHash("0x101"),
			Parent: common.HexToHash("0x100"),
			Block:  []byte("block 101"),
		}
		err = writer2.AddBlock(ctx, block)
		require.NoError(t, err)
	})
}

// TestMemoryCacheConsistency tests cache behavior
func TestMemoryCacheConsistency(t *testing.T) {
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

	ctx := context.Background()

	// Get access to internal state for cache verification
	var writerStruct *writer
	writerStruct = w.(*writer)
	rq := writerStruct.rq

	t.Run("cache maintains sliding window", func(t *testing.T) {
		// Add blocks and verify cache size
		for i := uint64(1); i <= 20; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}
			err := w.AddBlock(ctx, block)
			require.NoError(t, err)

			// Check cache state
			cacheInfo := rq.DumpCacheState()
			if i <= uint64(config.ReorgBuffer) {
				require.Equal(t, int(i), cacheInfo.CacheSize)
			} else {
				require.Equal(t, int(config.ReorgBuffer), cacheInfo.CacheSize)
				require.Equal(t, i-uint64(config.ReorgBuffer)+1, cacheInfo.LowestCached)
			}
			require.Equal(t, i, cacheInfo.HighestCached)
		}
	})

	t.Run("cache survives restart", func(t *testing.T) {
		// Create new instance
		w2, _, err := NewReorgQueue(db, config)
		require.NoError(t, err)

		var writerStruct2 *writer
		writerStruct2 = w2.(*writer)
		rq2 := writerStruct2.rq

		// Check cache was loaded
		cacheInfo := rq2.DumpCacheState()
		require.Equal(t, int(config.ReorgBuffer), cacheInfo.CacheSize)
		require.Equal(t, uint64(11), cacheInfo.LowestCached)
		require.Equal(t, uint64(20), cacheInfo.HighestCached)
	})
}

// TestExponentialBackoff tests backoff behavior when queue is full
func TestExponentialBackoff(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-backoff-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	config := Config{
		MaxBlocks:         10,
		ReorgBuffer:       3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	writer, consumer, err := NewReorgQueue(db, config)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("backoff when full", func(t *testing.T) {
		// Fill the queue
		for i := uint64(1); i <= 10; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}
			err := writer.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		// Queue is now full
		count, err := consumer.GetBlockCount(ctx)
		require.NoError(t, err)
		require.Equal(t, uint(10), count)

		// Try to add another block with timeout
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		block := Block{
			Number: 11,
			Hash:   common.BytesToHash([]byte{11}),
			Parent: common.BytesToHash([]byte{10}),
			Block:  []byte{11},
		}
		err = writer.AddBlock(ctxWithTimeout, block)
		elapsed := time.Since(start)

		// Should timeout after trying with backoff
		require.Error(t, err)
		require.Contains(t, err.Error(), "cancelled")
		require.Greater(t, elapsed, 40*time.Millisecond) // At least some backoff occurred
	})

	t.Run("succeeds after space available", func(t *testing.T) {
		// Delete some blocks
		err := consumer.DeleteBlocks(ctx, 1, 5)
		require.NoError(t, err)

		// Now add should succeed
		block := Block{
			Number: 11,
			Hash:   common.BytesToHash([]byte{11}),
			Parent: common.BytesToHash([]byte{10}),
			Block:  []byte{11},
		}
		err = writer.AddBlock(ctx, block)
		require.NoError(t, err)
	})
}

// TestConcurrentAccess tests concurrent reader access
func TestConcurrentAccess(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-concurrent-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	writer, consumer, err := NewReorgQueueWithDefaults(db)
	require.NoError(t, err)

	ctx := context.Background()

	// Add initial blocks
	for i := uint64(1); i <= 100; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i)}),
			Parent: common.BytesToHash([]byte{byte(i - 1)}),
			Block:  []byte{byte(i)},
		}
		err := writer.AddBlock(ctx, block)
		require.NoError(t, err)
	}

	t.Run("concurrent readers", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 10)

		// Start multiple reader goroutines
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					if _, err := consumer.GetBlockCount(ctx); err != nil {
						errors <- err
						return
					}
					if _, err := consumer.GetHighestBlock(ctx); err != nil {
						errors <- err
						return
					}
					if _, err := consumer.GetBlocks(ctx, 5); err != nil {
						errors <- err
						return
					}
					time.Sleep(time.Millisecond)
				}
			}()
		}

		// Writer continues adding blocks
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(101); i <= 110; i++ {
				block := Block{
					Number: i,
					Hash:   common.BytesToHash([]byte{byte(i)}),
					Parent: common.BytesToHash([]byte{byte(i - 1)}),
					Block:  []byte{byte(i)},
				}
				if err := writer.AddBlock(ctx, block); err != nil {
					errors <- err
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()

		// Wait for all goroutines
		done := make(chan bool)
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case err := <-errors:
			t.Fatalf("Concurrent operation failed: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent operations")
		}
	})
}

// TestReorgHandling tests reorganization scenarios
func TestReorgHandling(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-reorg-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	writer, consumer, err := NewReorgQueueWithDefaults(db)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("simple reorg", func(t *testing.T) {
		// Build initial chain
		for i := uint64(1); i <= 10; i++ {
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: common.BytesToHash([]byte{byte(i - 1)}),
				Block:  []byte{byte(i)},
			}
			err := writer.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		// Reorg at block 8 with different hash
		reorgBlock := Block{
			Number: 8,
			Hash:   common.HexToHash("0x8alt"),
			Parent: common.BytesToHash([]byte{7}),
			Block:  []byte("alt block 8"),
		}
		err = writer.AddBlock(ctx, reorgBlock)
		require.NoError(t, err)

		// Verify blocks 9-10 were removed
		highest, err := consumer.GetHighestBlock(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(8), highest)

		// Continue on new fork
		for i := uint64(9); i <= 12; i++ {
			var parent common.Hash
			if i == 9 {
				parent = common.HexToHash("0x8alt")
			} else {
				parent = common.HexToHash("0x" + string(rune('0'+i-1)) + "alt")
			}
			block := Block{
				Number: i,
				Hash:   common.HexToHash("0x" + string(rune('0'+i)) + "alt"),
				Parent: parent,
				Block:  []byte("alt block"),
			}
			err := writer.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		highest, err = consumer.GetHighestBlock(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(12), highest)
	})
}

// TestErrorScenarios tests various error conditions
func TestErrorScenarios(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-errors-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	require.NoError(t, err)
	defer db.Close()

	writer, consumer, err := NewReorgQueueWithDefaults(db)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("parent not found", func(t *testing.T) {
		block := Block{
			Number: 100,
			Hash:   common.HexToHash("0x100"),
			Parent: common.HexToHash("0x99"),
			Block:  []byte("block 100"),
		}
		err := writer.AddBlock(ctx, block)
		require.NoError(t, err) // First block is OK

		// Try to add non-sequential block
		orphan := Block{
			Number: 102,
			Hash:   common.HexToHash("0x102"),
			Parent: common.HexToHash("0x101"), // Parent doesn't exist
			Block:  []byte("orphan block"),
		}
		err = writer.AddBlock(ctx, orphan)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrParentNotFound)
		require.Contains(t, err.Error(), "block 102 expects parent")
	})

	t.Run("block too old", func(t *testing.T) {
		// Continue from block 100 that was added in previous test
		// Add more blocks
		for i := uint64(101); i <= 110; i++ {
			var parent common.Hash
			if i == 101 {
				// Block 100 has hash "0x100", not BytesToHash([100])
				parent = common.HexToHash("0x100")
			} else {
				parent = common.BytesToHash([]byte{byte(i - 1)})
			}
			block := Block{
				Number: i,
				Hash:   common.BytesToHash([]byte{byte(i)}),
				Parent: parent,
				Block:  []byte{byte(i)},
			}
			err := writer.AddBlock(ctx, block)
			require.NoError(t, err)
		}

		// Delete early blocks to move tail forward
		err = consumer.DeleteBlocks(ctx, 100, 105)
		require.NoError(t, err)

		// Try to add block before tail (tail is now at 105)
		oldBlock := Block{
			Number: 104,
			Hash:   common.HexToHash("0x104new"),
			Parent: common.HexToHash("0x103new"),
			Block:  []byte("old block"),
		}
		err = writer.AddBlock(ctx, oldBlock)
		require.NoError(t, err)
	})

	t.Run("invalid delete range", func(t *testing.T) {
		// Try invalid range
		err := consumer.DeleteBlocks(ctx, 10, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid range")
	})

	t.Run("delete would leave gap", func(t *testing.T) {
		// After previous test, we have blocks 106-110
		// Try to delete middle blocks (107-108), which would leave 109-110
		err := consumer.DeleteBlocks(ctx, 107, 108)
		require.Error(t, err)
		require.Contains(t, err.Error(), "would leave blocks")
	})
}
