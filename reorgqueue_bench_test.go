package reorgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	_ "github.com/mattn/go-sqlite3"
)

// BenchmarkAddBlock benchmarks single block addition
func BenchmarkAddBlock(b *testing.B) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-bench-*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	writer, _, err := NewReorgQueueWithDefaults(db)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	// Pre-populate with some blocks
	for i := uint64(1); i <= 1000; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i), byte(i >> 8)}),
			Parent: common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)}),
			Block:  []byte{byte(i)},
		}
		if err := writer.AddBlock(ctx, block); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		num := uint64(1001 + i)
		block := Block{
			Number: num,
			Hash:   common.BytesToHash([]byte{byte(num), byte(num >> 8)}),
			Parent: common.BytesToHash([]byte{byte(num - 1), byte((num - 1) >> 8)}),
			Block:  make([]byte, 100), // Typical block size
		}
		if err := writer.AddBlock(ctx, block); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddBlocksBatch benchmarks batch block addition
func BenchmarkAddBlocksBatch(b *testing.B) {
	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("batch-%d", size), func(b *testing.B) {
			tmpfile, err := os.CreateTemp("", "reorgqueue-bench-batch-*.db")
			if err != nil {
				b.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())
			tmpfile.Close()

			db, err := sql.Open("sqlite3", tmpfile.Name())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			writer, _, err := NewReorgQueueWithDefaults(db)
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				batch := make([]Block, size)
				for j := 0; j < size; j++ {
					num := uint64(i*size + j + 1)
					batch[j] = Block{
						Number:     num,
						Hash:       common.BytesToHash([]byte{byte(num), byte(num >> 8)}),
						Parent:     common.BytesToHash([]byte{byte(num - 1), byte((num - 1) >> 8)}),
						Block:      make([]byte, 100),
						Receipts:   make([]byte, 50),
						CallTraces: make([]byte, 200),
					}
				}
				if err := writer.AddBlocksBatch(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGetBlocks benchmarks block retrieval
func BenchmarkGetBlocks(b *testing.B) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-bench-get-*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	config := Config{
		MaxBlocks:         10000,
		ReorgBuffer:       64,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	writer, consumer, err := NewReorgQueue(db, config)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	// Pre-populate with blocks
	for i := uint64(1); i <= 5000; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i), byte(i >> 8)}),
			Parent: common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)}),
			Block:  make([]byte, 100),
		}
		if err := writer.AddBlock(ctx, block); err != nil {
			b.Fatal(err)
		}
	}

	retrieveSizes := []uint{10, 50, 100, 500}

	for _, size := range retrieveSizes {
		b.Run(fmt.Sprintf("get-%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				blocks, err := consumer.GetBlocks(ctx, size)
				if err != nil {
					b.Fatal(err)
				}
				if len(blocks) == 0 {
					b.Fatal("expected blocks but got none")
				}
			}
		})
	}
}

// BenchmarkDeleteBlocks benchmarks block deletion
func BenchmarkDeleteBlocks(b *testing.B) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-bench-del-*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	writer, consumer, err := NewReorgQueueWithDefaults(db)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	// Pre-populate with many blocks
	batchSize := 100
	for batch := 0; batch < 100; batch++ {
		blocks := make([]Block, batchSize)
		for i := 0; i < batchSize; i++ {
			num := uint64(batch*batchSize + i + 1)
			blocks[i] = Block{
				Number: num,
				Hash:   common.BytesToHash([]byte{byte(num), byte(num >> 8)}),
				Parent: common.BytesToHash([]byte{byte(num - 1), byte((num - 1) >> 8)}),
				Block:  make([]byte, 100),
			}
		}
		if err := writer.AddBlocksBatch(ctx, blocks); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	// Delete in chunks
	deleteSize := uint64(10)
	for i := 0; i < b.N; i++ {
		start := uint64(i)*deleteSize + 1
		end := start + deleteSize - 1
		if start > 9900 {
			b.StopTimer()
			break
		}
		if err := consumer.DeleteBlocks(ctx, start, end); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentReads benchmarks concurrent read operations
func BenchmarkConcurrentReads(b *testing.B) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-bench-concurrent-*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	writer, consumer, err := NewReorgQueueWithDefaults(db)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	// Pre-populate
	for i := uint64(1); i <= 1000; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i), byte(i >> 8)}),
			Parent: common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)}),
			Block:  make([]byte, 100),
		}
		if err := writer.AddBlock(ctx, block); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Mix of read operations
			if _, err := consumer.GetBlockCount(ctx); err != nil {
				b.Fatal(err)
			}
			if _, err := consumer.GetHighestBlock(ctx); err != nil {
				b.Fatal(err)
			}
			if _, err := consumer.GetBlocks(ctx, 10); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkReorg benchmarks reorganization handling
func BenchmarkReorg(b *testing.B) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-bench-reorg-*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	writer, _, err := NewReorgQueueWithDefaults(db)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	// Pre-populate a long chain
	for i := uint64(1); i <= 1000; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i), byte(i >> 8)}),
			Parent: common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)}),
			Block:  make([]byte, 100),
		}
		if err := writer.AddBlock(ctx, block); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	// Benchmark reorgs at different depths
	depths := []uint64{1, 5, 10}
	for _, depth := range depths {
		b.Run(fmt.Sprintf("depth-%d", depth), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Create reorg at current height - depth
				reorgAt := uint64(1000) - depth + 1
				for j := uint64(0); j < depth; j++ {
					num := reorgAt + j
					block := Block{
						Number: num,
						Hash:   common.HexToHash(fmt.Sprintf("0x%xreorg%d", num, i)),
						Parent: common.BytesToHash([]byte{byte(num - 1), byte((num - 1) >> 8)}),
						Block:  make([]byte, 100),
					}
					if j > 0 {
						// Update parent to previous reorg block
						block.Parent = common.HexToHash(fmt.Sprintf("0x%xreorg%d", num-1, i))
					}
					if err := writer.AddBlock(ctx, block); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkCacheHit benchmarks operations that hit the cache
func BenchmarkCacheHit(b *testing.B) {
	tmpfile, err := os.CreateTemp("", "reorgqueue-bench-cache-*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	config := Config{
		MaxBlocks:         10000,
		ReorgBuffer:       100, // Larger cache
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	writer, _, err := NewReorgQueue(db, config)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()

	// Pre-populate
	for i := uint64(1); i <= 200; i++ {
		block := Block{
			Number: i,
			Hash:   common.BytesToHash([]byte{byte(i), byte(i >> 8)}),
			Parent: common.BytesToHash([]byte{byte(i - 1), byte((i - 1) >> 8)}),
			Block:  make([]byte, 100),
		}
		if err := writer.AddBlock(ctx, block); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	// Add blocks that validate parent from cache
	for i := 0; i < b.N; i++ {
		// This should hit cache for parent validation
		num := uint64(201 + (i % 50)) // Keep within cache range
		block := Block{
			Number: num,
			Hash:   common.HexToHash(fmt.Sprintf("0x%x%d", num, i)),
			Parent: common.BytesToHash([]byte{byte(num - 1), byte((num - 1) >> 8)}),
			Block:  make([]byte, 100),
		}
		if err := writer.AddBlock(ctx, block); err != nil {
			b.Fatal(err)
		}
	}
}