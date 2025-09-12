# ReorgQueue

A robust blockchain block queue implementation with built-in reorganization protection for Ethereum-compatible chains. ReorgQueue provides a reliable way to process blockchain data while handling chain reorganizations gracefully.

## Features

- **Reorg Protection**: Maintains a configurable buffer of recent blocks to handle chain reorganizations
- **Persistent Storage**: SQLite-based storage for reliability across restarts
- **Memory Cache**: In-memory cache for recent blocks to improve performance
- **Batch Operations**: Support for batch block insertions for efficiency
- **Backoff Mechanism**: Configurable exponential backoff when queue is full
- **Metrics**: Prometheus metrics for monitoring queue operations
- **Trace Support**: Stores multiple types of blockchain traces including:
  - Call traces
  - Prestate traces
  - Keccak256 preimage traces
  - State access traces

## Installation

```bash
go get github.com/draganm/reorgqueue
```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "database/sql"
    "log"
    
    "github.com/draganm/reorgqueue"
    "github.com/ethereum/go-ethereum/common"
    _ "github.com/mattn/go-sqlite3"
)

func main() {
    // Open database connection
    db, err := sql.Open("sqlite3", "blocks.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create reorg queue with default configuration
    writer, consumer, err := reorgqueue.NewReorgQueueWithDefaults(db)
    if err != nil {
        log.Fatal(err)
    }

    // Add a block
    ctx := context.Background()
    block := reorgqueue.Block{
        Number: 1000,
        Hash:   common.HexToHash("0x123..."),
        Parent: common.HexToHash("0x122..."),
        Block:  []byte("block data"),
    }
    
    err = writer.AddBlock(ctx, block)
    if err != nil {
        log.Printf("Failed to add block: %v", err)
    }

    // Consume blocks
    blocks, err := consumer.GetBlocks(ctx, 10)
    if err != nil {
        log.Printf("Failed to get blocks: %v", err)
    }

    // Process blocks...
    
    // Delete processed blocks
    if len(blocks) > 0 {
        err = consumer.DeleteBlocks(ctx, blocks[0].Number, blocks[len(blocks)-1].Number)
        if err != nil {
            log.Printf("Failed to delete blocks: %v", err)
        }
    }
}
```

### Custom Configuration

```go
config := reorgqueue.Config{
    MaxBlocks:         5000,              // Maximum blocks to store
    ReorgBuffer:       128,               // Blocks to keep for reorg protection
    InitialBackoff:    100 * time.Millisecond,
    MaxBackoff:        30 * time.Second,
    BackoffMultiplier: 2.0,
}

writer, consumer, err := reorgqueue.NewReorgQueue(db, config)
```

## API

### Writer Interface

- `AddBlock(ctx context.Context, block Block) error` - Add a single block to the queue
- `AddBlocksBatch(ctx context.Context, blocks []Block) error` - Add multiple blocks in a batch

### Consumer Interface

- `GetBlocks(ctx context.Context, maxBlocks uint) ([]Block, error)` - Retrieve blocks from the queue
- `DeleteBlocks(ctx context.Context, startBlock, endBlock uint64) error` - Delete processed blocks
- `GetHighestBlock(ctx context.Context) (uint64, error)` - Get the highest block number in queue
- `GetBlockCount(ctx context.Context) (uint, error)` - Get total number of blocks in queue
- `GetTailBlock() (number uint64, hash common.Hash, exists bool)` - Get the tail block information

### Block Structure

```go
type Block struct {
    Number                   uint64
    Hash                     common.Hash
    Parent                   common.Hash
    Block                    []byte
    Receipts                 []byte
    CallTraces               []byte
    PrestateTraces           []byte
    Keccak256PreimageTraces  []byte
    StateAccessTraces        []byte
}
```

## How It Works

1. **Block Addition**: When blocks are added, they are validated against the parent chain
2. **Reorg Detection**: If a block with a different parent is added at an existing height, older blocks are removed
3. **Buffer Protection**: The most recent N blocks (ReorgBuffer) are kept and not available for consumption
4. **Consumption**: Only blocks older than the reorg buffer are available for processing
5. **Tail Tracking**: The system maintains a "tail" pointer to track processed blocks

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `MaxBlocks` | 2000 | Maximum number of blocks to store in the queue |
| `ReorgBuffer` | 64 | Number of recent blocks to keep for reorganization protection |
| `InitialBackoff` | 100ms | Initial backoff duration when queue is full |
| `MaxBackoff` | 5s | Maximum backoff duration |
| `BackoffMultiplier` | 1.5 | Multiplier for exponential backoff |

## Metrics

The following Prometheus metrics are exposed:

- `reorgqueue_blocks_inserted_total` - Total number of blocks inserted (with status label)
- `reorgqueue_blocks_deleted_total` - Total number of blocks deleted
- `reorgqueue_reorgs_detected_total` - Total number of reorganizations detected
- `reorgqueue_cache_hits_total` - Cache hit count
- `reorgqueue_cache_misses_total` - Cache miss count
- `reorgqueue_cache_size` - Current cache size
- `reorgqueue_queue_size` - Current queue size
- `reorgqueue_operation_duration_seconds` - Operation duration histogram

## Development

### Prerequisites

- Go 1.21 or higher
- SQLite3
- sqlc (for regenerating SQL code)

### Running Tests

```bash
go test ./...
```

### Regenerating SQL Code

If you modify the SQL schema or queries:

```bash
cd sqlitestore
sqlc generate
```

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please use the [GitHub issue tracker](https://github.com/draganm/reorgqueue/issues).