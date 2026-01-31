# minio-format

[![Crates.io](https://img.shields.io/crates/v/minio-format.svg)](https://crates.io/crates/minio-format)
[![Documentation](https://docs.rs/minio-format/badge.svg)](https://docs.rs/minio-format)
[![CI](https://github.com/dialohq/minio-format-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/dialohq/minio-format-rs/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Rust library for parsing MinIO's internal data formats. This enables direct access to MinIO object storage data without going through the S3 API, useful for data recovery, migration, forensics, and building custom storage tools.

## Features

- **xl.meta parsing** - Parse MinIO's binary metadata format (msgpack-based) to extract object metadata, erasure coding parameters, and version information
- **format.json parsing** - Parse cluster topology configuration to understand pool and erasure set layouts
- **Shard reading** - Read erasure-coded data shards with HighwayHash256 bitrot verification
- **Erasure decoding** - Reconstruct objects from erasure-coded shards using Reed-Solomon decoding

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
minio-format = "0.1"
```

## Quick Start

### Parse xl.meta

```rust
use minio_format::parse_xlmeta;
use std::fs;

let data = fs::read("path/to/xl.meta")?;
let meta = parse_xlmeta(&data)?;

println!("Object size: {} bytes", meta.size);
println!("Content-Type: {}", meta.content_type);
println!("ETag: {}", meta.etag);
println!("Erasure coding: {} data + {} parity shards",
    meta.data_blocks, meta.parity_blocks);
println!("Data directory: {}", meta.data_dir_string());
```

### Parse Cluster Topology

```rust
use minio_format::{parse_format, build_cluster_config};
use std::fs;

// Read format.json from each disk
let format_data = fs::read("disk1/format.json")?;
let disk_format = parse_format(&format_data)?;

println!("Pool ID: {}", disk_format.id);
println!("Disk UUID: {}", disk_format.xl.this);
println!("Erasure sets: {:?}", disk_format.xl.sets);
```

### Decode Erasure-Coded Objects

```rust
use minio_format::{parse_xlmeta, decode_object, FsShardReader};
use std::fs;

// Parse metadata
let xlmeta_data = fs::read("disk1/bucket/object/xl.meta")?;
let mut meta = parse_xlmeta(&xlmeta_data)?;
meta.bucket = "bucket".to_string();
meta.key = "object".to_string();

// Set up shard reader with disk paths
let reader = FsShardReader {
    disk_paths: vec![
        "/mnt/disk1".to_string(),
        "/mnt/disk2".to_string(),
        "/mnt/disk3".to_string(),
        "/mnt/disk4".to_string(),
    ],
};

// Decode object (handles missing shards via Reed-Solomon)
let data = decode_object(&reader, &meta, &[])?;
fs::write("recovered_object.bin", &data)?;
```

### Custom Shard Reader

Implement `ShardReader` trait for custom storage backends:

```rust
use minio_format::{ShardReader, decode_object};
use anyhow::Result;

struct MyShardReader {
    // Your storage backend
}

impl ShardReader for MyShardReader {
    fn read_shard(
        &self,
        disk_index: usize,
        bucket: &str,
        key: &str,
        data_dir: &str,
        part_num: i32,
    ) -> Result<Option<Vec<u8>>> {
        // Read shard from your storage backend
        // Return None if shard is missing (will be reconstructed)
        todo!()
    }
}
```

## Data Structures

### ObjectMeta

Contains all metadata for a MinIO object:

| Field | Type | Description |
|-------|------|-------------|
| `bucket` | `String` | Bucket name (set by caller) |
| `key` | `String` | Object key (set by caller) |
| `version_type` | `VersionType` | Object, DeleteMarker, or Legacy |
| `version_id` | `Uuid16` | Version UUID |
| `data_dir` | `Uuid16` | Data directory UUID |
| `data_blocks` | `usize` | Number of data shards (EC M) |
| `parity_blocks` | `usize` | Number of parity shards (EC N) |
| `block_size` | `i64` | Erasure block size (typically 1 MiB) |
| `erasure_index` | `usize` | This disk's position in distribution (1-based) |
| `distribution` | `Vec<u8>` | Shard distribution order |
| `parts` | `Vec<PartMeta>` | Multipart upload parts |
| `size` | `i64` | Total object size in bytes |
| `mod_time` | `i64` | Modification time (nanos since epoch) |
| `etag` | `String` | Object ETag |
| `content_type` | `String` | Content-Type header |
| `user_meta` | `HashMap<String, String>` | User-defined metadata |

### Erasure Coding

MinIO uses Reed-Solomon erasure coding. A typical configuration:

- **EC:4** (default): 2 data + 2 parity shards (50% storage overhead, tolerates 2 disk failures)
- **EC:8**: 4 data + 4 parity shards
- **EC:16**: 8 data + 8 parity shards

The `distribution` array in xl.meta defines which shard goes to which disk. For example, `[3, 4, 5, 1, 2]` means disk 1 stores shard 3, disk 2 stores shard 4, etc.

## MinIO Data Layout

MinIO stores objects using this directory structure:

```
<disk>/
  format.json                           # Cluster topology
  <bucket>/
    <object-key>/
      xl.meta                           # Object metadata
      <data-dir-uuid>/
        part.1                          # Shard for part 1
        part.2                          # Shard for part 2 (multipart)
        ...
```

Each shard file contains:
- 32-byte HighwayHash256 checksum
- Erasure-coded data block(s)

For large objects, each shard contains multiple blocks, each prefixed with its own 32-byte hash.

## Bitrot Protection

MinIO uses HighwayHash256 for bitrot detection. This library verifies checksums when reading shards:

```rust
use minio_format::shard::read_shard_block;

// Read with verification (default)
let block = read_shard_block(&shard_data, block_index, shard_size, true)?;

// Read without verification (faster, use with caution)
let block = read_shard_block(&shard_data, block_index, shard_size, false)?;
```

## Supported Versions

- xl.meta format version 1.3+ (MinIO RELEASE.2022-01-08 and later)
- Older xl.meta versions (< 1.3) are detected and rejected with a clear error

## Use Cases

- **Data Recovery**: Recover objects from damaged MinIO clusters
- **Migration**: Extract data for migration to other storage systems
- **Forensics**: Analyze MinIO storage at the file level
- **Custom Tools**: Build specialized storage tools that work directly with MinIO data
- **Testing**: Generate and verify MinIO-compatible data structures

## License

MIT License - see [LICENSE](LICENSE) for details.

This is a clean-room implementation that parses MinIO's documented data formats. It does not contain any MinIO source code.

## Related Projects

- [MinIO](https://github.com/minio/minio) - High Performance Object Storage
- [minio-rs](https://github.com/minio/minio-rs) - Official MinIO Rust SDK (S3 API client)
