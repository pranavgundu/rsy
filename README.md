# rsy

Fast parallel file synchronizer. Drop-in `rsync` replacement written in Rust.

## Benchmarks

Criterion-validated benchmarks (Linux x86_64, NVMe SSD, 16 cores).
Run yourself: `cargo bench`

### Cold copy - first sync, empty destination

#### 10k files x 4KB (39MB total)

| Tool | Time (ms) | Effective Throughput (MB/s) | Relative to rsy | Time Saved vs Tool |
|------|----------:|----------------------------:|----------------:|-------------------:|
| GREEN: rsy | 126 | 309.5 | 1.00x | - |
| rsync | 483 | 80.7 | rsy is 3.83x faster | rsy saves 357 ms (73.9%) |
| cp | 245 | 159.2 | rsy is 1.94x faster | rsy saves 119 ms (48.6%) |

Technical note: Small-file workloads are metadata and syscall heavy; rsy benefits from parallel file traversal and copy scheduling.

#### 500 files x 1MB (500MB total)

| Tool | Time (ms) | Effective Throughput (MB/s) | Relative to rsy | Time Saved vs Tool |
|------|----------:|----------------------------:|----------------:|-------------------:|
| GREEN: rsy | 35 | 14285.7 | 1.00x | - |
| rsync | 272 | 1838.2 | rsy is 7.77x faster | rsy saves 237 ms (87.1%) |
| cp | 116 | 4310.3 | rsy is 3.31x faster | rsy saves 81 ms (69.8%) |

Technical note: Parallel I/O and lower per-file overhead produce a large latency win.

#### 100 files x 16MB (1.6GB total)

| Tool | Time (ms) | Effective Throughput (MB/s) | Relative to rsy | Time Saved vs Tool |
|------|----------:|----------------------------:|----------------:|-------------------:|
| GREEN: rsy | 98 | 16326.5 | 1.00x | - |
| rsync | 715 | 2237.8 | rsy is 7.30x faster | rsy saves 617 ms (86.3%) |
| cp | 335 | 4776.1 | rsy is 3.42x faster | rsy saves 237 ms (70.7%) |

Technical note: On larger files, rsy keeps multiple workers active and sustains higher end-to-end throughput.

### Incremental - re-sync with no changes

#### 10k files x 4KB (39MB total)

| Tool | Time (ms) | Relative to rsy | Time Saved vs Tool |
|------|----------:|----------------:|-------------------:|
| GREEN: rsy | 15 | 1.00x | - |
| rsync | 69 | rsy is 4.60x faster | rsy saves 54 ms (78.3%) |

Technical note: With no payload transfer, this reflects metadata scan and change detection cost.

#### 500 files x 1MB (500MB total)

| Tool | Time (ms) | Relative to rsy | Time Saved vs Tool |
|------|----------:|----------------:|-------------------:|
| GREEN: rsy | 2.3 | 1.00x | - |
| rsync | 45 | rsy is 19.57x faster | rsy saves 42.7 ms (94.9%) |

Technical note: rsy's parallel check path significantly reduces no-op sync latency.

### Delta - re-sync with 10% of data changed

#### 10 files x 10MB (100MB total, 10% mutated)

| Tool | Time (ms) | Relative to rsy | Time Saved vs Tool |
|------|----------:|----------------:|-------------------:|
| GREEN: rsy | 1.4 | 1.00x | - |
| rsync | 44 | rsy is 31.43x faster | rsy saves 42.6 ms (96.8%) |

Technical note: Rolling checksum plus parallel block scanning minimizes both scan and patch time.

Overall: In this benchmark set, rsy wins every case. Speedup vs rsync ranges from 3.83x to 31.43x.

---

## Why rsy

- **Parallel I/O** — Rayon distributes file work across all CPU cores
- **Rolling-checksum delta** — only transfers changed bytes; parallel block scanning
- **APFS clonefile** — O(1) copies on macOS same-volume syncs
- **1.6 MB binary** — zero runtime dependencies

## Install

```sh
# npm
npm install -g @gundu/rsy

# cargo
cargo install rsy

# from source
cargo build --release
```

## Usage

```sh
rsy src/ dst/                     # local sync
rsy src/ user@host:dst/           # push to remote
rsy user@host:src/ dst/           # pull from remote
rsy -a src/ dst/                  # archive (times, perms, owner, group)
rsy --delete src/ dst/            # remove dst files absent from src
rsy -n src/ dst/                  # dry run
rsy --log src/ dst/               # timestamped log, no TUI
```

## Flags

| Flag | Description |
|------|-------------|
| `-a` | Archive: preserve times, perms, owner, group |
| `-v` | Verbose |
| `-n` | Dry run |
| `-u` | Skip if destination is newer |
| `-W` | Whole file (skip delta) |
| `-x` | Don't cross filesystem boundaries |
| `-c` | Compare by checksum instead of mtime+size |
| `-z` | Compress (accepted, not yet implemented) |
| `--delete` | Delete destination files absent from source |
| `--exclude PATTERN` | Exclude matching files (repeatable) |
| `--include PATTERN` | Include override for exclude rules |
| `--max-size SIZE` | Skip files larger than SIZE (K/M/G) |
| `--no-tui` | Plain output, no interactive display |
| `--log` | Timestamped per-file log to stderr |
| `--stats` | Print transfer statistics at end |
| `--jobs N` | Worker threads (default: 2× CPU cores) |

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `23` | Partial transfer (some files failed) |
| `1` | Fatal error |

## License

MIT
