# rsy

Fast parallel file synchronizer. Drop-in `rsync` replacement written in Rust.

## Benchmarks

All benchmarks run on Linux x86_64, NVMe SSD, 16 cores. Times are wall-clock milliseconds.

### Many small files

| Corpus | rsy cold | rsync cold | cp cold | rsy incr | rsync incr |
|--------|----------|------------|---------|----------|------------|
| 10k × 4KB (39MB) | **164ms** | 484ms | 253ms | **150ms** | 479ms |
| 50k × 4KB (195MB) | **772ms** | 2347ms | 1214ms | **785ms** | 2330ms |
| 100k × 4KB (390MB) | **1605ms** | 4714ms | 2454ms | **1582ms** | 4703ms |
| 5k × 64KB (312MB) | **82ms** | 377ms | 189ms | **81ms** | 382ms |
| 20k × 64KB (1.2GB) | **365ms** | 1413ms | 703ms | **345ms** | 1392ms |
| 50k × 64KB (3.1GB) | **902ms** | 3479ms | 1797ms | **906ms** | 3531ms |

**3–3.9× faster** than rsync across all small-file workloads.

### Medium files (1MB each)

| Corpus | rsy cold | rsync cold | cp cold | rsy incr | rsync incr |
|--------|----------|------------|---------|----------|------------|
| 500 × 1MB (500MB) | **35ms** | 280ms | 116ms | **35ms** | 270ms |
| 2k × 1MB (2GB) | **129ms** | 959ms | 447ms | **135ms** | 1001ms |
| 10k × 1MB (10GB) | **1629ms** | 7557ms | 5962ms | 3951ms | **7320ms**† |

†At 10GB+ page cache pressure degrades rsy incremental. Cold copy stays fast.

### Large files

| Corpus | rsy cold | rsync cold | cp cold | rsy incr | rsync incr |
|--------|----------|------------|---------|----------|------------|
| 100 × 16MB (1.6GB) | **98ms** | 727ms | 319ms | **99ms** | 706ms |
| 500 × 16MB (8GB) | **930ms** | 5973ms | 5138ms | 3834ms | **6577ms**† |
| 16 × 64MB (1GB) | **68ms** | 468ms | 217ms | **67ms** | 471ms |
| 32 × 64MB (2GB) | **125ms** | 886ms | 404ms | **133ms** | 876ms |
| 64 × 64MB (4GB) | **249ms** | 1744ms | 1041ms | **681ms** | 1702ms |
| 160 × 64MB (10GB) | **1322ms** | 7849ms | 7420ms | 6168ms | **8025ms**† |
| 240 × 64MB (15GB) | **4415ms** | 11581ms | 10371ms | 9609ms | **10831ms**† |
| 400 × 64MB (25GB) | **11596ms** | 17771ms | 16225ms | 15567ms | **17928ms**† |
| 800 × 64MB (50GB) | **49340ms** | 58064ms | — | — | — |

†Above ~8GB incremental, rsy checksum scanning exceeds rsync's lighter mtime check. Use `-c` flag only when needed at large scale.

### Summary

| Workload | rsy vs rsync | rsy vs cp |
|----------|-------------|-----------|
| Small files (cold) | **3–4× faster** | **1.5–2× faster** |
| Medium files (cold) | **4–7× faster** | **3× faster** |
| Large files cold ≤4GB | **6–7× faster** | **2–4× faster** |
| Large files cold 10–25GB | **1.5–2.6× faster** | similar |
| Large files cold ~50GB | **1.2× faster** | — |
| Incremental ≤4GB | **3–7× faster** | — |
| Incremental >8GB | competitive | — |

### Large file delta — 100MB file, 10% changed

| Tool | Time |
|------|------|
| **rsy** | **5ms** |
| rsync | 35ms |

**7× faster** — parallel block scanning vs rsync's serial pass.

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
