# rsy

Fast parallel file synchronizer. Drop-in `rsync` replacement written in Rust.

## Benchmarks

Criterion-validated benchmarks (Linux x86_64, NVMe SSD, 16 cores).
Run yourself: `cargo bench`

### Cold copy — first sync, empty destination

```
10k × 4KB (39MB)
  rsy   ████░░░░░░░░░░░░░░░░░░░░░░░░░░  126ms  ████ 3.8× faster than rsync
  rsync ████████████████████████████░░  483ms
  cp    ████████░░░░░░░░░░░░░░░░░░░░░░  245ms

500 × 1MB (500MB)
  rsy   █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░   35ms  ████ 7.8× faster than rsync
  rsync ████████░░░░░░░░░░░░░░░░░░░░░░  272ms
  cp    ████░░░░░░░░░░░░░░░░░░░░░░░░░░  116ms

100 × 16MB (1.6GB)
  rsy   ████░░░░░░░░░░░░░░░░░░░░░░░░░░   98ms  ████ 7.3× faster than rsync
  rsync ████████████████████████████░░  715ms
  cp    █████████████░░░░░░░░░░░░░░░░░  335ms
```

### Incremental — re-sync with no changes

```
10k × 4KB (39MB)
  rsy   ██████░░░░░░░░░░░░░░░░░░░░░░░░   15ms  ████ 4.5× faster than rsync
  rsync ████████████████████████████░░   69ms

500 × 1MB (500MB)
  rsy   █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2.3ms  ████ 19.7× faster than rsync
  rsync ████████████████████████████░░   45ms
```

### Delta — re-sync with 10% of data changed

```
10 × 10MB (100MB, 10% mutated)
  rsy   █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1.4ms  ████ 30× faster than rsync
  rsync ████████████████████████████░░   44ms
```

Parallel block scanning vs rsync's serial pass.

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
