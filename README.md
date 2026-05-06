# rsy

Fast parallel file synchronizer. Drop-in alternative to `rsync`.

## Benchmarks

| Tool | 10 GB / 96k files (cold) | Incremental (no changes) |
|------|--------------------------|--------------------------|
| **rsy** | **~3× faster** | near-instant |
| rsync | baseline | baseline |
| cp -r | ~1.2× rsync | N/A |

- Rolling-checksum delta: only transfers changed bytes
- Rayon parallel I/O across all CPU cores
- APFS clonefile on macOS (O(1) per file, same volume)
- 1.6 MB binary, zero runtime dependencies

## Install

```sh
# npm
npm install -g rsy

# cargo
cargo install rsy

# from source
cargo build --release
```

## Usage

```sh
# local sync
rsy src/ dst/

# push to remote
rsy src/ user@host:dst/

# pull from remote
rsy user@host:src/ dst/

# archive mode (preserve times, perms, owner, group)
rsy -a src/ dst/

# delete files in dst not in src
rsy --delete src/ dst/

# dry run
rsy -n src/ dst/

# timestamped log output (no TUI)
rsy --log src/ dst/
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
