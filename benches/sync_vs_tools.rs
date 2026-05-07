//! Comparative benchmarks: rsy vs rsync vs cp
//!
//! Each scenario is run with Criterion and measures wall-clock time for a full
//! sync of a generated corpus.  The corpus is written once per benchmark group
//! and re-used across iterations; Criterion handles warm-up and statistical
//! aggregation.
//!
//! Run:
//!   cargo bench
//!   cargo bench -- --baseline main          # compare against saved baseline
//!   cargo bench 2>/dev/null | tee bench.txt  # save results

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;

// ─── corpus generation ───────────────────────────────────────────────────────

struct Corpus {
    _src_dir: TempDir,
    src: PathBuf,
    total_bytes: u64,
}

#[derive(Clone, Copy)]
struct CorpusSpec {
    label: &'static str,
    /// number of files to create
    file_count: usize,
    /// size of each file in bytes
    file_size: usize,
    /// how many subdirectory levels (0 = flat)
    depth: usize,
}

impl CorpusSpec {
    const fn total_bytes(&self) -> u64 {
        (self.file_count * self.file_size) as u64
    }
}

fn generate_corpus(spec: &CorpusSpec) -> Corpus {
    let dir = TempDir::new().expect("tmpdir");
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();

    // deterministic pseudo-random content (no rand dep needed)
    let mut state: u64 = 0xdeadbeef_cafebabe;
    let mut next_byte = move || -> u8 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state as u8
    };

    let dirs: Vec<PathBuf> = if spec.depth == 0 {
        vec![src.clone()]
    } else {
        let mut d = vec![src.clone()];
        for i in 0..spec.depth {
            let sub = src.join(format!("d{i}"));
            fs::create_dir_all(&sub).unwrap();
            d.push(sub);
        }
        d
    };

    for i in 0..spec.file_count {
        let parent = &dirs[i % dirs.len()];
        let path = parent.join(format!("f{i:07}.bin"));
        let data: Vec<u8> = (0..spec.file_size).map(|_| next_byte()).collect();
        fs::write(&path, &data).unwrap();
    }

    Corpus {
        total_bytes: spec.total_bytes(),
        src,
        _src_dir: dir,
    }
}

// ─── tool runners ────────────────────────────────────────────────────────────

fn rsy_bin() -> PathBuf {
    // CARGO_MANIFEST_DIR is set by cargo for all build/test/bench invocations
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR not set — run via `cargo bench`");
    let p = PathBuf::from(manifest).join("target/release/rsy");
    if !p.exists() {
        panic!("rsy binary not found at {p:?}. Run `cargo build --release` first.");
    }
    p
}

fn run_rsy(src: &Path, dst: &Path) -> Duration {
    let dst_str = dst.to_str().unwrap().to_owned() + "/";
    let src_str = src.to_str().unwrap().to_owned() + "/";
    let bin = rsy_bin();
    let t = Instant::now();
    let status = Command::new(&bin)
        .args([&src_str, &dst_str])
        .status()
        .expect("failed to run rsy");
    let elapsed = t.elapsed();
    assert!(status.success(), "rsy exited with {status}");
    elapsed
}

fn run_rsync(src: &Path, dst: &Path) -> Duration {
    let dst_str = dst.to_str().unwrap().to_owned() + "/";
    let src_str = src.to_str().unwrap().to_owned() + "/";
    let t = Instant::now();
    let status = Command::new("rsync")
        .args(["-a", "--no-compress", &src_str, &dst_str])
        .status()
        .expect("failed to run rsync — is it installed?");
    let elapsed = t.elapsed();
    assert!(status.success(), "rsync exited with {status}");
    elapsed
}

fn run_cp(src: &Path, dst: &Path) -> Duration {
    // cp -r copies src/ INTO dst/, so use the parent
    let t = Instant::now();
    let status = Command::new("cp")
        .args(["-r", src.to_str().unwrap(), dst.to_str().unwrap()])
        .status()
        .expect("failed to run cp");
    let elapsed = t.elapsed();
    assert!(status.success(), "cp exited with {status}");
    elapsed
}

// ─── benchmark helpers ───────────────────────────────────────────────────────

fn cold_sync_bench(c: &mut Criterion, spec: CorpusSpec) {
    let corpus = generate_corpus(&spec);
    let mut group = c.benchmark_group(format!("cold/{}", spec.label));
    group.throughput(Throughput::Bytes(spec.total_bytes()));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("rsy", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let dst = TempDir::new().unwrap();
                total += run_rsy(&corpus.src, dst.path());
            }
            total
        })
    });

    group.bench_function("rsync", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let dst = TempDir::new().unwrap();
                total += run_rsync(&corpus.src, dst.path());
            }
            total
        })
    });

    group.bench_function("cp", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let dst = TempDir::new().unwrap();
                total += run_cp(&corpus.src, dst.path());
            }
            total
        })
    });

    group.finish();
}

fn incremental_bench(c: &mut Criterion, spec: CorpusSpec) {
    let corpus = generate_corpus(&spec);
    let mut group = c.benchmark_group(format!("incremental/{}", spec.label));
    group.throughput(Throughput::Bytes(spec.total_bytes()));
    group.sample_size(20);

    // pre-populate dst so iterations measure true incremental (nothing changed)
    let dst_rsy = TempDir::new().unwrap();
    let dst_rsync = TempDir::new().unwrap();
    run_rsy(&corpus.src, dst_rsy.path());
    run_rsync(&corpus.src, dst_rsync.path());

    group.bench_function("rsy", |b| b.iter(|| run_rsy(&corpus.src, dst_rsy.path())));

    group.bench_function("rsync", |b| {
        b.iter(|| run_rsync(&corpus.src, dst_rsync.path()))
    });

    group.finish();
}

fn delta_bench(c: &mut Criterion, spec: CorpusSpec) {
    let corpus = generate_corpus(&spec);
    let mut group = c.benchmark_group(format!("delta/{}", spec.label));
    group.throughput(Throughput::Bytes(spec.total_bytes()));
    group.sample_size(10);

    // pre-populate dst, then mutate ~10% of each file in src
    let dst_rsy = TempDir::new().unwrap();
    let dst_rsync = TempDir::new().unwrap();
    run_rsy(&corpus.src, dst_rsy.path());
    run_rsync(&corpus.src, dst_rsync.path());

    // patch 10% of each file with new bytes
    for entry in fs::read_dir(&corpus.src).unwrap().flatten() {
        if entry.file_type().unwrap().is_file() {
            let path = entry.path();
            let mut data = fs::read(&path).unwrap();
            let patch_len = data.len() / 10;
            let offset = data.len() / 2;
            for b in &mut data[offset..offset + patch_len] {
                *b = b.wrapping_add(1);
            }
            fs::write(&path, &data).unwrap();
        }
    }

    group.bench_function("rsy", |b| b.iter(|| run_rsy(&corpus.src, dst_rsy.path())));

    group.bench_function("rsync", |b| {
        b.iter(|| run_rsync(&corpus.src, dst_rsync.path()))
    });

    group.finish();
}

// ─── benchmark definitions ────────────────────────────────────────────────────

const SMALL_FILES: CorpusSpec = CorpusSpec {
    label: "10k-files-4KB",
    file_count: 10_000,
    file_size: 4_096,
    depth: 3,
};

const MEDIUM_FILES: CorpusSpec = CorpusSpec {
    label: "500-files-1MB",
    file_count: 500,
    file_size: 1_048_576,
    depth: 2,
};

const LARGE_FILES: CorpusSpec = CorpusSpec {
    label: "100-files-16MB",
    file_count: 100,
    file_size: 16_777_216,
    depth: 1,
};

const DELTA_CORPUS: CorpusSpec = CorpusSpec {
    label: "delta-10pct-changed",
    file_count: 10,
    file_size: 10_485_760, // 10MB each → 100MB total
    depth: 0,
};

fn bench_cold_small(c: &mut Criterion) {
    cold_sync_bench(c, SMALL_FILES);
}
fn bench_cold_medium(c: &mut Criterion) {
    cold_sync_bench(c, MEDIUM_FILES);
}
fn bench_cold_large(c: &mut Criterion) {
    cold_sync_bench(c, LARGE_FILES);
}
fn bench_incremental_small(c: &mut Criterion) {
    incremental_bench(c, SMALL_FILES);
}
fn bench_incremental_medium(c: &mut Criterion) {
    incremental_bench(c, MEDIUM_FILES);
}
fn bench_delta(c: &mut Criterion) {
    delta_bench(c, DELTA_CORPUS);
}

criterion_group!(cold, bench_cold_small, bench_cold_medium, bench_cold_large);
criterion_group!(
    incremental,
    bench_incremental_small,
    bench_incremental_medium
);
criterion_group!(delta, bench_delta);

criterion_main!(cold, incremental, delta);
