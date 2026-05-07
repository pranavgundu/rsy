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
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;

// ─── corpus generation ───────────────────────────────────────────────────────

struct Corpus {
    _src_dir: TempDir,
    src: PathBuf,
    #[allow(dead_code)]
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
    // Large corpora: minimum samples, generous time window
    let (samples, secs) = if spec.total_bytes() >= 10 * 1024 * 1024 * 1024 {
        (3, 600)
    } else if spec.total_bytes() >= 1024 * 1024 * 1024 {
        (5, 120)
    } else {
        (10, 30)
    };
    group.sample_size(samples);
    group.measurement_time(Duration::from_secs(secs));

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

// ── small ──────────────────────────────────────────────────────────────────

const S_10K_4KB: CorpusSpec = CorpusSpec {
    label: "10k-files-4KB",
    file_count: 10_000,
    file_size: 4_096,
    depth: 3,
};
const S_50K_4KB: CorpusSpec = CorpusSpec {
    label: "50k-files-4KB",
    file_count: 50_000,
    file_size: 4_096,
    depth: 4,
};
const S_100K_4KB: CorpusSpec = CorpusSpec {
    label: "100k-files-4KB",
    file_count: 100_000,
    file_size: 4_096,
    depth: 4,
};

// ── medium ─────────────────────────────────────────────────────────────────

const M_500_1MB: CorpusSpec = CorpusSpec {
    label: "500-files-1MB",
    file_count: 500,
    file_size: 1_048_576,
    depth: 2,
};
const M_2K_1MB: CorpusSpec = CorpusSpec {
    label: "2k-files-1MB",
    file_count: 2_000,
    file_size: 1_048_576,
    depth: 2,
};
const M_10K_1MB: CorpusSpec = CorpusSpec {
    label: "10k-files-1MB",
    file_count: 10_000,
    file_size: 1_048_576,
    depth: 3,
};

// ── large ──────────────────────────────────────────────────────────────────

const L_100_16MB: CorpusSpec = CorpusSpec {
    label: "100-files-16MB",
    file_count: 100,
    file_size: 16_777_216,
    depth: 1,
};
const L_500_16MB: CorpusSpec = CorpusSpec {
    label: "500-files-16MB",
    file_count: 500,
    file_size: 16_777_216,
    depth: 2,
};
const L_16_64MB: CorpusSpec = CorpusSpec {
    label: "16-files-64MB",
    file_count: 16,
    file_size: 67_108_864,
    depth: 1,
}; // ~1GB
const L_32_64MB: CorpusSpec = CorpusSpec {
    label: "32-files-64MB",
    file_count: 32,
    file_size: 67_108_864,
    depth: 1,
}; // ~2GB
const L_64_64MB: CorpusSpec = CorpusSpec {
    label: "64-files-64MB",
    file_count: 64,
    file_size: 67_108_864,
    depth: 2,
}; // ~4GB
const L_160_64MB: CorpusSpec = CorpusSpec {
    label: "160-files-64MB",
    file_count: 160,
    file_size: 67_108_864,
    depth: 2,
}; // ~10GB
const L_240_64MB: CorpusSpec = CorpusSpec {
    label: "240-files-64MB",
    file_count: 240,
    file_size: 67_108_864,
    depth: 2,
}; // ~15GB
const L_400_64MB: CorpusSpec = CorpusSpec {
    label: "400-files-64MB",
    file_count: 400,
    file_size: 67_108_864,
    depth: 3,
}; // ~25GB
const L_640_64MB: CorpusSpec = CorpusSpec {
    label: "640-files-64MB",
    file_count: 640,
    file_size: 67_108_864,
    depth: 3,
}; // ~40GB
const L_800_64MB: CorpusSpec = CorpusSpec {
    label: "800-files-64MB",
    file_count: 800,
    file_size: 67_108_864,
    depth: 3,
}; // ~50GB
const L_1600_64MB: CorpusSpec = CorpusSpec {
    label: "1600-files-64MB",
    file_count: 1_600,
    file_size: 67_108_864,
    depth: 4,
}; // ~100GB

// ── delta ──────────────────────────────────────────────────────────────────

const DELTA_CORPUS: CorpusSpec = CorpusSpec {
    label: "delta-10pct-changed",
    file_count: 10,
    file_size: 10_485_760,
    depth: 0,
};

// ── bench fns ─────────────────────────────────────────────────────────────

fn bench_cold_10k_4kb(c: &mut Criterion) {
    cold_sync_bench(c, S_10K_4KB);
}
fn bench_cold_50k_4kb(c: &mut Criterion) {
    cold_sync_bench(c, S_50K_4KB);
}
fn bench_cold_100k_4kb(c: &mut Criterion) {
    cold_sync_bench(c, S_100K_4KB);
}
fn bench_cold_500_1mb(c: &mut Criterion) {
    cold_sync_bench(c, M_500_1MB);
}
fn bench_cold_2k_1mb(c: &mut Criterion) {
    cold_sync_bench(c, M_2K_1MB);
}
fn bench_cold_10k_1mb(c: &mut Criterion) {
    cold_sync_bench(c, M_10K_1MB);
}
fn bench_cold_100_16mb(c: &mut Criterion) {
    cold_sync_bench(c, L_100_16MB);
}
fn bench_cold_500_16mb(c: &mut Criterion) {
    cold_sync_bench(c, L_500_16MB);
}
fn bench_cold_1gb(c: &mut Criterion) {
    cold_sync_bench(c, L_16_64MB);
}
fn bench_cold_2gb(c: &mut Criterion) {
    cold_sync_bench(c, L_32_64MB);
}
fn bench_cold_4gb(c: &mut Criterion) {
    cold_sync_bench(c, L_64_64MB);
}
fn bench_cold_10gb(c: &mut Criterion) {
    cold_sync_bench(c, L_160_64MB);
}
fn bench_cold_15gb(c: &mut Criterion) {
    cold_sync_bench(c, L_240_64MB);
}
fn bench_cold_25gb(c: &mut Criterion) {
    cold_sync_bench(c, L_400_64MB);
}
fn bench_cold_40gb(c: &mut Criterion) {
    cold_sync_bench(c, L_640_64MB);
}
fn bench_cold_50gb(c: &mut Criterion) {
    cold_sync_bench(c, L_800_64MB);
}
fn bench_cold_100gb(c: &mut Criterion) {
    cold_sync_bench(c, L_1600_64MB);
}

fn bench_incr_10k_4kb(c: &mut Criterion) {
    incremental_bench(c, S_10K_4KB);
}
fn bench_incr_500_1mb(c: &mut Criterion) {
    incremental_bench(c, M_500_1MB);
}
fn bench_incr_1gb(c: &mut Criterion) {
    incremental_bench(c, L_16_64MB);
}
fn bench_incr_10gb(c: &mut Criterion) {
    incremental_bench(c, L_160_64MB);
}

fn bench_delta(c: &mut Criterion) {
    delta_bench(c, DELTA_CORPUS);
}

criterion_group!(
    cold,
    bench_cold_10k_4kb,
    bench_cold_50k_4kb,
    bench_cold_100k_4kb,
    bench_cold_500_1mb,
    bench_cold_2k_1mb,
    bench_cold_10k_1mb,
    bench_cold_100_16mb,
    bench_cold_500_16mb,
    bench_cold_1gb,
    bench_cold_2gb,
    bench_cold_4gb,
    bench_cold_10gb,
    bench_cold_15gb,
    bench_cold_25gb,
    bench_cold_40gb,
    bench_cold_50gb,
    bench_cold_100gb
);
criterion_group!(
    incremental,
    bench_incr_10k_4kb,
    bench_incr_500_1mb,
    bench_incr_1gb,
    bench_incr_10gb
);
criterion_group!(delta, bench_delta);

criterion_main!(cold, incremental, delta);
