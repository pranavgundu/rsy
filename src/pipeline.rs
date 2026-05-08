use std::collections::HashSet;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;

use anyhow::{Result, bail};
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashSet;

use crate::checksum::{block_size as auto_block_size, file_hash};
use crate::delta::{PatchWriter, TokenSink, basis_sums, diff_stream};
use crate::filter::FilterList;
use crate::flist::{EntryKind, FileEntry, FileList, device_id};
use crate::protocol::{
    ReceiverReq, TAG_FLIST_END, TAG_FLIST_ENTRY, TAG_FLIST_START, WireSink, apply_token_stream,
    read_entry, read_receiver_req, read_sum_blocks, write_done, write_entry, write_file_hash,
    write_flist_end, write_flist_start, write_sum_block, write_sum_end, write_sum_head,
    write_token_end,
};
use crate::transport::Pipe;

// ─── path safety ─────────────────────────────────────────────────────────────

/// Lexically clean a relative path, rejecting absolute paths and `..` traversal.
/// Does NOT touch the filesystem — caller must validate parents separately
/// (see `validate_parents`).
fn clean_rel(rel: &Path) -> Result<PathBuf> {
    if rel.is_absolute() {
        bail!("path traversal: absolute path rejected: {}", rel.display());
    }
    let mut out = PathBuf::new();
    for c in rel.components() {
        match c {
            Component::CurDir => {}
            Component::Normal(seg) => out.push(seg),
            Component::ParentDir => {
                bail!("path traversal: '..' is not allowed: {}", rel.display())
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("path traversal: absolute path rejected: {}", rel.display())
            }
        }
    }
    Ok(out)
}

/// Lexically confine `rel` within `base`, rejecting any symlinked ancestor in
/// the destination tree. Single-call helper preserved for tests and code paths
/// without a pre-validated parent set.
fn confine_path(base: &Path, rel: &Path) -> Result<PathBuf> {
    let clean = clean_rel(rel)?;

    let mut cur = base.to_path_buf();
    let mut comps = clean.components().peekable();
    while let Some(c) = comps.next() {
        if let Component::Normal(seg) = c {
            cur.push(seg);
            if comps.peek().is_none() {
                break;
            }
            match fs::symlink_metadata(&cur) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    bail!(
                        "path traversal: destination contains symlinked ancestor: {}",
                        cur.display()
                    );
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(base.join(clean))
}

/// Walk every parent directory once, in parallel, and reject if any component
/// is a symlink. After this returns Ok, per-file `confine` calls become a
/// pure lexical operation — no syscalls.
fn validate_parents(base: &Path, parents: &HashSet<PathBuf>) -> Result<()> {
    parents.par_iter().try_for_each(|rel| {
        let clean = clean_rel(rel)?;
        let mut cur = base.to_path_buf();
        for c in clean.components() {
            if let Component::Normal(seg) = c {
                cur.push(seg);
                match fs::symlink_metadata(&cur) {
                    Ok(meta) if meta.file_type().is_symlink() => bail!(
                        "path traversal: destination contains symlinked ancestor: {}",
                        cur.display()
                    ),
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    })
}

fn safe_symlink_target(target: &Path) -> bool {
    if target.is_absolute() {
        return false;
    }
    let mut depth = 0usize;
    for c in target.components() {
        match c {
            Component::CurDir => {}
            Component::Normal(_) => depth += 1,
            Component::ParentDir => {
                if depth == 0 {
                    return false;
                }
                depth -= 1;
            }
            Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    true
}

// ─── options ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct SyncOpts {
    pub delete: bool,
    pub update: bool,
    pub ignore_existing: bool,
    pub whole_file: bool,
    pub checksum: bool,
    pub inplace: bool,
    pub one_file_system: bool,
    #[allow(dead_code)]
    pub preserve_times: bool,
    pub preserve_perms: bool,
    pub preserve_owner: bool,
    pub preserve_group: bool,
    pub filter: FilterList,
    pub max_size: Option<u64>,
    pub min_size: Option<u64>,
    pub block_size: Option<usize>,
    pub compress: bool,
    pub verbose: bool,
    pub progress: bool,
    pub stats: bool,
    pub dry_run: bool,
    pub progress_tx: Option<crossbeam_channel::Sender<ProgressEvent>>,
}

// ─── progress events ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct FileRecord {
    pub path: String,
    pub size: u64,
    pub literal: u64,
    pub matched: u64,
    pub skipped: bool,
}

pub enum ProgressEvent {
    Start {
        total_files: usize,
        total_bytes: u64,
    },
    File(FileRecord),
    Done(Stats),
    Error(String),
}

// ─── stats ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub files_total: usize,
    pub files_xferred: usize,
    pub files_skipped: usize,
    pub files_errored: usize,
    pub literal_bytes: u64,
    pub matched_bytes: u64,
    pub total_size: u64,
}

struct ParStats {
    xferred: AtomicUsize,
    skipped: AtomicUsize,
    errored: AtomicUsize,
    literal: AtomicU64,
    matched: AtomicU64,
}

impl ParStats {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            xferred: AtomicUsize::new(0),
            skipped: AtomicUsize::new(0),
            errored: AtomicUsize::new(0),
            literal: AtomicU64::new(0),
            matched: AtomicU64::new(0),
        })
    }
    fn add(&self, xferred: bool, lit: u64, mat: u64) {
        if xferred {
            self.xferred.fetch_add(1, Ordering::Relaxed);
        } else {
            self.skipped.fetch_add(1, Ordering::Relaxed);
        }
        self.literal.fetch_add(lit, Ordering::Relaxed);
        self.matched.fetch_add(mat, Ordering::Relaxed);
    }
    fn snapshot(&self, total: usize, total_size: u64) -> Stats {
        Stats {
            files_total: total,
            files_xferred: self.xferred.load(Ordering::Relaxed),
            files_skipped: self.skipped.load(Ordering::Relaxed),
            files_errored: self.errored.load(Ordering::Relaxed),
            literal_bytes: self.literal.load(Ordering::Relaxed),
            matched_bytes: self.matched.load(Ordering::Relaxed),
            total_size,
        }
    }
}

// ─── local sync ──────────────────────────────────────────────────────────────

pub fn sync_local(src_root: &Path, dst_root: &Path, opts: &SyncOpts) -> Result<Stats> {
    let root_dev = if opts.one_file_system {
        device_id(src_root)
    } else {
        None
    };

    // Only walk dst when --delete needs the dst inventory. Saves a full tree
    // walk on every cold sync.
    let (src_res, dst_res) = rayon::join(
        || FileList::build(src_root, root_dev),
        || -> Result<FileList> {
            if opts.delete {
                Ok(FileList::build(dst_root, None).unwrap_or_else(|_| FileList::empty()))
            } else {
                Ok(FileList::empty())
            }
        },
    );
    let src_list = src_res?;
    let dst_list = dst_res?;

    let src_filtered: Vec<&FileEntry> = src_list
        .0
        .iter()
        .filter(|e| {
            if !opts.filter.is_empty() && !opts.filter.allow(&e.path, e.is_dir()) {
                return false;
            }
            if let Some(max) = opts.max_size
                && e.size > max
            {
                return false;
            }
            if let Some(min) = opts.min_size
                && e.size < min
            {
                return false;
            }
            true
        })
        .collect();

    // Pre-create directories serially (sorted order ensures parents first).
    if !opts.dry_run {
        for e in src_filtered.iter().filter(|e| e.is_dir()) {
            let dst = dst_root.join(clean_rel(&e.path)?);
            fs::create_dir_all(dst)?;
        }
    }

    // Pre-validate every parent dir once, in parallel — eliminates the per-file
    // ancestor symlink walk that used to dominate small-file workloads.
    if !opts.dry_run {
        let mut parents: HashSet<PathBuf> = HashSet::new();
        for e in src_filtered.iter().filter(|e| e.is_regular()) {
            if let Some(p) = e.path.parent()
                && !p.as_os_str().is_empty()
            {
                parents.insert(p.to_path_buf());
            }
        }
        validate_parents(dst_root, &parents)?;
    }

    // Symlinks
    for e in src_filtered.iter() {
        if let EntryKind::Symlink { target } = &e.kind {
            if opts.dry_run {
                continue;
            }
            if !safe_symlink_target(target) {
                eprintln!(
                    "warning: skipping symlink with unsafe target: {}",
                    e.path.display()
                );
                continue;
            }
            let dst = dst_root.join(clean_rel(&e.path)?);
            let _ = fs::remove_file(&dst);
            if let Some(p) = dst.parent() {
                fs::create_dir_all(p)?;
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, &dst)?;
        }
    }

    let regular: Vec<&FileEntry> = src_filtered
        .iter()
        .copied()
        .filter(|e| e.is_regular())
        .collect();
    let total = regular.len();
    let total_size = regular.iter().map(|e| e.size).sum::<u64>();
    let pstats = ParStats::new();
    let pstats2 = Arc::clone(&pstats);

    if let Some(tx) = &opts.progress_tx {
        let _ = tx.send(ProgressEvent::Start {
            total_files: total,
            total_bytes: total_size,
        });
    }

    regular.into_par_iter().for_each(|entry| {
        let src = src_root.join(&entry.path);
        let rel = match clean_rel(&entry.path) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("warning: {}: {e}", entry.path.display());
                pstats2.errored.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let dst = dst_root.join(rel);
        match sync_one(&src, &dst, entry, opts) {
            Ok((xferred, lit, mat)) => {
                pstats2.add(xferred, lit, mat);
                if let Some(tx) = &opts.progress_tx {
                    let _ = tx.send(ProgressEvent::File(FileRecord {
                        path: entry.path.to_string_lossy().into_owned(),
                        size: entry.size,
                        literal: lit,
                        matched: mat,
                        skipped: !xferred,
                    }));
                } else if opts.progress {
                    let x = pstats2.xferred.load(Ordering::Relaxed);
                    eprint!("\r{}/{} files", x, total);
                }
            }
            Err(e) => {
                eprintln!("warning: {}: {e}", entry.path.display());
                pstats2.errored.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    if opts.progress && opts.progress_tx.is_none() {
        eprintln!();
    }

    if opts.delete {
        let src_paths: FxHashSet<&PathBuf> = src_list.0.iter().map(|e| &e.path).collect();
        for e in &dst_list.0 {
            if !src_paths.contains(&e.path) {
                if opts.dry_run {
                    continue;
                }
                let p = match confine_path(dst_root, &e.path) {
                    Ok(p) => p,
                    Err(err) => {
                        eprintln!("warning: {}: {err}", e.path.display());
                        pstats.errored.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                if e.is_dir() {
                    let _ = fs::remove_dir_all(&p);
                } else {
                    let _ = fs::remove_file(&p);
                }
            }
        }
    }

    let stats = pstats.snapshot(total, total_size);
    if let Some(tx) = &opts.progress_tx {
        let _ = tx.send(ProgressEvent::Done(stats.clone()));
    }
    Ok(stats)
}

// ─── single file ─────────────────────────────────────────────────────────────

const MMAP_THRESHOLD: u64 = 512 * 1024;
/// Skip delta entirely above this size — basis mmap + rolling-hash on multi-GB
/// files would page-fault for longer than a flat clone/copy.
const DELTA_SIZE_LIMIT: u64 = 512 * 1024 * 1024;
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

enum Bytes {
    Mmap(Mmap),
    Heap(Vec<u8>),
    Empty,
}

impl Bytes {
    fn as_slice(&self) -> &[u8] {
        match self {
            Bytes::Mmap(m) => m,
            Bytes::Heap(v) => v,
            Bytes::Empty => &[],
        }
    }
}

fn read_for_delta(path: &Path, size: u64) -> Result<Bytes> {
    if size == 0 {
        return Ok(Bytes::Empty);
    }
    if size >= MMAP_THRESHOLD {
        let f = fs::File::open(path)?;
        Ok(Bytes::Mmap(unsafe { Mmap::map(&f)? }))
    } else {
        Ok(Bytes::Heap(fs::read(path)?))
    }
}

fn unique_tmp(dst: &Path) -> Result<(PathBuf, fs::File)> {
    let parent = dst.parent().unwrap_or_else(|| Path::new("."));
    let leaf = dst
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "rsy".to_string());
    for _ in 0..128 {
        let nonce = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = parent.join(format!(".{leaf}.rsy.{}.{}.tmp", std::process::id(), nonce));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(f) => return Ok((tmp, f)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    bail!("failed to create unique temp file for {}", dst.display())
}

/// macOS APFS clone: O(1) copy-on-write, no data movement. Returns Err on any
/// platform/filesystem mismatch so callers can fall back.
#[cfg(target_os = "macos")]
fn try_clonefile(src: &Path, dst: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let s = CString::new(src.as_os_str().as_bytes())?;
    let d = CString::new(dst.as_os_str().as_bytes())?;
    let r = unsafe { libc::clonefile(s.as_ptr(), d.as_ptr(), 0) };
    if r == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Linux reflink: O(1) copy-on-write on filesystems supporting FICLONE
/// (Btrfs, XFS reflink, OCFS2, and some overlay setups). Returns Err on
/// unsupported filesystems so callers can fall back.
#[cfg(target_os = "linux")]
fn try_reflink(src: &Path, dst: &Path) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let src_file = fs::File::open(src)?;
    let src_meta = src_file.metadata()?;
    let dst_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(src_meta.permissions().mode())
        .open(dst)?;

    let rc = unsafe { libc::ioctl(dst_file.as_raw_fd(), libc::FICLONE, src_file.as_raw_fd()) };
    if rc == 0 {
        fs::set_permissions(dst, src_meta.permissions())?;
        Ok(())
    } else {
        let err = std::io::Error::last_os_error();
        drop(dst_file);
        let _ = fs::remove_file(dst);
        Err(err)
    }
}

/// Whole-file cold copy. Tries platform reflink/clone first:
/// macOS `clonefile()` or Linux `FICLONE`. Falls back to `fs::copy`, which
/// itself uses efficient kernel copy paths where available.
fn fast_copy(src: &Path, dst: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        if try_clonefile(src, dst).is_ok() {
            return Ok(());
        }
    }
    #[cfg(target_os = "linux")]
    {
        if try_reflink(src, dst).is_ok() {
            return Ok(());
        }
    }
    fs::copy(src, dst)?;
    Ok(())
}

fn copy_atomic(src: &Path, dst: &Path) -> Result<()> {
    let (tmp, out) = unique_tmp(dst)?;
    drop(out);
    // clonefile/fs::copy expect dst to NOT exist; unique_tmp just created it.
    let _ = fs::remove_file(&tmp);
    if let Err(e) = fast_copy(src, &tmp) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp, dst) {
        let _ = fs::remove_file(&tmp);
        return Err(e.into());
    }
    Ok(())
}

fn write_atomic(
    dst: &Path,
    mut writer_fn: impl FnMut(&mut BufWriter<fs::File>) -> Result<()>,
) -> Result<()> {
    let (tmp, f) = unique_tmp(dst)?;
    let mut w = BufWriter::new(f);
    let res: Result<()> = (|| {
        writer_fn(&mut w)?;
        w.flush()?;
        Ok(())
    })();
    if let Err(e) = res {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp, dst) {
        let _ = fs::remove_file(&tmp);
        return Err(e.into());
    }
    Ok(())
}

fn sync_one(
    src: &Path,
    dst: &Path,
    entry: &FileEntry,
    opts: &SyncOpts,
) -> Result<(bool, u64, u64)> {
    let mut dst_meta = dst.symlink_metadata().ok();
    if dst_meta
        .as_ref()
        .is_some_and(|m| m.file_type().is_symlink())
    {
        fs::remove_file(dst)?;
        dst_meta = None;
    }
    let basis_exists = dst_meta.is_some();

    if opts.ignore_existing && basis_exists {
        return Ok((false, 0, entry.size));
    }

    if !opts.checksum
        && !opts.dry_run
        && let Some(ref m) = dst_meta
    {
        let dst_mtime = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(-1);
        if opts.update && dst_mtime > entry.mtime {
            return Ok((false, 0, entry.size));
        }
        if dst_mtime == entry.mtime && m.len() == entry.size {
            return Ok((false, 0, entry.size));
        }
    }

    if opts.dry_run {
        if opts.verbose {
            eprintln!("would send: {}", entry.path.display());
        }
        return Ok((true, entry.size, 0));
    }

    // Cold copy or whole-file: try APFS clone first, then fall back.
    if !basis_exists && !opts.checksum {
        fast_copy(src, dst)?;
        set_metadata(dst, entry, opts, true)?;
        if opts.verbose {
            eprintln!("{}", entry.path.display());
        }
        return Ok((true, entry.size, 0));
    }

    if (opts.whole_file || entry.size > DELTA_SIZE_LIMIT) && !opts.checksum {
        copy_atomic(src, dst)?;
        set_metadata(dst, entry, opts, true)?;
        if opts.verbose {
            eprintln!("{}", entry.path.display());
        }
        return Ok((true, entry.size, 0));
    }

    // ── delta path ─────────────────────────────────────────────────────────
    let src_bytes = read_for_delta(src, entry.size)?;
    let src_data = src_bytes.as_slice();

    let basis_bytes = if basis_exists {
        let basis_size = dst_meta.as_ref().map_or(0, |m| m.len());
        read_for_delta(dst, basis_size)?
    } else {
        Bytes::Empty
    };
    let basis = basis_bytes.as_slice();

    if opts.checksum && !basis.is_empty() && file_hash(src_data) == file_hash(basis) {
        return Ok((false, 0, src_data.len() as u64));
    }

    let blen = opts
        .block_size
        .unwrap_or_else(|| auto_block_size(entry.size))
        .clamp(512, 1_048_576);

    if opts.whole_file || basis.is_empty() {
        copy_atomic(src, dst)?;
        set_metadata(dst, entry, opts, true)?;
        if opts.verbose {
            eprintln!("{}", entry.path.display());
        }
        return Ok((true, src_data.len() as u64, 0));
    }

    let sums = basis_sums(basis, blen);

    // Stream patched output directly to disk via PatchWriter; no full-file Vec.
    let mut lit = 0u64;
    let mut mat = 0u64;
    if opts.inplace {
        let f = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(dst)?;
        let mut pw = PatchWriter::new(basis, BufWriter::new(f));
        let (l, m) = diff_stream(src_data, &sums, blen, &mut pw)?;
        lit = l as u64;
        mat = m as u64;
        let (mut w, hash) = pw.finalize();
        w.flush()?;
        debug_assert_eq!(hash, file_hash(src_data));
    } else {
        write_atomic(dst, |w| {
            let mut pw = PatchWriter::new(basis, w);
            let (l, m) = diff_stream(src_data, &sums, blen, &mut pw)?;
            lit = l as u64;
            mat = m as u64;
            let (_, hash) = pw.finalize();
            debug_assert_eq!(hash, file_hash(src_data));
            Ok(())
        })?;
    }

    set_metadata(dst, entry, opts, false)?;
    if opts.verbose {
        eprintln!(
            "{}: {}B literal, {}B matched",
            entry.path.display(),
            lit,
            mat
        );
    }
    Ok((true, lit, mat))
}

fn set_metadata(
    path: &Path,
    entry: &FileEntry,
    opts: &SyncOpts,
    perms_already_copied: bool,
) -> Result<()> {
    set_mtime(path, entry.mtime)?;
    // fs::copy preserves perms on Unix already; only chmod when patching or
    // when caller didn't go through fs::copy.
    if opts.preserve_perms && !perms_already_copied {
        set_mode(path, entry.mode)?;
    }
    if opts.preserve_owner || opts.preserve_group {
        set_owner(
            path,
            entry.uid,
            entry.gid,
            opts.preserve_owner,
            opts.preserve_group,
        )?;
    }
    Ok(())
}

// ─── network sender ──────────────────────────────────────────────────────────

pub fn run_sender(src_root: &Path, pipe: &mut Pipe, opts: &SyncOpts) -> Result<Stats> {
    let root_dev = if opts.one_file_system {
        device_id(src_root)
    } else {
        None
    };
    let src_list = FileList::build(src_root, root_dev)?;
    let mut w = BufWriter::new(&mut *pipe.tx);

    write_flist_start(&mut w)?;
    for e in &src_list.0 {
        write_entry(&mut w, e)?;
    }
    write_flist_end(&mut w)?;
    w.flush()?;

    let pstats = ParStats::new();
    let total = src_list.len();

    loop {
        match read_receiver_req(&mut *pipe.rx)? {
            ReceiverReq::Done => break,
            ReceiverReq::Sums(sh) => {
                let idx = sh.file_idx as usize;
                let entry = src_list
                    .0
                    .get(idx)
                    .ok_or_else(|| anyhow::anyhow!("file_idx {idx} out of range"))?;
                let blen = (sh.block_len as usize).clamp(512, 1_048_576);

                let raw = read_sum_blocks(&mut *pipe.rx, sh.count)?;
                let sums: Vec<crate::delta::BlockSum> = raw
                    .into_iter()
                    .enumerate()
                    .map(|(i, (r, s))| crate::delta::BlockSum {
                        rolling: r,
                        strong: s,
                        offset: (i * blen) as u64,
                    })
                    .collect();

                let src_path = src_root.join(&entry.path);
                let src_bytes = read_for_delta(&src_path, entry.size)?;
                let src_data = src_bytes.as_slice();
                let src_hash = file_hash(src_data);

                let (lit, mat) = if sh.count == 0 || opts.whole_file {
                    let mut sink = WireSink(&mut w);
                    sink.on_data(src_data)?;
                    (src_data.len(), 0)
                } else {
                    let mut sink = WireSink(&mut w);
                    diff_stream(src_data, &sums, blen, &mut sink)?
                };

                pstats.add(true, lit as u64, mat as u64);
                write_token_end(&mut w)?;
                write_file_hash(&mut w, &src_hash)?;
                w.flush()?;
            }
        }
    }

    Ok(pstats.snapshot(total, src_list.0.iter().map(|e| e.size).sum()))
}

// ─── network receiver ────────────────────────────────────────────────────────

/// Streams tokens from the wire into a destination file, copying bytes from
/// the basis mmap on Copy and from the wire on Data. Hashes incrementally
/// so the post-transfer integrity check needs no second pass.
struct ReceiverSink<'b, W: Write> {
    basis: &'b [u8],
    out: W,
    hasher: blake3::Hasher,
    lit: u64,
    mat: u64,
}

impl<'b, W: Write> TokenSink for ReceiverSink<'b, W> {
    fn on_copy(&mut self, offset: u64, len: u32) -> std::io::Result<()> {
        let s = offset as usize;
        let e = s.saturating_add(len as usize).min(self.basis.len());
        if s < self.basis.len() {
            let slice = &self.basis[s..e];
            self.hasher.update(slice);
            self.out.write_all(slice)?;
            self.mat += slice.len() as u64;
        }
        Ok(())
    }
    fn on_data(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.hasher.update(bytes);
        self.out.write_all(bytes)?;
        self.lit += bytes.len() as u64;
        Ok(())
    }
}

pub fn run_receiver(dst_root: &Path, pipe: &mut Pipe, opts: &SyncOpts) -> Result<Stats> {
    use crate::protocol::read_file_hash;
    use byteorder::ReadBytesExt;

    let tag = pipe.rx.read_u8()?;
    anyhow::ensure!(tag == TAG_FLIST_START, "expected flist start, got {tag}");

    let mut file_list: Vec<FileEntry> = Vec::new();
    loop {
        let t = pipe.rx.read_u8()?;
        match t {
            TAG_FLIST_ENTRY => file_list.push(read_entry(&mut *pipe.rx)?),
            TAG_FLIST_END => break,
            other => bail!("unexpected tag {other}"),
        }
    }

    let mut w = BufWriter::new(&mut *pipe.tx);
    let pstats = ParStats::new();
    let total = file_list.len();
    let total_bytes: u64 = file_list.iter().map(|e| e.size).sum();

    if let Some(tx) = &opts.progress_tx {
        let _ = tx.send(ProgressEvent::Start {
            total_files: total,
            total_bytes,
        });
    }

    for (idx, entry) in file_list.iter().enumerate() {
        let dst_path = dst_root.join(clean_rel(&entry.path)?);

        match &entry.kind {
            EntryKind::Dir => {
                fs::create_dir_all(&dst_path)?;
                continue;
            }
            EntryKind::Symlink { target } => {
                #[cfg(unix)]
                {
                    if !safe_symlink_target(target) {
                        eprintln!(
                            "warning: skipping symlink with unsafe target: {}",
                            entry.path.display()
                        );
                        continue;
                    }
                    let _ = fs::remove_file(&dst_path);
                    if let Some(p) = dst_path.parent() {
                        fs::create_dir_all(p)?;
                    }
                    std::os::unix::fs::symlink(target, &dst_path)?;
                }
                continue;
            }
            EntryKind::Regular => {}
            EntryKind::Other => continue,
        }

        if let Some(p) = dst_path.parent() {
            fs::create_dir_all(p)?;
        }

        let basis_bytes = if dst_path.exists() {
            let meta = fs::metadata(&dst_path)?;
            read_for_delta(&dst_path, meta.len())?
        } else {
            Bytes::Empty
        };
        let basis = basis_bytes.as_slice();

        let blen = opts
            .block_size
            .unwrap_or_else(|| auto_block_size(entry.size))
            .clamp(512, 1_048_576);
        let sums = if opts.whole_file {
            Vec::new()
        } else {
            basis_sums(basis, blen)
        };

        write_sum_head(&mut w, idx as u32, blen as u32, sums.len() as u32)?;
        for bs in &sums {
            write_sum_block(&mut w, bs.rolling, &bs.strong)?;
        }
        write_sum_end(&mut w)?;
        w.flush()?;

        // Stream tokens directly into a tmp file — never materialise the new
        // file in memory.
        let (tmp, f) = unique_tmp(&dst_path)?;
        let mut bw = BufWriter::new(f);
        let mut sink = ReceiverSink {
            basis,
            out: &mut bw,
            hasher: blake3::Hasher::new(),
            lit: 0,
            mat: 0,
        };
        let stream_res: Result<()> = (|| {
            apply_token_stream(&mut *pipe.rx, &mut sink)?;
            Ok(())
        })();
        if let Err(e) = stream_res {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        let lit = sink.lit;
        let mat = sink.mat;
        let computed = *sink.hasher.finalize().as_bytes();
        bw.flush()?;
        drop(bw);

        let expected = read_file_hash(&mut *pipe.rx)?;
        if computed != expected {
            let _ = fs::remove_file(&tmp);
            bail!("hash mismatch: {}", entry.path.display());
        }

        if opts.inplace {
            fs::rename(&tmp, &dst_path).or_else(|_| {
                let data = fs::read(&tmp)?;
                let _ = fs::remove_file(&tmp);
                fs::write(&dst_path, data)
            })?;
        } else if let Err(e) = fs::rename(&tmp, &dst_path) {
            let _ = fs::remove_file(&tmp);
            return Err(e.into());
        }

        pstats.add(true, lit, mat);

        if let Some(tx) = &opts.progress_tx {
            let _ = tx.send(ProgressEvent::File(FileRecord {
                path: entry.path.display().to_string(),
                size: entry.size,
                literal: lit,
                matched: mat,
                skipped: false,
            }));
        } else if opts.verbose || opts.progress {
            eprintln!("{}", entry.path.display());
        }

        set_metadata(&dst_path, entry, opts, false)?;
    }

    write_done(&mut w)?;
    w.flush()?;

    let stats = pstats.snapshot(total, total_bytes);
    if let Some(tx) = &opts.progress_tx {
        let _ = tx.send(ProgressEvent::Done(stats.clone()));
    }
    Ok(stats)
}

// ─── metadata helpers ─────────────────────────────────────────────────────────

#[cfg(unix)]
fn set_mtime(path: &Path, mtime: i64) -> Result<()> {
    filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(mtime, 0))?;
    Ok(())
}
#[cfg(not(unix))]
fn set_mtime(_: &Path, _: i64) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o777))?;
    Ok(())
}
#[cfg(not(unix))]
fn set_mode(_: &Path, _: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_owner(path: &Path, uid: u32, gid: u32, do_uid: bool, do_gid: bool) -> Result<()> {
    use nix::unistd::{Gid, Uid, chown};
    let u = if do_uid {
        Some(Uid::from_raw(uid))
    } else {
        None
    };
    let g = if do_gid {
        Some(Gid::from_raw(gid))
    } else {
        None
    };
    if u.is_some() || g.is_some() {
        chown(path, u, g)?;
    }
    Ok(())
}
#[cfg(not(unix))]
fn set_owner(_: &Path, _: u32, _: u32, _: bool, _: bool) -> Result<()> {
    Ok(())
}

// ─── stats printer ────────────────────────────────────────────────────────────

pub fn print_stats(s: &Stats) {
    fn human(n: u64) -> String {
        const G: u64 = 1 << 30;
        const M: u64 = 1 << 20;
        const K: u64 = 1 << 10;
        if n >= G {
            format!("{:.2} GB", n as f64 / G as f64)
        } else if n >= M {
            format!("{:.2} MB", n as f64 / M as f64)
        } else if n >= K {
            format!("{:.2} KB", n as f64 / K as f64)
        } else {
            format!("{} B", n)
        }
    }
    let transferred = s.literal_bytes + s.matched_bytes;
    let speedup = if s.literal_bytes > 0 {
        transferred as f64 / s.literal_bytes as f64
    } else {
        f64::INFINITY
    };
    eprintln!("Number of files: {}", s.files_total);
    eprintln!("Files transferred: {}", s.files_xferred);
    eprintln!("Files skipped: {}", s.files_skipped);
    eprintln!("Total file size: {}", human(s.total_size));
    eprintln!("Literal data: {}", human(s.literal_bytes));
    eprintln!("Matched data: {}", human(s.matched_bytes));
    if speedup.is_finite() {
        eprintln!("Speedup: {:.2}", speedup);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn confine_path_normal() {
        let base = Path::new("/dst");
        let r = confine_path(base, Path::new("a/b/c")).unwrap();
        assert_eq!(r, Path::new("/dst/a/b/c"));
    }

    #[test]
    fn confine_path_rejects_absolute() {
        assert!(confine_path(Path::new("/dst"), Path::new("/etc/passwd")).is_err());
    }

    #[test]
    fn confine_path_rejects_dotdot_escape() {
        assert!(confine_path(Path::new("/dst"), Path::new("../../etc/passwd")).is_err());
        assert!(confine_path(Path::new("/dst"), Path::new("a/../../etc")).is_err());
    }

    #[test]
    fn confine_path_rejects_dotdot_within_root() {
        assert!(confine_path(Path::new("/dst"), Path::new("a/b/../c")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn confine_path_rejects_symlinked_ancestor() {
        let base = tempfile::tempdir().unwrap();
        let linked = base.path().join("linked");
        std::os::unix::fs::symlink("/tmp", &linked).unwrap();
        assert!(confine_path(base.path(), Path::new("linked/file.txt")).is_err());
    }

    #[test]
    fn safe_symlink_target_rejects_escape() {
        assert!(safe_symlink_target(Path::new("target")));
        assert!(safe_symlink_target(Path::new("dir/../target")));
        assert!(!safe_symlink_target(Path::new("/etc/passwd")));
        assert!(!safe_symlink_target(Path::new("../outside")));
        assert!(!safe_symlink_target(Path::new("dir/../../outside")));
    }

    fn default_opts() -> SyncOpts {
        SyncOpts {
            preserve_times: true,
            ..Default::default()
        }
    }

    #[test]
    fn sync_copies_files() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("hello.txt"), b"hello world").unwrap();
        fs::create_dir(src.path().join("subdir")).unwrap();
        fs::write(src.path().join("subdir/nested.txt"), b"nested").unwrap();

        sync_local(src.path(), dst.path(), &default_opts()).unwrap();

        assert_eq!(
            fs::read(dst.path().join("hello.txt")).unwrap(),
            b"hello world"
        );
        assert_eq!(
            fs::read(dst.path().join("subdir/nested.txt")).unwrap(),
            b"nested"
        );
    }

    #[test]
    fn sync_skips_unchanged_files() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("file.txt"), b"content").unwrap();

        let s1 = sync_local(src.path(), dst.path(), &default_opts()).unwrap();
        assert_eq!(s1.files_xferred, 1);

        let s2 = sync_local(src.path(), dst.path(), &default_opts()).unwrap();
        assert_eq!(s2.files_xferred, 0);
        assert_eq!(s2.files_skipped, 1);
    }

    #[test]
    fn sync_transfers_changed_file() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("file.txt"), b"version 1").unwrap();
        sync_local(src.path(), dst.path(), &default_opts()).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(20));
        fs::write(src.path().join("file.txt"), b"version 2 with more content").unwrap();

        sync_local(src.path(), dst.path(), &default_opts()).unwrap();
        assert_eq!(
            fs::read(dst.path().join("file.txt")).unwrap(),
            b"version 2 with more content"
        );
    }

    #[test]
    fn sync_empty_src() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let stats = sync_local(src.path(), dst.path(), &default_opts()).unwrap();
        assert_eq!(stats.files_total, 0);
    }

    #[test]
    fn sync_delete_removes_stale_dst_files() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("keep.txt"), b"keep").unwrap();
        fs::write(dst.path().join("extra.txt"), b"stale").unwrap();

        let opts = SyncOpts {
            delete: true,
            preserve_times: true,
            ..Default::default()
        };
        sync_local(src.path(), dst.path(), &opts).unwrap();

        assert!(dst.path().join("keep.txt").exists());
        assert!(!dst.path().join("extra.txt").exists());
    }

    #[test]
    fn sync_filter_excludes_logs() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("main.rs"), b"fn main() {}").unwrap();
        fs::write(src.path().join("debug.log"), b"log output").unwrap();

        let mut opts = default_opts();
        opts.filter.add_exclude("*.log");
        sync_local(src.path(), dst.path(), &opts).unwrap();

        assert!(dst.path().join("main.rs").exists());
        assert!(!dst.path().join("debug.log").exists());
    }

    #[test]
    fn sync_max_size_skips_large_files() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("small.txt"), b"hi").unwrap();
        fs::write(src.path().join("large.bin"), vec![0u8; 1024]).unwrap();

        let opts = SyncOpts {
            max_size: Some(100),
            preserve_times: true,
            ..Default::default()
        };
        sync_local(src.path(), dst.path(), &opts).unwrap();

        assert!(dst.path().join("small.txt").exists());
        assert!(!dst.path().join("large.bin").exists());
    }

    #[test]
    fn sync_min_size_skips_small_files() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("small.txt"), b"hi").unwrap();
        fs::write(src.path().join("large.bin"), vec![0u8; 1024]).unwrap();

        let opts = SyncOpts {
            min_size: Some(100),
            preserve_times: true,
            ..Default::default()
        };
        sync_local(src.path(), dst.path(), &opts).unwrap();

        assert!(!dst.path().join("small.txt").exists());
        assert!(dst.path().join("large.bin").exists());
    }

    #[test]
    fn sync_update_preserves_newer_destination() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let src_file = src.path().join("file.txt");
        let dst_file = dst.path().join("file.txt");
        fs::write(&src_file, b"old source").unwrap();
        fs::write(&dst_file, b"new destination").unwrap();

        let old = filetime::FileTime::from_unix_time(1_000, 0);
        let new = filetime::FileTime::from_unix_time(2_000, 0);
        filetime::set_file_mtime(&src_file, old).unwrap();
        filetime::set_file_mtime(&dst_file, new).unwrap();

        let opts = SyncOpts {
            update: true,
            preserve_times: true,
            ..Default::default()
        };
        let stats = sync_local(src.path(), dst.path(), &opts).unwrap();

        assert_eq!(stats.files_xferred, 0);
        assert_eq!(fs::read(dst_file).unwrap(), b"new destination");
    }

    #[test]
    fn sync_whole_file_replaces_existing_content() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("file.txt"), b"replacement content").unwrap();
        fs::write(dst.path().join("file.txt"), b"old").unwrap();

        let opts = SyncOpts {
            whole_file: true,
            preserve_times: true,
            ..Default::default()
        };
        let stats = sync_local(src.path(), dst.path(), &opts).unwrap();

        assert_eq!(stats.files_xferred, 1);
        assert_eq!(
            fs::read(dst.path().join("file.txt")).unwrap(),
            b"replacement content"
        );
    }

    #[test]
    fn sync_broken_symlink_does_not_crash() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("real.txt"), b"real").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("nonexistent_target", src.path().join("broken_link")).unwrap();

        sync_local(src.path(), dst.path(), &default_opts()).unwrap();
        assert!(dst.path().join("real.txt").exists());
    }

    #[test]
    fn sync_dry_run_copies_nothing() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("file.txt"), b"data").unwrap();
        fs::create_dir(src.path().join("dir")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("file.txt", src.path().join("link")).unwrap();
        fs::write(dst.path().join("stale.txt"), b"stale").unwrap();

        let opts = SyncOpts {
            dry_run: true,
            delete: true,
            ..Default::default()
        };
        sync_local(src.path(), dst.path(), &opts).unwrap();

        assert!(!dst.path().join("file.txt").exists());
        assert!(!dst.path().join("dir").exists());
        assert!(dst.path().join("stale.txt").exists());
        #[cfg(unix)]
        assert!(!dst.path().join("link").exists());
    }

    #[test]
    fn sync_ignore_existing_skips_present_dst() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("file.txt"), b"new content").unwrap();
        fs::write(dst.path().join("file.txt"), b"old content").unwrap();

        let opts = SyncOpts {
            ignore_existing: true,
            ..Default::default()
        };
        sync_local(src.path(), dst.path(), &opts).unwrap();
        assert_eq!(
            fs::read(dst.path().join("file.txt")).unwrap(),
            b"old content"
        );
    }

    #[test]
    fn protocol_max_sum_blocks_rejected() {
        use crate::protocol::read_sum_blocks;
        use std::io::Cursor;
        let count = crate::protocol::MAX_SUM_BLOCKS + 1;
        let buf: Vec<u8> = vec![];
        let mut cur = Cursor::new(buf);
        assert!(read_sum_blocks(&mut cur, count).is_err());
    }

    #[test]
    fn protocol_write_buf_rejects_long_path() {
        use crate::flist::{EntryKind, FileEntry};
        use crate::protocol::write_entry;
        use std::path::PathBuf;
        let entry = FileEntry {
            path: PathBuf::from("a".repeat(65536)),
            size: 0,
            mtime: 0,
            mode: 0o644,
            uid: 0,
            gid: 0,
            kind: EntryKind::Regular,
        };
        let mut buf = vec![];
        assert!(write_entry(&mut buf, &entry).is_err());
    }
}
