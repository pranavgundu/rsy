use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::SystemTime;

use anyhow::Result;
use memmap2::Mmap;
use rayon::prelude::*;

use crate::checksum::{block_size as auto_block_size, file_hash};

/// Lexically confine `rel` within `base` — reject `..` traversal and absolute paths.
fn confine_path(base: &Path, rel: &Path) -> Result<PathBuf> {
    use std::path::Component;

    if rel.is_absolute() {
        anyhow::bail!("path traversal: absolute path rejected: {}", rel.display());
    }

    let mut clean_rel = PathBuf::new();
    for c in rel.components() {
        match c {
            Component::CurDir => {}
            Component::Normal(seg) => clean_rel.push(seg),
            Component::ParentDir => {
                anyhow::bail!("path traversal: '..' is not allowed: {}", rel.display())
            }
            Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("path traversal: absolute path rejected: {}", rel.display())
            }
        }
    }

    // Do not allow writes through symlinked ancestor directories.
    let mut cur = base.to_path_buf();
    let mut comps = clean_rel.components().peekable();
    while let Some(c) = comps.next() {
        if let Component::Normal(seg) = c {
            cur.push(seg);
            if comps.peek().is_none() {
                break;
            }
            match fs::symlink_metadata(&cur) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    anyhow::bail!(
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

    Ok(base.join(clean_rel))
}
use crate::delta::{BlockSum, Token, basis_sums, diff, patch, token_stats};
use crate::filter::FilterList;
use crate::flist::{EntryKind, FileEntry, FileList, device_id};
use crate::protocol::{
    ReceiverReq, TAG_FLIST_END, TAG_FLIST_ENTRY, TAG_FLIST_START, read_entry, read_file_hash,
    read_receiver_req, read_sum_blocks, read_token, write_done, write_entry, write_file_hash,
    write_flist_end, write_flist_start, write_sum_block, write_sum_end, write_sum_head,
    write_token, write_token_end,
};
use crate::transport::Pipe;

// ─── options ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct SyncOpts {
    // transfer control
    pub delete: bool,
    pub update: bool,          // skip if dst is newer
    pub ignore_existing: bool, // skip if dst exists
    pub whole_file: bool,
    pub checksum: bool,
    pub inplace: bool, // write in-place, no tmp rename
    pub one_file_system: bool,
    // metadata — preserve_times is kept for CLI compat but mtime is always synced
    #[allow(dead_code)]
    pub preserve_times: bool,
    pub preserve_perms: bool,
    pub preserve_owner: bool,
    pub preserve_group: bool,
    // filtering
    pub filter: FilterList,
    pub max_size: Option<u64>,
    pub min_size: Option<u64>,
    // algorithm
    pub block_size: Option<usize>,
    pub compress: bool,
    // output
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

    let (src_res, dst_res) = rayon::join(
        || FileList::build(src_root, root_dev),
        || FileList::build(dst_root, None).or_else(|_| Ok::<_, anyhow::Error>(FileList::empty())),
    );
    let src_list = src_res?;
    let dst_list = dst_res?;

    // Apply global filters + size limits to source list
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

    // Dirs first — parallel creation; sorted order ensures parents precede children
    src_filtered
        .iter()
        .filter(|e| e.is_dir())
        .try_for_each(|e| {
            let dst = confine_path(dst_root, &e.path)?;
            fs::create_dir_all(dst)?;
            Ok::<_, anyhow::Error>(())
        })?;

    // Symlinks
    for e in src_filtered.iter() {
        if let EntryKind::Symlink { target } = &e.kind {
            let dst = confine_path(dst_root, &e.path)?;
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
        let dst = match confine_path(dst_root, &entry.path) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("warning: {}: {e}", entry.path.display());
                pstats2.errored.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
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

    // --delete: remove dst entries absent from src
    if opts.delete {
        let src_paths: std::collections::HashSet<&PathBuf> =
            src_list.0.iter().map(|e| &e.path).collect();
        for e in &dst_list.0 {
            if !src_paths.contains(&e.path) {
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
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

enum SrcBytes {
    Mmap(Mmap),
    Heap(Vec<u8>),
}

fn create_unique_tmp_file(dst: &Path) -> Result<(PathBuf, fs::File)> {
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
    anyhow::bail!("failed to create unique temp file for {}", dst.display())
}

fn write_file_atomically(dst: &Path, data: &[u8]) -> Result<()> {
    let (tmp, mut f) = create_unique_tmp_file(dst)?;
    let write_res: Result<()> = (|| {
        f.write_all(data)?;
        f.flush()?;
        Ok(())
    })();
    drop(f);
    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp, dst) {
        let _ = fs::remove_file(&tmp);
        return Err(e.into());
    }
    Ok(())
}

fn copy_file_atomically(src: &Path, dst: &Path) -> Result<()> {
    let (tmp, mut out) = create_unique_tmp_file(dst)?;
    let copy_res: Result<()> = (|| {
        let mut input = fs::File::open(src)?;
        std::io::copy(&mut input, &mut out)?;
        out.flush()?;
        Ok(())
    })();
    drop(out);
    if let Err(e) = copy_res {
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
    // Single stat — reused for mtime check AND basis_exists determination
    let mut dst_meta = dst.symlink_metadata().ok();
    if dst_meta
        .as_ref()
        .is_some_and(|m| m.file_type().is_symlink())
    {
        fs::remove_file(dst)?;
        dst_meta = None;
    }
    let basis_exists = dst_meta.is_some();

    // --ignore-existing
    if opts.ignore_existing && basis_exists {
        return Ok((false, 0, entry.size));
    }

    // mtime/size quick skip (default, unless --checksum or --dry-run)
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

    // Dirs are pre-created in sync_local; only needed for network receiver path
    // where we don't have the pre-create phase.
    // Fast path: new file — copy directly to dst (no tmp needed, nothing to protect)
    if !basis_exists && !opts.checksum {
        copy_file_atomically(src, dst)?;
        set_metadata(dst, entry, opts)?;
        if opts.verbose {
            eprintln!("{}", entry.path.display());
        }
        return Ok((true, entry.size, 0));
    }

    // Very large files: skip delta to avoid multi-GB mmap page-fault hangs
    const DELTA_SIZE_LIMIT: u64 = 512 * 1024 * 1024; // 512 MB
    if !opts.checksum && entry.size > DELTA_SIZE_LIMIT {
        copy_file_atomically(src, dst)?;
        set_metadata(dst, entry, opts)?;
        if opts.verbose {
            eprintln!("{}", entry.path.display());
        }
        return Ok((true, entry.size, 0));
    }

    // Delta path
    let src_bytes = if entry.size >= MMAP_THRESHOLD {
        let f = fs::File::open(src)?;
        SrcBytes::Mmap(unsafe { Mmap::map(&f)? })
    } else {
        SrcBytes::Heap(fs::read(src)?)
    };
    let src_data: &[u8] = match &src_bytes {
        SrcBytes::Mmap(m) => m,
        SrcBytes::Heap(v) => v,
    };

    // Mmap basis (dst) for large files to avoid heap allocation
    enum BasisBytes {
        Mmap(Mmap),
        Heap(Vec<u8>),
        Empty,
    }
    let basis_bytes = if basis_exists {
        let f = fs::File::open(dst)?;
        // Use entry.size (from flist, already known) to decide — avoids TOCTOU
        // between fstat and mmap on a file another process may truncate.
        if entry.size >= MMAP_THRESHOLD {
            BasisBytes::Mmap(unsafe { Mmap::map(&f)? })
        } else {
            BasisBytes::Heap(fs::read(dst)?)
        }
    } else {
        BasisBytes::Empty
    };
    let basis: &[u8] = match &basis_bytes {
        BasisBytes::Mmap(m) => m,
        BasisBytes::Heap(v) => v,
        BasisBytes::Empty => &[],
    };

    if opts.checksum && !basis.is_empty() && file_hash(src_data) == file_hash(basis) {
        return Ok((false, 0, src_data.len() as u64));
    }

    let blen = opts
        .block_size
        .unwrap_or_else(|| auto_block_size(entry.size))
        .clamp(512, 1_048_576);

    let (new_data, lit, matched) = if opts.whole_file || basis.is_empty() {
        // No delta needed — write src directly
        copy_file_atomically(src, dst)?;
        set_metadata(dst, entry, opts)?;
        if opts.verbose {
            eprintln!("{}", entry.path.display());
        }
        return Ok((true, src_data.len() as u64, 0));
    } else {
        let sums = basis_sums(basis, blen);
        let tokens = diff(src_data, &sums, blen);
        let (l, m) = token_stats(&tokens);
        (patch(basis, &tokens), l, m)
    };

    anyhow::ensure!(
        file_hash(&new_data) == file_hash(src_data),
        "checksum mismatch after patch: {}",
        src.display()
    );

    if opts.verbose {
        eprintln!(
            "{}: {}B literal, {}B matched",
            entry.path.display(),
            lit,
            matched
        );
    }

    if opts.inplace {
        fs::write(dst, &new_data)?;
    } else {
        write_file_atomically(dst, &new_data)?;
    }

    set_metadata(dst, entry, opts)?;
    Ok((true, lit as u64, matched as u64))
}

fn set_metadata(path: &Path, entry: &FileEntry, opts: &SyncOpts) -> Result<()> {
    // Always sync mtime so subsequent incremental runs can skip unchanged files.
    // preserve_times (-t) controls whether we honor the original timestamp;
    // without it we still need dst mtime == src mtime for skip detection.
    set_mtime(path, entry.mtime)?;
    if opts.preserve_perms {
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
                let entry = src_list.0.get(idx).ok_or_else(|| {
                    anyhow::anyhow!("file_idx {idx} out of range ({})", src_list.0.len())
                })?;
                let blen = (sh.block_len as usize).clamp(512, 1_048_576);

                let raw = read_sum_blocks(&mut *pipe.rx, sh.count)?;
                let sums: Vec<BlockSum> = raw
                    .into_iter()
                    .enumerate()
                    .map(|(i, (r, s))| BlockSum {
                        rolling: r,
                        strong: s,
                        offset: (i * blen) as u64,
                    })
                    .collect();

                let src_data = fs::read(src_root.join(&entry.path))?;
                let src_hash = file_hash(&src_data);
                let tokens = if sh.count == 0 || opts.whole_file {
                    vec![Token::Data(src_data)]
                } else {
                    diff(&src_data, &sums, blen)
                };

                let (lit, mat) = token_stats(&tokens);
                pstats.add(true, lit as u64, mat as u64);

                for t in &tokens {
                    write_token(&mut w, t)?;
                }
                write_token_end(&mut w)?;
                write_file_hash(&mut w, &src_hash)?;
                w.flush()?;
            }
        }
    }

    Ok(pstats.snapshot(total, src_list.0.iter().map(|e| e.size).sum()))
}

// ─── network receiver ────────────────────────────────────────────────────────

pub fn run_receiver(dst_root: &Path, pipe: &mut Pipe, opts: &SyncOpts) -> Result<Stats> {
    use byteorder::ReadBytesExt;

    let tag = pipe.rx.read_u8()?;
    anyhow::ensure!(tag == TAG_FLIST_START, "expected flist start, got {tag}");

    let mut file_list: Vec<FileEntry> = Vec::new();
    loop {
        let t = pipe.rx.read_u8()?;
        match t {
            TAG_FLIST_ENTRY => file_list.push(read_entry(&mut *pipe.rx)?),
            TAG_FLIST_END => break,
            other => anyhow::bail!("unexpected tag {other}"),
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
        let dst_path = confine_path(dst_root, &entry.path)?;

        match &entry.kind {
            EntryKind::Dir => {
                fs::create_dir_all(&dst_path)?;
                continue;
            }
            EntryKind::Symlink { target } => {
                // Reject absolute symlink targets and targets that escape via ..
                #[cfg(unix)]
                {
                    use std::path::Component;
                    let is_absolute = target.is_absolute();
                    let escapes = target.components().fold(0i32, |depth, c| match c {
                        Component::ParentDir => depth - 1,
                        Component::Normal(_) => depth + 1,
                        _ => depth,
                    }) < 0;
                    if is_absolute || escapes {
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

        let basis = if dst_path.exists() {
            fs::read(&dst_path)?
        } else {
            Vec::new()
        };
        let blen = opts
            .block_size
            .unwrap_or_else(|| auto_block_size(entry.size))
            .clamp(512, 1_048_576);
        let sums = if opts.whole_file {
            Vec::new()
        } else {
            basis_sums(&basis, blen)
        };

        write_sum_head(&mut w, idx as u32, blen as u32, sums.len() as u32)?;
        for bs in &sums {
            write_sum_block(&mut w, bs.rolling, &bs.strong)?;
        }
        write_sum_end(&mut w)?;
        w.flush()?;

        let mut lit: usize = 0;
        let mut mat: usize = 0;
        let max_reconstructed: usize = 2 * 1024 * 1024 * 1024; // 2 GiB hard cap, not sender-controlled
        let capacity = usize::try_from(entry.size)
            .unwrap_or(0)
            .min(max_reconstructed);
        let mut new_data = Vec::with_capacity(capacity);
        loop {
            match read_token(&mut *pipe.rx)? {
                None => break,
                Some(Token::Copy { offset, len }) => {
                    mat = mat.saturating_add(len as usize);
                    let s = offset as usize;
                    let e = s.saturating_add(len as usize).min(basis.len());
                    if s < basis.len() {
                        new_data.extend_from_slice(&basis[s..e]);
                    }
                }
                Some(Token::Data(data)) => {
                    lit = lit.saturating_add(data.len());
                    new_data.extend_from_slice(&data);
                }
            }
            anyhow::ensure!(
                new_data.len() <= max_reconstructed,
                "reconstructed data exceeds limit for {}",
                entry.path.display()
            );
        }
        let expected = read_file_hash(&mut *pipe.rx)?;

        anyhow::ensure!(
            file_hash(&new_data) == expected,
            "hash mismatch: {}",
            entry.path.display()
        );

        pstats.add(true, lit as u64, mat as u64);

        if let Some(tx) = &opts.progress_tx {
            let _ = tx.send(ProgressEvent::File(FileRecord {
                path: entry.path.display().to_string(),
                size: entry.size,
                literal: lit as u64,
                matched: mat as u64,
                skipped: false,
            }));
        } else if opts.verbose || opts.progress {
            eprintln!("{}", entry.path.display());
        }

        if let Some(p) = dst_path.parent() {
            fs::create_dir_all(p)?;
        }
        if opts.inplace {
            fs::write(&dst_path, &new_data)?;
        } else {
            write_file_atomically(&dst_path, &new_data)?;
        }
        set_metadata(&dst_path, entry, opts)?;
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
        if n >= 1 << 30 {
            format!("{:.2} GB", n as f64 / (1 << 30) as f64)
        } else if n >= 1 << 20 {
            format!("{:.2} MB", n as f64 / (1 << 20) as f64)
        } else if n >= 1 << 10 {
            format!("{:.2} KB", n as f64 / (1 << 10) as f64)
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

    // ── confine_path ────────────────────────────────────────────────────────────

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

    // ── sync_local integration ──────────────────────────────────────────────────

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

        let opts = SyncOpts {
            dry_run: true,
            ..Default::default()
        };
        sync_local(src.path(), dst.path(), &opts).unwrap();

        assert!(!dst.path().join("file.txt").exists());
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

        // dst file should be unchanged
        assert_eq!(
            fs::read(dst.path().join("file.txt")).unwrap(),
            b"old content"
        );
    }

    // ── protocol bounds ─────────────────────────────────────────────────────────

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
