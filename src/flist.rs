use anyhow::Result;
use jwalk::WalkDir;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    Regular,
    Dir,
    Symlink { target: PathBuf },
    Other,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub mtime: i64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub kind: EntryKind,
}

impl FileEntry {
    pub fn is_regular(&self) -> bool {
        matches!(self.kind, EntryKind::Regular)
    }
    pub fn is_dir(&self) -> bool {
        matches!(self.kind, EntryKind::Dir)
    }
}

pub struct FileList(pub Vec<FileEntry>);

impl FileList {
    /// Parallel directory walk via jwalk. Reuses lstat metadata captured during
    /// the readdir pass, avoiding a second `symlink_metadata` per entry.
    pub fn build(root: &Path, root_dev: Option<u64>) -> Result<Self> {
        let root = std::fs::canonicalize(root)?;

        let walker = WalkDir::new(&root)
            .follow_links(false)
            .skip_hidden(false)
            .sort(true);

        let mut entries: Vec<FileEntry> = Vec::new();
        for de in walker.into_iter().flatten() {
            // Skip the root itself
            let abs = de.path();
            let Ok(rel) = abs.strip_prefix(&root) else {
                continue;
            };
            if rel.as_os_str().is_empty() {
                continue;
            }
            // Skip macOS AppleDouble metadata files (._*)
            if de.file_name.to_str().is_some_and(|n| n.starts_with("._")) {
                continue;
            }

            // jwalk caches lstat metadata from readdir's d_type + a follow-up
            // stat that it would have done anyway. Reuse it.
            let Ok(meta) = de.metadata() else { continue };

            // --one-file-system: skip entries on a different device
            let dev = get_dev(&meta);
            if let Some(rd) = root_dev
                && dev != rd
                && !meta.is_symlink()
            {
                continue;
            }

            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let (uid, gid, mode) = platform_meta(&meta);

            let kind = if meta.file_type().is_symlink() {
                EntryKind::Symlink {
                    target: std::fs::read_link(&abs).unwrap_or_default(),
                }
            } else if meta.is_dir() {
                EntryKind::Dir
            } else if meta.is_file() {
                EntryKind::Regular
            } else {
                EntryKind::Other
            };

            entries.push(FileEntry {
                path: rel.to_path_buf(),
                size: meta.len(),
                mtime,
                mode,
                uid,
                gid,
                kind,
            });
        }

        Ok(Self(entries))
    }

    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Get the device ID of a path (for --one-file-system)
pub fn device_id(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        path.metadata().ok().map(|m| m.dev())
    }
    #[cfg(not(unix))]
    {
        None
    }
}

#[cfg(unix)]
fn get_dev(meta: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    meta.dev()
}
#[cfg(not(unix))]
fn get_dev(_: &std::fs::Metadata) -> u64 {
    0
}

#[cfg(unix)]
fn platform_meta(meta: &std::fs::Metadata) -> (u32, u32, u32) {
    use std::os::unix::fs::MetadataExt;
    (meta.uid(), meta.gid(), meta.mode())
}
#[cfg(not(unix))]
fn platform_meta(_: &std::fs::Metadata) -> (u32, u32, u32) {
    (0, 0, 0o644)
}
