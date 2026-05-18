use crate::transport::daemon::DEFAULT_PORT;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "rsy",
    about = "Fast local and remote file sync",
    long_about = "\
rsy is a fast rsync-style file synchronizer for local copies, SSH copies, and
daemon-style rsync:// targets.

It builds a file list, skips unchanged files by size+mtime by default, uses a
rolling-checksum delta algorithm when updating existing files, and uses fast
whole-file copy paths for cold copies. On supported filesystems this includes
copy-on-write clones: clonefile() on macOS/APFS and FICLONE reflinks on Linux
Btrfs/XFS-reflink.

Path forms:
  rsy SRC/ DST/                  local copy
  rsy SRC/ host:/path/           push over SSH
  rsy host:/path/ DST/           pull over SSH
  rsy SRC/ rsync://host/path/    daemon-style push
  rsy SRC/ host::module/path     daemon-style push

Trailing slash behavior follows rsync convention: SRC/ means copy the contents
of SRC into DST. Without a trailing slash, the source directory itself is copied
under DST.",
    after_help = "\
Examples:
  rsy -a ./site/ /backup/site/
  rsy --delete --exclude target --exclude .git ./project/ /mnt/project/
  rsy -az ./media/ user@example.com:/srv/media/
  rsy -a user@example.com:/srv/data/ ./data/
  rsy --checksum --stats ./src/ ./dst/

Notes:
  - Unchanged files are skipped by size and modification time unless --checksum
    is used.
  - Modification times are synced so later incremental runs can skip quickly.
  - --delete only removes destination entries missing from the source list.
  - --compress is accepted for CLI compatibility but is not implemented yet.
  - Fast clone/reflink copies only work within a supporting filesystem; rsy
    automatically falls back to a normal copy when unavailable."
)]
pub struct Cli {
    /// Source path.
    ///
    /// Accepts local paths, SSH-style paths such as `user@host:/path`, and
    /// daemon-style paths through the destination form. A trailing slash copies
    /// the contents of a directory; no trailing slash copies the directory
    /// itself into the destination.
    #[arg(value_name = "SRC")]
    pub src: String,

    /// Destination path.
    ///
    /// Accepts local paths, SSH-style paths such as `host:/path`, rsync daemon
    /// URLs such as `rsync://host/path`, or module syntax such as
    /// `host::module/path`.
    #[arg(required_unless_present = "server", value_name = "DST")]
    pub dst: Option<String>,

    // ── transfer control ──────────────────────────────────────────────────
    /// Archive mode.
    ///
    /// Enables the preservation flags normally expected for archive copies:
    /// modification times, permissions, owner, group, and symlinks. Owner and
    /// group preservation require suitable privileges on Unix.
    #[arg(help_heading = "Transfer Control")]
    #[arg(short = 'a', long)]
    pub archive: bool,

    /// Delete destination entries that are not present in the source.
    ///
    /// Deletions are based on the filtered source file list. If an entry is
    /// excluded from the source list, it is not considered present for delete
    /// purposes.
    #[arg(help_heading = "Transfer Control")]
    #[arg(long)]
    pub delete: bool,

    /// Skip a file if the destination copy is newer than the source.
    #[arg(help_heading = "Transfer Control")]
    #[arg(short = 'u', long)]
    pub update: bool,

    /// Skip files that already exist at the destination.
    ///
    /// Existing files are left untouched even if the source differs.
    #[arg(help_heading = "Transfer Control")]
    #[arg(long)]
    pub ignore_existing: bool,

    /// Transfer whole files instead of using the delta algorithm.
    ///
    /// This can be faster for local same-filesystem copies, cold copies, or
    /// files where most bytes changed. Cold copies already use whole-file fast
    /// paths automatically.
    #[arg(help_heading = "Transfer Control")]
    #[arg(short = 'W', long)]
    pub whole_file: bool,

    /// Update destination files in place.
    ///
    /// By default rsy writes a temporary file and renames it into place. In-place
    /// updates avoid the temporary replacement but can leave a partial file if
    /// the process is interrupted.
    #[arg(help_heading = "Transfer Control")]
    #[arg(long)]
    pub inplace: bool,

    /// Do not cross filesystem boundaries while walking the source tree.
    #[arg(help_heading = "Transfer Control")]
    #[arg(short = 'x', long)]
    pub one_file_system: bool,

    // ── what to sync ──────────────────────────────────────────────────────
    /// Exclude files matching PATTERN.
    ///
    /// May be repeated. Patterns are matched against source-relative paths.
    /// Directory-only patterns can be written with a trailing slash.
    #[arg(help_heading = "Filtering")]
    #[arg(long = "exclude", value_name = "PATTERN")]
    pub excludes: Vec<String>,

    /// Include files matching PATTERN.
    ///
    /// May be repeated. Include rules can override excludes depending on rule
    /// order.
    #[arg(help_heading = "Filtering")]
    #[arg(long = "include", value_name = "PATTERN")]
    pub includes: Vec<String>,

    /// Read exclude patterns from FILE.
    ///
    /// One pattern per line. Empty lines and comments are ignored by the filter
    /// loader.
    #[arg(help_heading = "Filtering")]
    #[arg(long, value_name = "FILE")]
    pub exclude_from: Option<String>,

    /// Do not transfer files larger than SIZE.
    ///
    /// SIZE accepts plain bytes or K/M/G suffixes.
    #[arg(help_heading = "Filtering")]
    #[arg(long, value_name = "SIZE")]
    pub max_size: Option<String>,

    /// Do not transfer files smaller than SIZE.
    ///
    /// SIZE accepts plain bytes or K/M/G suffixes.
    #[arg(help_heading = "Filtering")]
    #[arg(long, value_name = "SIZE")]
    pub min_size: Option<String>,

    // ── metadata preservation ─────────────────────────────────────────────
    /// Preserve modification times.
    ///
    /// rsy also sets mtimes after transfers so future incremental runs can skip
    /// unchanged files quickly.
    #[arg(help_heading = "Metadata")]
    #[arg(short = 't', long)]
    pub times: bool,

    /// Preserve file permissions.
    #[arg(help_heading = "Metadata")]
    #[arg(long)]
    pub perms: bool,

    /// Preserve file owner.
    ///
    /// Usually requires super-user privileges on Unix.
    #[arg(help_heading = "Metadata")]
    #[arg(short = 'o', long)]
    pub owner: bool,

    /// Preserve file group.
    #[arg(help_heading = "Metadata")]
    #[arg(short = 'g', long)]
    pub group: bool,

    // ── algorithm tuning ──────────────────────────────────────────────────
    /// Compare file contents by checksum instead of size+mtime.
    ///
    /// This is more reliable when timestamps are not trustworthy, but it must
    /// read both source and destination contents.
    #[arg(help_heading = "Algorithm")]
    #[arg(short = 'c', long)]
    pub checksum: bool,

    /// Force the rolling-checksum block size in bytes.
    ///
    /// The default is chosen from the source file size. Smaller blocks can find
    /// finer-grained matches but cost more CPU and protocol overhead.
    #[arg(help_heading = "Algorithm")]
    #[arg(short = 'B', long, value_name = "SIZE")]
    pub block_size: Option<usize>,

    /// Request compression for network transfers.
    ///
    /// Currently accepted for CLI compatibility but not implemented.
    #[arg(help_heading = "Algorithm")]
    #[arg(short = 'z', long)]
    pub compress: bool,

    // ── output ────────────────────────────────────────────────────────────
    /// Print transferred file names and a final summary.
    #[arg(help_heading = "Output")]
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Show per-file transfer progress.
    #[arg(help_heading = "Output")]
    #[arg(long)]
    pub progress: bool,

    /// Print transfer statistics at the end.
    #[arg(help_heading = "Output")]
    #[arg(long)]
    pub stats: bool,

    /// Disable the terminal UI and use plain stderr output.
    #[arg(help_heading = "Output")]
    #[arg(long)]
    pub no_tui: bool,

    /// Print each transferred file with a timestamp.
    ///
    /// This implies non-TUI logging behavior.
    #[arg(help_heading = "Output")]
    #[arg(long)]
    pub log: bool,

    /// Show what would be transferred without writing destination changes.
    #[arg(help_heading = "Output")]
    #[arg(short = 'n', long)]
    pub dry_run: bool,

    /// Number of worker threads.
    ///
    /// Defaults to 2x logical CPUs, with a minimum of 4, because this workload
    /// is often I/O-bound.
    #[arg(help_heading = "Performance")]
    #[arg(short = 'j', long)]
    pub jobs: Option<usize>,

    // ── SSH options ───────────────────────────────────────────────────────
    /// SSH port for remote connections.
    #[arg(help_heading = "SSH")]
    #[arg(short = 'p', long, value_name = "PORT")]
    pub port: Option<u16>,

    /// SSH identity file (private key) for remote connections.
    #[arg(help_heading = "SSH")]
    #[arg(short = 'i', long, value_name = "FILE")]
    pub identity: Option<String>,

    // ── server flags (hidden, invoked via SSH) ─────────────────────────────
    #[arg(long, hide = true)]
    pub server: bool,
    #[arg(long, hide = true)]
    pub sender: bool,
}

#[derive(Debug)]
pub enum Mode {
    Local {
        src: String,
        dst: String,
    },
    SshPush {
        host: String,
        src: String,
        remote_dst: String,
    },
    SshPull {
        host: String,
        remote_src: String,
        dst: String,
    },
    Server {
        sender_side: bool,
        path: String,
    },
    Daemon {
        host: String,
        port: u16,
        src: String,
        dst: String,
    },
}

pub fn parse_mode(cli: &Cli) -> Mode {
    if cli.server {
        return Mode::Server {
            sender_side: cli.sender,
            path: cli.src.clone(),
        };
    }
    let dst = cli.dst.as_deref().unwrap_or("");
    if let Some((host, port, path)) = split_daemon(dst) {
        return Mode::Daemon {
            host,
            port,
            src: cli.src.clone(),
            dst: path,
        };
    }
    if let Some((host, rpath)) = split_remote(dst) {
        return Mode::SshPush {
            host,
            src: cli.src.clone(),
            remote_dst: rpath,
        };
    }
    if let Some((host, rpath)) = split_remote(&cli.src) {
        return Mode::SshPull {
            host,
            remote_src: rpath,
            dst: dst.to_string(),
        };
    }
    Mode::Local {
        src: cli.src.clone(),
        dst: dst.to_string(),
    }
}

fn split_remote(s: &str) -> Option<(String, String)> {
    if let Some(rest) = s.strip_prefix("rsync://") {
        let slash = rest.find('/')?;
        return Some((rest[..slash].to_string(), rest[slash..].to_string()));
    }
    let colon = s.find(':')?;
    if colon == 0 {
        return None;
    }
    Some((s[..colon].to_string(), s[colon + 1..].to_string()))
}

fn split_daemon(s: &str) -> Option<(String, u16, String)> {
    if let Some(rest) = s.strip_prefix("rsync://") {
        let (host_port, path) = rest.split_once('/')?;
        let (host, port) = if let Some((h, p)) = host_port.rsplit_once(':') {
            (h, p.parse().ok()?)
        } else {
            (host_port, DEFAULT_PORT)
        };
        if host.is_empty() {
            return None;
        }
        return Some((host.to_string(), port, format!("/{path}")));
    }

    if let Some((host, path)) = s.split_once("::") {
        if host.is_empty() {
            return None;
        }
        return Some((host.to_string(), DEFAULT_PORT, path.to_string()));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> Cli {
        Cli::parse_from(args)
    }

    #[test]
    fn parse_mode_detects_local_copy() {
        let cli = cli(&["rsy", "src", "dst"]);
        assert!(matches!(
            parse_mode(&cli),
            Mode::Local { src, dst } if src == "src" && dst == "dst"
        ));
    }

    #[test]
    fn parse_mode_detects_ssh_push() {
        let cli = cli(&["rsy", "src", "example.com:/srv/data"]);
        assert!(matches!(
            parse_mode(&cli),
            Mode::SshPush { host, src, remote_dst }
                if host == "example.com" && src == "src" && remote_dst == "/srv/data"
        ));
    }

    #[test]
    fn parse_mode_detects_ssh_pull() {
        let cli = cli(&["rsy", "user@example.com:/srv/data", "dst"]);
        assert!(matches!(
            parse_mode(&cli),
            Mode::SshPull { host, remote_src, dst }
                if host == "user@example.com" && remote_src == "/srv/data" && dst == "dst"
        ));
    }

    #[test]
    fn parse_mode_detects_server_sender() {
        let cli = cli(&["rsy", "--server", "--sender", "/tmp/src"]);
        assert!(matches!(
            parse_mode(&cli),
            Mode::Server { sender_side: true, path } if path == "/tmp/src"
        ));
    }

    #[test]
    fn parse_mode_detects_rsync_url_daemon() {
        let cli = cli(&["rsy", "src", "rsync://example.com:10873/module/path"]);
        assert!(matches!(
            parse_mode(&cli),
            Mode::Daemon { host, port, src, dst }
                if host == "example.com" && port == 10873 && src == "src" && dst == "/module/path"
        ));
    }

    #[test]
    fn parse_mode_detects_module_daemon() {
        let cli = cli(&["rsy", "src", "example.com::module/path"]);
        assert!(matches!(
            parse_mode(&cli),
            Mode::Daemon { host, port, src, dst }
                if host == "example.com" && port == DEFAULT_PORT && src == "src" && dst == "module/path"
        ));
    }

    #[test]
    fn split_remote_rejects_empty_host() {
        assert!(split_remote(":/tmp").is_none());
    }

    #[test]
    fn split_daemon_rejects_bad_port() {
        assert!(split_daemon("rsync://example.com:notaport/module").is_none());
    }
}
