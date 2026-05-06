use crate::transport::daemon::DEFAULT_PORT;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "rsy", about = "Fast parallel file synchronizer")]
pub struct Cli {
    /// Source  (local or user@host:path)
    pub src: String,
    /// Destination  (local or user@host:path)
    #[arg(required_unless_present = "server")]
    pub dst: Option<String>,

    // ── transfer control ──────────────────────────────────────────────────
    /// Archive: preserve times, perms, owner, group, symlinks (-t -p -o -g -l)
    #[arg(short = 'a', long)]
    pub archive: bool,

    /// Delete files in dst not present in src
    #[arg(long)]
    pub delete: bool,

    /// Skip if destination file is newer than source
    #[arg(short = 'u', long)]
    pub update: bool,

    /// Skip files that already exist at destination
    #[arg(long)]
    pub ignore_existing: bool,

    /// Skip delta, always transfer whole files
    #[arg(short = 'W', long)]
    pub whole_file: bool,

    /// Update destination file in-place (no tmp+rename)
    #[arg(long)]
    pub inplace: bool,

    /// Don't cross filesystem boundaries
    #[arg(short = 'x', long)]
    pub one_file_system: bool,

    // ── what to sync ──────────────────────────────────────────────────────
    /// Exclude files matching PATTERN (repeatable)
    #[arg(long = "exclude", value_name = "PATTERN")]
    pub excludes: Vec<String>,

    /// Include files matching PATTERN (overrides exclude, repeatable)
    #[arg(long = "include", value_name = "PATTERN")]
    pub includes: Vec<String>,

    /// Read exclude patterns from FILE
    #[arg(long, value_name = "FILE")]
    pub exclude_from: Option<String>,

    /// Don't transfer files larger than SIZE (K/M/G suffix OK)
    #[arg(long, value_name = "SIZE")]
    pub max_size: Option<String>,

    /// Don't transfer files smaller than SIZE
    #[arg(long, value_name = "SIZE")]
    pub min_size: Option<String>,

    // ── metadata preservation ─────────────────────────────────────────────
    /// Preserve modification times
    #[arg(short = 't', long)]
    pub times: bool,

    /// Preserve permissions
    #[arg(short = 'p', long)]
    pub perms: bool,

    /// Preserve owner (Unix, super-user only)
    #[arg(short = 'o', long)]
    pub owner: bool,

    /// Preserve group
    #[arg(short = 'g', long)]
    pub group: bool,

    // ── algorithm tuning ──────────────────────────────────────────────────
    /// Always compare by checksum (not mtime+size)
    #[arg(short = 'c', long)]
    pub checksum: bool,

    /// Force checksum block size in bytes
    #[arg(short = 'B', long, value_name = "SIZE")]
    pub block_size: Option<usize>,

    /// Compress data during transfer (network mode)
    #[arg(short = 'z', long)]
    pub compress: bool,

    // ── output ────────────────────────────────────────────────────────────
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Show per-file transfer progress
    #[arg(long)]
    pub progress: bool,

    /// Print transfer statistics at the end
    #[arg(long)]
    pub stats: bool,

    /// Disable TUI; print plain progress to stderr
    #[arg(long)]
    pub no_tui: bool,

    /// Print each transferred file with timestamp (implies --no-tui)
    #[arg(long)]
    pub log: bool,

    #[arg(short = 'n', long)]
    pub dry_run: bool,

    /// Worker threads (default: logical CPUs)
    #[arg(short = 'j', long)]
    pub jobs: Option<usize>,

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
