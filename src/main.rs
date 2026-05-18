mod checksum;
mod cli;
mod delta;
mod filter;
mod flist;
mod pipeline;
mod protocol;
mod transport;
mod tui;

use anyhow::Result;
use clap::Parser;
use std::path::Path;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Mode};
use filter::{FilterList, RuleKind, parse_size};
use pipeline::{ProgressEvent, Stats, SyncOpts, print_stats};

// ─── output mode ─────────────────────────────────────────────────────────────

enum OutputMode {
    Tui,
    Plain,
    Log,
}

fn output_mode(cli: &Cli) -> OutputMode {
    use std::io::IsTerminal;
    if cli.log {
        return OutputMode::Log;
    }
    if cli.no_tui || cli.dry_run || !std::io::stderr().is_terminal() {
        return OutputMode::Plain;
    }
    OutputMode::Tui
}

/// Drain a progress channel, printing log lines to stderr.
fn drain_log(rx: crossbeam_channel::Receiver<ProgressEvent>) -> Stats {
    use std::time::SystemTime;
    let mut stats = Stats::default();
    for ev in rx {
        match ev {
            ProgressEvent::Start { total_files, .. } => {
                eprintln!("[rsy] syncing {} files", total_files);
            }
            ProgressEvent::File(r) => {
                let ts = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| {
                        let s = d.as_secs();
                        format!("{:02}:{:02}:{:02}", (s / 3600) % 24, (s / 60) % 60, s % 60)
                    })
                    .unwrap_or_default();
                if r.skipped {
                    eprintln!("[{}] skip  {}", ts, r.path);
                } else {
                    eprintln!("[{}] send  {}  ({} B)", ts, r.path, r.literal + r.matched);
                }
            }
            ProgressEvent::Done(s) => {
                stats = s;
            }
            ProgressEvent::Error(msg) => {
                eprintln!("[rsy] error: {}", msg);
            }
        }
    }
    stats
}

// ─── generic run-with-output helper ──────────────────────────────────────────

fn finish(stats: Stats, verbose: bool, show_stats: bool, quiet: bool, human: bool) {
    if !quiet {
        print_summary(&stats, verbose, human);
    }
    if show_stats {
        print_stats(&stats);
    }
    if stats.files_errored > 0 {
        std::process::exit(23);
    }
}

#[allow(clippy::too_many_arguments)]
fn run_with_output<F>(
    out_mode: OutputMode,
    opts: SyncOpts,
    verbose: bool,
    show_stats: bool,
    quiet: bool,
    human: bool,
    display_src: String,
    display_dst: String,
    run: F,
) -> Result<()>
where
    F: FnOnce(SyncOpts) -> Result<Stats> + Send + 'static,
{
    match out_mode {
        OutputMode::Plain => {
            let stats = run(opts)?;
            finish(stats, verbose, show_stats, quiet, human);
        }
        OutputMode::Log => {
            let (tx, rx) = crossbeam_channel::unbounded::<ProgressEvent>();
            let tx_err = tx.clone();
            let mut run_opts = opts;
            run_opts.progress_tx = Some(tx);
            let sync_thread = std::thread::spawn(move || {
                let result = run(run_opts);
                if let Err(err) = &result {
                    let _ = tx_err.send(ProgressEvent::Error(err.to_string()));
                }
                result
            });
            let _ = drain_log(rx);
            let stats = sync_thread
                .join()
                .map_err(|_| anyhow::anyhow!("sync panicked"))??;
            finish(stats, verbose, show_stats, quiet, human);
        }
        OutputMode::Tui => {
            let (tx, rx) = crossbeam_channel::unbounded::<ProgressEvent>();
            let tx_err = tx.clone();
            let mut run_opts = opts;
            run_opts.progress_tx = Some(tx);
            let sync_thread = std::thread::spawn(move || {
                let result = run(run_opts);
                if let Err(err) = &result {
                    let _ = tx_err.send(ProgressEvent::Error(err.to_string()));
                }
                result
            });
            let tui_result = tui::run_tui(display_src, display_dst, rx)?;
            let join_result = sync_thread
                .join()
                .map_err(|_| anyhow::anyhow!("sync panicked"))?;
            let stats = match tui_result {
                Some(s) => {
                    join_result?;
                    s
                }
                None => join_result?,
            };
            finish(stats, verbose, show_stats, quiet, human);
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    // Default to 2× logical CPUs for I/O-bound work; user can override with --jobs
    let threads = cli.jobs.unwrap_or_else(|| {
        (std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            * 2)
        .max(4)
    });
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .ok();

    // Build filter list (includes before excludes, matching cli order)
    let mut filter = FilterList::default();
    for pat in &cli.includes {
        filter.add_include(pat);
    }
    for pat in &cli.excludes {
        filter.add_exclude(pat);
    }
    if let Some(f) = &cli.exclude_from {
        filter.load_from_file(RuleKind::Exclude, f)?;
    }

    let files_from = if let Some(ref path) = cli.files_from {
        let raw = std::fs::read_to_string(path)?;
        let sep: char = if cli.from0 { '\0' } else { '\n' };
        let mut set: std::collections::HashSet<std::path::PathBuf> =
            std::collections::HashSet::new();
        for line in raw.split(sep) {
            let l = line.trim_end_matches('\r').trim();
            if l.is_empty() || l.starts_with('#') {
                continue;
            }
            set.insert(std::path::PathBuf::from(l));
        }
        Some(set)
    } else {
        None
    };

    let archive = cli.archive;
    let out_mode = output_mode(&cli);

    let progress_effective = cli.progress || cli.partial_progress;
    let partial_effective = cli.partial || cli.partial_progress;

    if cli.hard_links {
        tracing::warn!("--hard-links is accepted but not implemented");
    }
    if cli.acls {
        tracing::warn!("--acls is accepted but not implemented");
    }
    if cli.xattrs {
        tracing::warn!("--xattrs is accepted but not implemented");
    }
    if cli.devices {
        tracing::warn!("--devices is accepted but not implemented");
    }
    if cli.sparse {
        tracing::warn!("--sparse is accepted but not implemented");
    }
    if cli.link_dest.is_some() {
        tracing::warn!("--link-dest is accepted but not implemented");
    }
    if cli.bwlimit.is_some() {
        tracing::warn!("--bwlimit is accepted but not implemented");
    }

    let backup_active = cli.backup || cli.backup_dir.is_some();
    let backup_suffix = cli.suffix.clone().unwrap_or_else(|| {
        if cli.backup_dir.is_some() {
            String::new()
        } else {
            "~".to_string()
        }
    });
    if backup_suffix.contains('/') || backup_suffix.contains('\\') || backup_suffix.contains('\0') {
        anyhow::bail!(
            "--suffix must not contain path separators or NUL: {:?}",
            backup_suffix
        );
    }

    let opts = SyncOpts {
        delete: cli.delete,
        update: cli.update,
        ignore_existing: cli.ignore_existing,
        existing: cli.existing,
        remove_source_files: cli.remove_source_files,
        size_only: cli.size_only,
        modify_window: cli.modify_window.unwrap_or(0),
        copy_links: cli.copy_links,
        whole_file: cli.whole_file,
        checksum: cli.checksum,
        inplace: cli.inplace,
        one_file_system: cli.one_file_system,
        preserve_times: cli.times || archive,
        preserve_perms: cli.perms || archive,
        preserve_owner: cli.owner || archive,
        preserve_group: cli.group || archive,
        filter,
        max_size: cli.max_size.as_deref().map(parse_size).transpose()?,
        min_size: cli.min_size.as_deref().map(parse_size).transpose()?,
        files_from,
        prune_empty_dirs: cli.prune_empty_dirs,
        block_size: cli.block_size,
        compress: cli.compress,
        bwlimit: cli.bwlimit.as_deref().map(parse_size).transpose()?,
        backup: backup_active,
        backup_dir: cli.backup_dir.as_ref().map(std::path::PathBuf::from),
        backup_suffix,
        partial: partial_effective,
        verbose: cli.verbose || cli.log,
        quiet: cli.quiet,
        human_readable: cli.human_readable,
        progress: progress_effective,
        stats: cli.stats,
        dry_run: cli.dry_run,
        progress_tx: None,
    };

    if opts.compress {
        tracing::warn!("--compress is not implemented yet; continuing without compression");
    }

    let verbose = opts.verbose;
    let quiet = opts.quiet;
    let human = opts.human_readable;
    let show_stats = opts.stats;
    let ssh_opts = transport::ssh::SshOpts {
        port: cli.port,
        identity: cli.identity.clone(),
        rsh: cli.rsh.clone(),
        jump: cli.jump.clone(),
        ssh_config: cli.ssh_config.clone(),
        ssh_compress: cli.ssh_compress,
        ssh_opts: cli.ssh_opts.clone(),
        connect_timeout: cli.contimeout,
        keepalive: cli.timeout,
        rsync_path: cli.rsync_path.clone(),
        quiet: cli.quiet,
    };

    match cli::parse_mode(&cli) {
        Mode::Local { src, dst } => {
            let src_run = src.clone();
            let dst_run = dst.clone();
            run_with_output(
                out_mode,
                opts,
                verbose,
                show_stats,
                quiet,
                human,
                src,
                dst,
                move |run_opts| {
                    pipeline::sync_local(Path::new(&src_run), Path::new(&dst_run), &run_opts)
                },
            )?;
        }

        Mode::SshPush {
            host,
            src,
            remote_dst,
        } => {
            let mut pipe = transport::ssh::connect(&host, &remote_dst, false, &ssh_opts)?;
            pipe.wait_for_remote()?;
            let src_run = src.clone();
            let display_dst = format!("{}:{}", host, remote_dst);
            run_with_output(
                out_mode,
                opts,
                verbose,
                show_stats,
                quiet,
                human,
                src,
                display_dst,
                move |run_opts| pipeline::run_sender(Path::new(&src_run), &mut pipe, &run_opts),
            )?;
        }

        Mode::SshPull {
            host,
            remote_src,
            dst,
        } => {
            let mut pipe = transport::ssh::connect(&host, &remote_src, true, &ssh_opts)?;
            pipe.wait_for_remote()?;
            let dst_run = dst.clone();
            let display_src = format!("{}:{}", host, remote_src);
            run_with_output(
                out_mode,
                opts,
                verbose,
                show_stats,
                quiet,
                human,
                display_src,
                dst,
                move |run_opts| pipeline::run_receiver(Path::new(&dst_run), &mut pipe, &run_opts),
            )?;
        }

        Mode::Server { sender_side, path } => {
            let mut pipe = transport::Pipe::new(std::io::stdin(), std::io::stdout());
            if sender_side {
                pipeline::run_sender(Path::new(&path), &mut pipe, &opts)?;
            } else {
                pipeline::run_receiver(Path::new(&path), &mut pipe, &opts)?;
            }
        }

        Mode::Daemon {
            host,
            port,
            src,
            dst,
        } => {
            tracing::debug!(target = "rsy::daemon", "target path: {dst}");
            let mut pipe = transport::daemon::connect(&host, port)?;
            let stats = pipeline::run_sender(Path::new(&src), &mut pipe, &opts)?;
            finish(stats, verbose, show_stats, quiet, human);
        }
    }

    Ok(())
}

fn print_summary(s: &Stats, verbose: bool, human: bool) {
    fn human_bytes(n: u64) -> String {
        const G: u64 = 1 << 30;
        const M: u64 = 1 << 20;
        const K: u64 = 1 << 10;
        if n >= G {
            format!("{:.2}G", n as f64 / G as f64)
        } else if n >= M {
            format!("{:.2}M", n as f64 / M as f64)
        } else if n >= K {
            format!("{:.2}K", n as f64 / K as f64)
        } else {
            format!("{}B", n)
        }
    }
    if verbose || s.files_xferred > 0 {
        if human {
            eprintln!(
                "sent {}/{} files  literal {}  matched {}",
                s.files_xferred,
                s.files_total,
                human_bytes(s.literal_bytes),
                human_bytes(s.matched_bytes),
            );
        } else {
            eprintln!(
                "sent {}/{} files  literal {} B  matched {} B",
                s.files_xferred, s.files_total, s.literal_bytes, s.matched_bytes,
            );
        }
    }
    if s.files_errored > 0 {
        eprintln!(
            "warning: {} file(s) could not be transferred",
            s.files_errored
        );
    }
}
