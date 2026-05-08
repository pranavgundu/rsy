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

fn finish(stats: Stats, verbose: bool, show_stats: bool) {
    print_summary(&stats, verbose);
    if show_stats {
        print_stats(&stats);
    }
    if stats.files_errored > 0 {
        std::process::exit(23);
    }
}

fn run_with_output<F>(
    out_mode: OutputMode,
    opts: SyncOpts,
    verbose: bool,
    show_stats: bool,
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
            finish(stats, verbose, show_stats);
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
            finish(stats, verbose, show_stats);
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
            finish(stats, verbose, show_stats);
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

    let archive = cli.archive;
    let out_mode = output_mode(&cli);

    let opts = SyncOpts {
        delete: cli.delete,
        update: cli.update,
        ignore_existing: cli.ignore_existing,
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
        block_size: cli.block_size,
        compress: cli.compress,
        verbose: cli.verbose || cli.log,
        progress: cli.progress,
        stats: cli.stats,
        dry_run: cli.dry_run,
        progress_tx: None,
    };

    if opts.compress {
        tracing::warn!("--compress is not implemented yet; continuing without compression");
    }

    let verbose = opts.verbose;
    let show_stats = opts.stats;

    match cli::parse_mode(&cli) {
        Mode::Local { src, dst } => {
            let src_run = src.clone();
            let dst_run = dst.clone();
            run_with_output(
                out_mode,
                opts,
                verbose,
                show_stats,
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
            let mut pipe = transport::ssh::connect(&host, &remote_dst, false)?;
            pipe.wait_for_remote()?;
            let src_run = src.clone();
            let display_dst = format!("{}:{}", host, remote_dst);
            run_with_output(
                out_mode,
                opts,
                verbose,
                show_stats,
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
            let mut pipe = transport::ssh::connect(&host, &remote_src, true)?;
            pipe.wait_for_remote()?;
            let dst_run = dst.clone();
            let display_src = format!("{}:{}", host, remote_src);
            run_with_output(
                out_mode,
                opts,
                verbose,
                show_stats,
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
            finish(stats, verbose, show_stats);
        }
    }

    Ok(())
}

fn print_summary(s: &Stats, verbose: bool) {
    if verbose || s.files_xferred > 0 {
        eprintln!(
            "sent {}/{} files  literal {} B  matched {} B",
            s.files_xferred, s.files_total, s.literal_bytes, s.matched_bytes,
        );
    }
    if s.files_errored > 0 {
        eprintln!(
            "warning: {} file(s) could not be transferred",
            s.files_errored
        );
    }
}
