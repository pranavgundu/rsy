use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, TryRecvError};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Gauge, Paragraph},
};

use crate::pipeline::{FileRecord, ProgressEvent, Stats};

const TICK: Duration = Duration::from_millis(50);
const MAX_RECENT: usize = 200;
const SPEED_PANE_W: u16 = 24;

// ─── state ────────────────────────────────────────────────────────────────────

struct TuiState {
    src: String,
    dst: String,
    total_files: usize,
    total_bytes: u64,
    done_files: usize,
    done_bytes: u64,
    skipped: usize,
    literal: u64,
    matched: u64,
    recent: VecDeque<FileRecord>,
    start: Instant,
    speed_window: VecDeque<(Instant, u64)>,
    peak_speed: f64,
    finished: bool,
    error: Option<String>,
}

impl TuiState {
    fn new(src: String, dst: String) -> Self {
        Self {
            src,
            dst,
            total_files: 0,
            total_bytes: 0,
            done_files: 0,
            done_bytes: 0,
            skipped: 0,
            literal: 0,
            matched: 0,
            recent: VecDeque::new(),
            start: Instant::now(),
            speed_window: VecDeque::new(),
            peak_speed: 0.0,
            finished: false,
            error: None,
        }
    }

    fn apply(&mut self, ev: ProgressEvent) {
        match ev {
            ProgressEvent::Start {
                total_files,
                total_bytes,
            } => {
                self.total_files = total_files;
                self.total_bytes = total_bytes;
            }
            ProgressEvent::File(r) => {
                self.done_bytes += r.size;
                if r.skipped {
                    self.skipped += 1;
                } else {
                    self.done_files += 1;
                    self.literal += r.literal;
                    self.matched += r.matched;
                }
                let now = Instant::now();
                self.speed_window.push_back((now, self.done_bytes));
                while self.speed_window.len() > 1 {
                    if now.duration_since(self.speed_window[0].0) > Duration::from_secs(4) {
                        self.speed_window.pop_front();
                    } else {
                        break;
                    }
                }
                let spd = self.speed_bps();
                if spd > self.peak_speed {
                    self.peak_speed = spd;
                }
                if self.recent.len() >= MAX_RECENT {
                    self.recent.pop_front();
                }
                self.recent.push_back(r);
            }
            ProgressEvent::Done(s) => {
                self.done_files = s.files_xferred;
                self.skipped = s.files_skipped;
                self.literal = s.literal_bytes;
                self.matched = s.matched_bytes;
                self.done_bytes = s.total_size;
                self.finished = true;
            }
            ProgressEvent::Error(msg) => {
                self.error = Some(msg);
                self.finished = true;
            }
        }
    }

    fn speed_bps(&self) -> f64 {
        if self.speed_window.len() < 2 {
            return 0.0;
        }
        let (t0, b0) = self.speed_window[0];
        let (t1, b1) = self.speed_window[self.speed_window.len() - 1];
        let dt = t1.duration_since(t0).as_secs_f64();
        if dt < 0.01 {
            return 0.0;
        }
        b1.saturating_sub(b0) as f64 / dt
    }

    fn avg_speed_bps(&self) -> f64 {
        let elapsed = self.start.elapsed().as_secs_f64();
        if elapsed < 0.1 {
            return 0.0;
        }
        self.done_bytes as f64 / elapsed
    }

    fn files_per_sec(&self) -> f64 {
        let elapsed = self.start.elapsed().as_secs_f64();
        if elapsed < 0.1 {
            return 0.0;
        }
        (self.done_files + self.skipped) as f64 / elapsed
    }

    fn eta(&self) -> Option<u64> {
        let remaining = self.total_bytes.saturating_sub(self.done_bytes);
        if remaining == 0 {
            return Some(0);
        }
        let spd = self.speed_bps();
        if spd < 100.0 {
            return None;
        }
        Some((remaining as f64 / spd) as u64)
    }
}

// ─── formatting helpers ───────────────────────────────────────────────────────

fn human(n: u64) -> String {
    const G: u64 = 1 << 30;
    const M: u64 = 1 << 20;
    const K: u64 = 1 << 10;
    if n >= G {
        format!("{:.2} GB", n as f64 / G as f64)
    } else if n >= M {
        format!("{:.1} MB", n as f64 / M as f64)
    } else if n >= K {
        format!("{:.1} KB", n as f64 / K as f64)
    } else {
        format!("{} B", n)
    }
}

fn speed_str(bps: f64) -> String {
    if bps >= 1e9 {
        format!("{:.2} GB/s", bps / 1e9)
    } else if bps >= 1e6 {
        format!("{:.1} MB/s", bps / 1e6)
    } else if bps >= 1e3 {
        format!("{:.0} KB/s", bps / 1e3)
    } else {
        format!("{:.0} B/s", bps)
    }
}

fn eta_str(secs: Option<u64>) -> String {
    match secs {
        None => "--:--:--".to_string(),
        Some(s) => format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60),
    }
}

fn trim_path(p: &str, max: usize) -> String {
    if p.len() <= max {
        format!("{:<width$}", p, width = max)
    } else {
        format!(">{}", &p[p.len().saturating_sub(max - 1)..])
    }
}

// ─── styles ───────────────────────────────────────────────────────────────────

fn s_dim() -> Style {
    Style::new().fg(Color::DarkGray)
}
fn s_cyan_bold() -> Style {
    Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD)
}
fn s_white() -> Style {
    Style::new().fg(Color::White)
}
fn s_white_bold() -> Style {
    Style::new().fg(Color::White).add_modifier(Modifier::BOLD)
}
fn s_yellow() -> Style {
    Style::new().fg(Color::Yellow)
}
fn s_green() -> Style {
    Style::new().fg(Color::Green).add_modifier(Modifier::BOLD)
}
fn s_red_bold() -> Style {
    Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)
}

fn bordered(title: &str) -> Block<'_> {
    let b = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(s_dim());
    if title.is_empty() {
        b
    } else {
        b.title(Span::styled(format!(" {} ", title), s_cyan_bold()))
    }
}

// ─── render ───────────────────────────────────────────────────────────────────

fn render(f: &mut Frame, s: &TuiState) {
    let area = f.area();
    let chunks = Layout::vertical([
        Constraint::Length(3), // header
        Constraint::Length(5), // transfer gauge
        Constraint::Min(4),    // speed pane + recent files
        Constraint::Length(4), // statistics
    ])
    .split(area);

    render_header(f, s, chunks[0]);
    render_transfer(f, s, chunks[1]);

    // horizontal split: speed pane left, recent files right
    let mid =
        Layout::horizontal([Constraint::Length(SPEED_PANE_W), Constraint::Min(0)]).split(chunks[2]);

    render_speed(f, s, mid[0]);
    render_recent(f, s, mid[1]);
    render_stats(f, s, chunks[3]);
}

fn render_header(f: &mut Frame, s: &TuiState, area: ratatui::layout::Rect) {
    let elapsed = s.start.elapsed().as_secs();
    let hh = elapsed / 3600;
    let mm = (elapsed % 3600) / 60;
    let ss = elapsed % 60;

    let mut spans = vec![
        Span::styled(" rsy  ", s_cyan_bold()),
        Span::styled(s.src.as_str(), s_white()),
        Span::styled("  ->  ", s_dim()),
        Span::styled(s.dst.as_str(), s_white()),
        Span::styled(format!("  [{hh:02}:{mm:02}:{ss:02}]"), s_dim()),
    ];
    if let Some(err) = &s.error {
        spans.push(Span::styled("  ERROR: ", s_red_bold()));
        spans.push(Span::styled(err.as_str(), s_red_bold()));
    }
    let blk = bordered("");
    let inner = blk.inner(area);
    f.render_widget(blk, area);
    f.render_widget(Paragraph::new(Line::from(spans)), inner);
}

fn render_transfer(f: &mut Frame, s: &TuiState, area: ratatui::layout::Rect) {
    let ratio = if s.total_bytes > 0 {
        (s.done_bytes as f64 / s.total_bytes as f64).clamp(0.0, 1.0)
    } else if s.total_files > 0 {
        ((s.done_files + s.skipped) as f64 / s.total_files as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let pct = (ratio * 100.0) as u64;
    let done = s.done_files + s.skipped;

    let spd = s.speed_bps();
    let spd_label = if spd > 0.0 {
        speed_str(spd)
    } else {
        "--".to_string()
    };
    let title = format!(" Transfer   {}   ETA {} ", spd_label, eta_str(s.eta()));

    let blk = Block::default()
        .title(Span::styled(title, s_cyan_bold()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(s_dim());
    let inner = blk.inner(area);
    f.render_widget(blk, area);

    let rows = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .split(inner);

    let gauge_label = format!(
        " {:3}%   {}/{} files   {}",
        pct,
        done,
        s.total_files,
        human(s.done_bytes),
    );
    let gauge = Gauge::default()
        .gauge_style(Style::new().fg(Color::Cyan).bg(Color::DarkGray))
        .ratio(ratio)
        .label(Span::styled(gauge_label, s_white_bold()))
        .use_unicode(true);
    f.render_widget(gauge, rows[0]);

    let info = Line::from(vec![
        Span::styled(" Literal ", s_dim()),
        Span::styled(human(s.literal), s_yellow()),
        Span::styled("   Matched ", s_dim()),
        Span::styled(human(s.matched), s_yellow()),
    ]);
    f.render_widget(Paragraph::new(info), rows[1]);
}

fn render_speed(f: &mut Frame, s: &TuiState, area: ratatui::layout::Rect) {
    let blk = bordered("Speed");
    let inner = blk.inner(area);
    f.render_widget(blk, area);

    let cur = s.speed_bps();
    let avg = s.avg_speed_bps();
    let fps = s.files_per_sec();

    // current speed on its own bold line, then avg/peak/fps below
    let cur_str = if cur > 0.0 {
        speed_str(cur)
    } else {
        "--".to_string()
    };

    let lines = vec![
        Line::from(vec![
            Span::styled(" now  ", s_dim()),
            Span::styled(cur_str, s_white_bold()),
        ]),
        Line::from(vec![
            Span::styled(" avg  ", s_dim()),
            Span::styled(
                if avg > 0.0 {
                    speed_str(avg)
                } else {
                    "--".to_string()
                },
                s_yellow(),
            ),
        ]),
        Line::from(vec![
            Span::styled(" peak ", s_dim()),
            Span::styled(
                if s.peak_speed > 0.0 {
                    speed_str(s.peak_speed)
                } else {
                    "--".to_string()
                },
                s_cyan_bold(),
            ),
        ]),
        Line::from(vec![Span::raw("")]),
        Line::from(vec![
            Span::styled(" f/s  ", s_dim()),
            Span::styled(
                if fps > 0.0 {
                    format!("{fps:.1}")
                } else {
                    "--".to_string()
                },
                s_yellow(),
            ),
        ]),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn render_recent(f: &mut Frame, s: &TuiState, area: ratatui::layout::Rect) {
    let blk = bordered("Recent Files");
    let inner = blk.inner(area);
    f.render_widget(blk, area);

    let height = inner.height as usize;
    let path_w = (inner.width as usize).saturating_sub(22); // leave room for size + kind
    let skip = s.recent.len().saturating_sub(height);

    let lines: Vec<Line> = s
        .recent
        .iter()
        .skip(skip)
        .map(|r| {
            let (icon, icon_style) = if r.skipped {
                ("-", s_dim())
            } else {
                ("+", s_green())
            };
            let kind_span = if r.skipped {
                Span::styled("skip", s_dim())
            } else if r.matched == 0 {
                Span::styled(" new", s_green())
            } else {
                let total = r.literal + r.matched;
                let pct = r
                    .literal
                    .saturating_mul(100)
                    .checked_div(total)
                    .unwrap_or(0);
                Span::styled(format!("~{pct:2}%"), s_yellow())
            };
            Line::from(vec![
                Span::styled(format!(" {icon} "), icon_style),
                Span::styled(trim_path(&r.path, path_w.max(10)), s_white()),
                Span::styled(format!(" {:>10}", human(r.size)), s_yellow()),
                Span::raw("  "),
                kind_span,
            ])
        })
        .collect();

    f.render_widget(Paragraph::new(lines), inner);
}

fn render_stats(f: &mut Frame, s: &TuiState, area: ratatui::layout::Rect) {
    let blk = bordered("Statistics");
    let inner = blk.inner(area);
    f.render_widget(blk, area);

    let lines = vec![
        Line::from(vec![
            Span::styled("  Files    ", s_dim()),
            Span::styled(
                format!("{:>5} / {:<5}", s.done_files + s.skipped, s.total_files),
                s_white_bold(),
            ),
            Span::styled("   Literal  ", s_dim()),
            Span::styled(human(s.literal), s_yellow()),
        ]),
        Line::from(vec![
            Span::styled("  Skipped  ", s_dim()),
            Span::styled(format!("{:>5}", s.skipped), s_dim()),
            Span::styled("   Matched  ", s_dim()),
            Span::styled(human(s.matched), s_yellow()),
        ]),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

// ─── entry point ─────────────────────────────────────────────────────────────

pub fn run_tui(
    src: String,
    dst: String,
    rx: Receiver<ProgressEvent>,
) -> anyhow::Result<Option<Stats>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        orig_hook(info);
    }));

    let backend = CrosstermBackend::new(io::stdout());
    let mut term = Terminal::new(backend)?;

    let mut state = TuiState::new(src, dst);
    let mut final_stats: Option<Stats> = None;

    'main: loop {
        loop {
            match rx.try_recv() {
                Ok(ProgressEvent::Done(stats)) => {
                    final_stats = Some(stats.clone());
                    state.apply(ProgressEvent::Done(stats));
                }
                Ok(ev) => state.apply(ev),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    // Worker thread exited without sending Done/Error.
                    // Mark finished so UI can't spin forever on a dead channel.
                    if state.error.is_none() && final_stats.is_none() {
                        state.error = Some("progress channel disconnected".to_string());
                    }
                    state.finished = true;
                    break;
                }
            }
        }

        term.draw(|f| render(f, &state))?;

        if state.finished {
            std::thread::sleep(Duration::from_millis(400));
            term.draw(|f| render(f, &state))?;
            break 'main;
        }

        if event::poll(TICK)?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            break 'main;
        }
    }

    disable_raw_mode()?;
    execute!(term.backend_mut(), LeaveAlternateScreen)?;
    term.show_cursor()?;

    Ok(final_stats)
}
