use super::Pipe;
use anyhow::{Context, Result};
use std::process::{Command, Stdio};

/// Options governing the SSH process used to reach the remote `rsy --server`.
#[derive(Debug, Default, Clone)]
pub struct SshOpts {
    pub port: Option<u16>,
    pub identity: Option<String>,
    pub rsh: Option<String>,
    pub jump: Option<String>,
    pub ssh_config: Option<String>,
    pub ssh_compress: bool,
    pub ssh_opts: Vec<String>,
    pub connect_timeout: Option<u32>,
    pub keepalive: Option<u32>,
    pub rsync_path: Option<String>,
    pub quiet: bool,
}

fn validate_remote_bin(bin: &str) -> Result<()> {
    if !bin
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '.'))
    {
        anyhow::bail!("rsync-path contains unsafe characters: {bin:?}");
    }
    Ok(())
}

fn validate_host(host: &str) -> Result<()> {
    if host.starts_with('-') {
        anyhow::bail!("host looks like an ssh flag: {host:?}");
    }
    if host.chars().any(|c| {
        matches!(
            c,
            '`' | '$' | '\\' | '\'' | '"' | ';' | '&' | '|' | '(' | ')' | '<' | '>' | '\n' | '\r'
        )
    }) {
        anyhow::bail!("host contains unsafe characters: {host:?}");
    }
    Ok(())
}

/// Build the (program, args) the SSH transport will spawn. Pulled out of
/// `connect` so the argv shape is unit-testable without spawning a process.
fn build_ssh_argv(
    host: &str,
    remote_path: &str,
    sender_side: bool,
    rsy_remote: &str,
    opts: &SshOpts,
) -> Result<(String, Vec<String>)> {
    let mut args: Vec<String> = Vec::new();
    let prog: String = if let Some(ref rsh) = opts.rsh {
        let mut parts = rsh.split_whitespace();
        let head = parts.next().context("--rsh value is empty")?.to_string();
        for a in parts {
            args.push(a.to_string());
        }
        head
    } else {
        args.push("-e".into());
        args.push("none".into());
        if let Some(p) = opts.port {
            args.push("-p".into());
            args.push(p.to_string());
        }
        if let Some(ref id) = opts.identity {
            args.push("-i".into());
            args.push(id.clone());
        }
        if let Some(ref j) = opts.jump {
            args.push("-J".into());
            args.push(j.clone());
        }
        if let Some(ref cfg) = opts.ssh_config {
            args.push("-F".into());
            args.push(cfg.clone());
        }
        if opts.ssh_compress {
            args.push("-C".into());
        }
        if opts.quiet {
            args.push("-q".into());
        }
        if let Some(t) = opts.connect_timeout {
            args.push("-o".into());
            args.push(format!("ConnectTimeout={t}"));
        }
        if let Some(t) = opts.keepalive {
            args.push("-o".into());
            args.push(format!("ServerAliveInterval={t}"));
            args.push("-o".into());
            args.push("ServerAliveCountMax=3".into());
        }
        for o in &opts.ssh_opts {
            args.push("-o".into());
            args.push(o.clone());
        }
        "ssh".into()
    };

    args.push("--".into());
    args.push(host.to_string());
    args.push(rsy_remote.to_string());
    args.push("--server".into());
    if sender_side {
        args.push("--sender".into());
    }
    args.push(remote_path.to_string());
    Ok((prog, args))
}

/// Launch the remote `rsy --server [--sender] <path>` over SSH (or a custom
/// remote shell via `--rsh`) and return a Pipe wired to its stdin/stdout.
/// A background thread reaps the child when the pipes close.
pub fn connect(
    host: &str,
    remote_path: &str,
    sender_side: bool,
    opts: &SshOpts,
) -> Result<Pipe> {
    let rsy_remote = opts
        .rsync_path
        .clone()
        .or_else(|| std::env::var("RSY_REMOTE_BIN").ok())
        .unwrap_or_else(|| "rsy".into());
    validate_remote_bin(&rsy_remote)?;
    validate_host(host)?;

    let (prog, args) = build_ssh_argv(host, remote_path, sender_side, &rsy_remote, opts)?;
    let mut cmd = Command::new(&prog);
    for a in &args {
        cmd.arg(a);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let mut child = cmd.spawn().context("ssh spawn failed")?;
    let stdin = child.stdin.take().context("no stdin")?;
    let stdout = child.stdout.take().context("no stdout")?;

    std::thread::spawn(move || {
        let _ = child.wait();
    });

    Ok(Pipe::new(stdout, stdin))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(host: &str, sender: bool, opts: &SshOpts) -> (String, Vec<String>) {
        build_ssh_argv(host, "/remote/path", sender, "rsy", opts).unwrap()
    }

    #[test]
    fn ssh_argv_defaults_to_ssh_with_no_pty() {
        let opts = SshOpts::default();
        let (prog, args) = argv("host", false, &opts);
        assert_eq!(prog, "ssh");
        assert_eq!(&args[..2], &["-e", "none"]);
        let tail = &args[args.len() - 4..];
        assert_eq!(tail, &["host", "rsy", "--server", "/remote/path"]);
    }

    #[test]
    fn ssh_argv_includes_port_and_identity() {
        let opts = SshOpts {
            port: Some(2222),
            identity: Some("/keys/id".into()),
            ..Default::default()
        };
        let (_, args) = argv("h", false, &opts);
        let s = args.join(" ");
        assert!(s.contains("-p 2222"), "argv missing port: {s}");
        assert!(s.contains("-i /keys/id"), "argv missing identity: {s}");
    }

    #[test]
    fn ssh_argv_includes_jump_config_compress_opts() {
        let opts = SshOpts {
            jump: Some("bastion".into()),
            ssh_config: Some("/etc/ssh_conf".into()),
            ssh_compress: true,
            ssh_opts: vec!["StrictHostKeyChecking=no".into()],
            ..Default::default()
        };
        let (_, args) = argv("h", false, &opts);
        let s = args.join(" ");
        assert!(s.contains("-J bastion"), "missing -J: {s}");
        assert!(s.contains("-F /etc/ssh_conf"), "missing -F: {s}");
        assert!(s.contains("-C"), "missing -C: {s}");
        assert!(
            s.contains("-o StrictHostKeyChecking=no"),
            "missing ssh_opt: {s}"
        );
    }

    #[test]
    fn ssh_argv_includes_timeouts() {
        let opts = SshOpts {
            connect_timeout: Some(10),
            keepalive: Some(30),
            ..Default::default()
        };
        let (_, args) = argv("h", false, &opts);
        let s = args.join(" ");
        assert!(s.contains("-o ConnectTimeout=10"), "missing contimeout: {s}");
        assert!(
            s.contains("-o ServerAliveInterval=30"),
            "missing keepalive: {s}"
        );
        assert!(
            s.contains("-o ServerAliveCountMax=3"),
            "missing keepalive count: {s}"
        );
    }

    #[test]
    fn ssh_argv_sender_flag_appended_after_server() {
        let opts = SshOpts::default();
        let (_, args) = argv("h", true, &opts);
        let i = args.iter().position(|a| a == "--server").unwrap();
        assert_eq!(args[i + 1], "--sender");
    }

    #[test]
    fn ssh_argv_uses_rsh_program_and_ignores_ssh_only_flags() {
        let opts = SshOpts {
            rsh: Some("mysh -X".into()),
            port: Some(22),
            identity: Some("/k".into()),
            ..Default::default()
        };
        let (prog, args) = argv("h", false, &opts);
        assert_eq!(prog, "mysh");
        let s = args.join(" ");
        assert!(s.contains("-X"), "rsh extra arg missing: {s}");
        assert!(!s.contains("-p 22"), "ssh -p must not appear with --rsh: {s}");
        assert!(!s.contains("-i /k"), "ssh -i must not appear with --rsh: {s}");
    }

    #[test]
    fn ssh_argv_rejects_unsafe_host() {
        assert!(validate_host("-oProxyCommand=foo").is_err());
        assert!(validate_host("ok-host").is_ok());
        assert!(validate_host("u@host;rm").is_err());
    }

    #[test]
    fn ssh_argv_rejects_unsafe_remote_bin() {
        assert!(validate_remote_bin("rsy").is_ok());
        assert!(validate_remote_bin("/usr/local/bin/rsy").is_ok());
        assert!(validate_remote_bin("rsy; rm -rf /").is_err());
    }

    #[test]
    fn ssh_argv_empty_rsh_errors() {
        let opts = SshOpts {
            rsh: Some("   ".into()),
            ..Default::default()
        };
        assert!(build_ssh_argv("h", "/p", false, "rsy", &opts).is_err());
    }
}
