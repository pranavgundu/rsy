use super::Pipe;
use anyhow::{Context, Result};
use std::process::{Command, Stdio};

/// Launch `ssh <host> rsy --server [--sender] <path>` and return a Pipe
/// wired to the child's stdin/stdout.  A background thread reaps the child.
pub fn connect(host: &str, remote_path: &str, sender_side: bool) -> Result<Pipe> {
    let rsy_remote = std::env::var("RSY_REMOTE_BIN").unwrap_or_else(|_| "rsy".into());
    if !rsy_remote
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '.'))
    {
        anyhow::bail!("RSY_REMOTE_BIN contains unsafe characters: {rsy_remote:?}");
    }

    // Reject hosts that start with '-' to prevent flag injection, and hosts
    // containing shell metacharacters that could escape the remote command.
    if host.starts_with('-') {
        anyhow::bail!("host looks like an ssh flag: {host:?}");
    }
    if host
        .chars()
        .any(|c| matches!(c, '`' | '$' | '\\' | '\'' | '"' | ';' | '&' | '|' | '(' | ')' | '<' | '>' | '\n' | '\r'))
    {
        anyhow::bail!("host contains unsafe characters: {host:?}");
    }

    let mut cmd = Command::new("ssh");
    cmd.args(["-e", "none", "--", host, &rsy_remote, "--server"]);
    if sender_side {
        cmd.arg("--sender");
    }
    cmd.arg(remote_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let mut child = cmd.spawn().context("ssh spawn failed")?;
    let stdin = child.stdin.take().context("no stdin")?;
    let stdout = child.stdout.take().context("no stdout")?;

    // Background thread keeps child alive and reaps it when pipes close
    std::thread::spawn(move || {
        let _ = child.wait();
    });

    Ok(Pipe::new(stdout, stdin))
}
