use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleKind {
    Include,
    Exclude,
}

#[derive(Debug, Clone)]
pub struct Rule {
    kind: RuleKind,
    pattern: String,
    anchored: bool, // pattern contains / → match full path
    dir_only: bool, // pattern ends with / → dirs only
}

impl Rule {
    pub fn new(kind: RuleKind, raw: &str) -> Self {
        let dir_only = raw.ends_with('/');
        let pat = raw.trim_end_matches('/');
        // anchored if pattern contains an interior slash (not just trailing)
        let anchored = pat.contains('/');
        Rule {
            kind,
            pattern: pat.to_string(),
            anchored,
            dir_only,
        }
    }

    fn matches(&self, rel: &Path, is_dir: bool) -> bool {
        if self.dir_only && !is_dir {
            return false;
        }
        let s = rel.to_string_lossy();
        if self.anchored {
            glob_match(&self.pattern, &s)
        } else {
            // Unanchored: test against each path component and the full path
            if glob_match(&self.pattern, &s) {
                return true;
            }
            for comp in rel.components() {
                let c = comp.as_os_str().to_string_lossy();
                if glob_match(&self.pattern, &c) {
                    return true;
                }
            }
            false
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FilterList(pub Vec<Rule>);

impl FilterList {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn add_exclude(&mut self, pat: &str) {
        self.0.push(Rule::new(RuleKind::Exclude, pat));
    }
    pub fn add_include(&mut self, pat: &str) {
        self.0.push(Rule::new(RuleKind::Include, pat));
    }

    pub fn load_from_file(&mut self, kind: RuleKind, path: &str) -> anyhow::Result<()> {
        for line in std::fs::read_to_string(path)?.lines() {
            let l = line.trim();
            if l.is_empty() || l.starts_with('#') {
                continue;
            }
            self.0.push(Rule::new(kind.clone(), l));
        }
        Ok(())
    }

    /// Returns true if the file should be transferred
    pub fn allow(&self, rel: &Path, is_dir: bool) -> bool {
        // Direct match
        for rule in &self.0 {
            if rule.matches(rel, is_dir) {
                return rule.kind == RuleKind::Include;
            }
        }
        // Ancestor directory excluded? Walk up the path.
        let mut ancestor = rel.parent();
        while let Some(p) = ancestor {
            if p.as_os_str().is_empty() {
                break;
            }
            for rule in &self.0 {
                if rule.dir_only && rule.matches(p, true) {
                    return rule.kind == RuleKind::Include;
                }
            }
            ancestor = p.parent();
        }
        true
    }
}

// ─── glob matcher ────────────────────────────────────────────────────────────
// * = any chars except /    ** = any chars including /    ? = one char (not /)

fn glob_match(pattern: &str, text: &str) -> bool {
    glob_inner(pattern.as_bytes(), text.as_bytes(), 0, 0)
}

fn glob_inner(p: &[u8], t: &[u8], mut pi: usize, mut ti: usize) -> bool {
    loop {
        if pi == p.len() {
            return ti == t.len();
        }

        // double star
        if p[pi] == b'*' && pi + 1 < p.len() && p[pi + 1] == b'*' {
            pi += 2;
            if pi < p.len() && p[pi] == b'/' {
                pi += 1;
            }
            // try matching from every suffix of t
            loop {
                if glob_inner(p, t, pi, ti) {
                    return true;
                }
                if ti == t.len() {
                    return false;
                }
                ti += 1;
            }
        }

        // single star
        if p[pi] == b'*' {
            pi += 1;
            loop {
                if glob_inner(p, t, pi, ti) {
                    return true;
                }
                if ti == t.len() || t[ti] == b'/' {
                    return false;
                }
                ti += 1;
            }
        }

        if ti == t.len() {
            return false;
        }

        if p[pi] == b'?' {
            if t[ti] == b'/' {
                return false;
            }
            pi += 1;
            ti += 1;
            continue;
        }

        if !p[pi].eq_ignore_ascii_case(&t[ti]) {
            return false;
        }
        pi += 1;
        ti += 1;
    }
}

// ─── size parser ─────────────────────────────────────────────────────────────

pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    let (num, suffix) = if s.ends_with(|c: char| c.is_ascii_alphabetic()) {
        (&s[..s.len() - 1], &s[s.len() - 1..])
    } else {
        (s, "")
    };
    let base: u64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid size: {s}"))?;
    let mult = match suffix.to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "K" => 1024,
        "M" => 1024 * 1024,
        "G" => 1024 * 1024 * 1024,
        "T" => 1024u64 * 1024 * 1024 * 1024,
        other => anyhow::bail!("unknown size suffix: {other}"),
    };
    base.checked_mul(mult)
        .ok_or_else(|| anyhow::anyhow!("size overflow: {s}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn glob_basics() {
        assert!(glob_match("*.rs", "main.rs"));
        assert!(!glob_match("*.rs", "src/main.rs"));
        assert!(glob_match("**/*.rs", "src/main.rs"));
        assert!(glob_match("*.log", "access.log"));
        assert!(!glob_match("*.log", "access.log.gz"));
        assert!(glob_match("foo?ar", "foobar"));
        assert!(!glob_match("foo?ar", "foo/ar"));
    }

    #[test]
    fn glob_double_star() {
        assert!(glob_match("**", "a/b/c"));
        assert!(glob_match("a/**/d", "a/b/c/d"));
        assert!(!glob_match("a/**/d", "a/b/c/e"));
        assert!(glob_match("**/foo", "x/y/foo"));
    }

    #[test]
    fn filter_exclude_file() {
        let mut f = FilterList::default();
        f.add_exclude("*.log");
        assert!(!f.allow(Path::new("access.log"), false));
        assert!(f.allow(Path::new("main.rs"), false));
    }

    #[test]
    fn filter_dir_only_excludes_contents() {
        let mut f = FilterList::default();
        f.add_exclude("build/");
        assert!(!f.allow(Path::new("build/out.o"), false));
        assert!(!f.allow(Path::new("build/sub/a.o"), false));
        assert!(f.allow(Path::new("src/main.rs"), false));
    }

    #[test]
    fn filter_include_overrides_exclude() {
        let mut f = FilterList::default();
        f.add_include("keep.txt");
        f.add_exclude("*.txt");
        assert!(f.allow(Path::new("keep.txt"), false));
        assert!(!f.allow(Path::new("drop.txt"), false));
    }

    #[test]
    fn parse_size_units() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size(" 5k ").unwrap(), 5 * 1024);
        assert_eq!(parse_size("7B").unwrap(), 7);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("2M").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_size("3G").unwrap(), 3 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1T").unwrap(), 1024u64 * 1024 * 1024 * 1024);
        assert!(parse_size("bad").is_err());
        assert!(parse_size("1Z").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn parse_size_overflow_rejected() {
        // u64::MAX * K would overflow
        assert!(parse_size("18446744073709551615K").is_err());
    }

    #[test]
    fn glob_question_mark_no_slash() {
        assert!(glob_match("f?o", "foo"));
        assert!(!glob_match("f?o", "f/o"));
    }

    #[test]
    fn glob_star_no_slash() {
        assert!(glob_match("*.rs", "main.rs"));
        assert!(!glob_match("*.rs", "src/main.rs")); // * doesn't cross /
    }

    #[test]
    fn glob_double_star_nested() {
        assert!(glob_match("**/a/**/b", "x/a/y/z/b"));
        assert!(glob_match("**/a/**/b", "a/b"));
    }

    #[test]
    fn filter_empty_allows_all() {
        let f = FilterList::default();
        assert!(f.allow(Path::new("anything.txt"), false));
        assert!(f.allow(Path::new("deep/nested/file"), false));
    }

    #[test]
    fn filter_anchored_pattern() {
        let mut f = FilterList::default();
        f.add_exclude("src/main.rs"); // contains / → anchored
        assert!(!f.allow(Path::new("src/main.rs"), false));
        assert!(f.allow(Path::new("lib/main.rs"), false)); // anchored = no partial match
    }

    #[test]
    fn filter_unanchored_matches_component() {
        let mut f = FilterList::default();
        f.add_exclude("node_modules");
        assert!(!f.allow(Path::new("node_modules"), true));
        assert!(!f.allow(Path::new("packages/app/node_modules/foo.js"), false));
        assert!(f.allow(Path::new("src/foo.js"), false));
    }

    #[test]
    fn filter_multiple_excludes_first_match_wins() {
        let mut f = FilterList::default();
        f.add_include("*.rs");
        f.add_exclude("*"); // exclude everything — but includes come first
        assert!(f.allow(Path::new("main.rs"), false));
        assert!(!f.allow(Path::new("main.py"), false));
    }

    #[test]
    fn filter_dir_only_does_not_affect_files() {
        let mut f = FilterList::default();
        f.add_exclude("dist/");
        // file named dist (not a dir) should be unaffected
        assert!(f.allow(Path::new("dist"), false));
        // but contents of dir named dist are excluded
        assert!(!f.allow(Path::new("dist/bundle.js"), false));
    }

    #[test]
    fn filter_load_from_file_ignores_blanks_and_comments() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("exclude.txt");
        std::fs::write(&file, "\n# comment\n*.tmp\n cache/ \n").unwrap();

        let mut f = FilterList::default();
        f.load_from_file(RuleKind::Exclude, file.to_str().unwrap())
            .unwrap();

        assert!(!f.allow(Path::new("scratch.tmp"), false));
        assert!(!f.allow(Path::new("cache/data.bin"), false));
        assert!(f.allow(Path::new("src/main.rs"), false));
    }

    #[test]
    fn filter_first_matching_rule_wins_for_includes_and_excludes() {
        let mut f = FilterList::default();
        f.add_exclude("*.txt");
        f.add_include("keep.txt");

        assert!(!f.allow(Path::new("keep.txt"), false));
    }
}
