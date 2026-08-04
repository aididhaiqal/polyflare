//! Re-tag Codex's own session records so `codex resume` still finds them after the provider name
//! behind them changes.
//!
//! # Why this exists
//! Codex stores the provider a session ran under in two places, and `codex resume` filters the
//! list by the provider currently configured in `~/.codex/config.toml`. Point `model_provider` at
//! a new name — moving from `codex-lb` to `polyflare`, say — and every earlier session vanishes
//! from the picker. Nothing is lost; Codex simply will not show you history tagged with a provider
//! you are no longer using.
//!
//! # Both stores must move together
//! - `sessions/**/*.jsonl` — the rollout transcripts. The provider appears either at the top level
//!   of a record or inside a `session_meta` payload, depending on the Codex version that wrote it.
//! - `state_*.sqlite` — a `threads` table with its own `model_provider` column. This is the one
//!   that actually drives the resume picker, so retagging only the JSONL files looks like it
//!   worked and changes nothing you can see.
//!
//! # Why the JSONL rewrite is a targeted byte replacement, not a JSON round-trip
//! A rollout's `session_meta` line carries the whole `base_instructions` prompt — tens of
//! kilobytes with its own escaping. Parsing that to a `serde_json::Value` and re-serialising would
//! rewrite the entire line: key order, number formatting, and escape choices are all serialiser
//! decisions, and none of them are ours to make in a file another program owns.
//!
//! So a line is parsed only to ASK whether it names the provider being replaced; the write itself
//! substitutes the first occurrence of the exact `"model_provider":"<from>"` token on that line.
//! Every other byte is preserved verbatim.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Where backups land, under `<codex_home>/backups/`.
const BACKUP_DIR_NAME: &str = "provider-retag";

/// What a retag did, or would do.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RetagReport {
    pub jsonl_scanned: usize,
    pub jsonl_matched: usize,
    pub jsonl_rewritten: usize,
    pub sqlite_dbs_scanned: usize,
    pub sqlite_rows_matched: usize,
    pub sqlite_rows_updated: usize,
    /// Where the originals were copied before anything was written. `None` on a dry run, or when
    /// there was nothing to change.
    pub backup: Option<PathBuf>,
}

/// The Codex home to operate on: `$CODEX_HOME`, else `~/.codex`.
pub fn default_codex_home() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("CODEX_HOME") {
        if !explicit.trim().is_empty() {
            return Some(PathBuf::from(explicit));
        }
    }
    std::env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".codex"))
}

/// Every `*.jsonl` under `<codex_home>/sessions`, which Codex nests by date.
///
/// Deliberately not restricted to `rollout-*.jsonl`: Codex has written session records under other
/// names across versions, and a file this tool skips is history the operator silently loses.
pub fn session_files(codex_home: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    collect_jsonl(&codex_home.join("sessions"), &mut found);
    found.sort();
    found
}

fn collect_jsonl(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            out.push(path);
        }
    }
}

/// Codex's state databases, newest schema last (`state_5.sqlite` sorts after `state_4.sqlite`).
///
/// Strictly `state_<digits>.sqlite`. A looser `state_*.sqlite` glob also matches an operator's own
/// snapshots — `state_5.backup-20260725.sqlite` sits right next to the live file — and rewriting
/// someone's backup is precisely the thing a backup exists to prevent.
pub fn state_dbs(codex_home: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(codex_home) else {
        return Vec::new();
    };
    let mut dbs: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(is_state_db)
        })
        .collect();
    dbs.sort();
    dbs
}

fn is_state_db(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("state_") else {
        return false;
    };
    let Some(version) = rest.strip_suffix(".sqlite") else {
        return false;
    };
    !version.is_empty() && version.chars().all(|c| c.is_ascii_digit())
}

/// The provider one JSONL record names, wherever this Codex version put it.
fn record_provider(line: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    if let Some(top) = value.get("model_provider").and_then(|v| v.as_str()) {
        return Some(top.to_string());
    }
    if value.get("type")?.as_str()? != "session_meta" {
        return None;
    }
    Some(
        value
            .get("payload")?
            .get("model_provider")?
            .as_str()?
            .to_string(),
    )
}

/// Replace the provider token on one line, or `None` when it is absent.
///
/// Only the FIRST occurrence is replaced. The record's own `model_provider` appears before any
/// long prompt text, so a later textual match inside that prose — a user who pasted this very
/// config into a conversation — is left alone.
fn retag_line(line: &str, from: &str, to: &str) -> Option<String> {
    let needle = format!("\"model_provider\":\"{from}\"");
    let at = line.find(&needle)?;
    let mut out = String::with_capacity(line.len() + to.len());
    out.push_str(&line[..at]);
    out.push_str(&format!("\"model_provider\":\"{to}\""));
    out.push_str(&line[at + needle.len()..]);
    Some(out)
}

/// Whether any record in this file names `provider`.
fn file_matches(path: &Path, provider: &str) -> bool {
    let Ok(content) = fs::read_to_string(path) else {
        return false;
    };
    content
        .lines()
        .any(|line| record_provider(line).as_deref() == Some(provider))
}

/// Rewrite one session file's provider tags, atomically.
///
/// Written to a sibling temp file and renamed over the original, so an interrupted run leaves
/// either the old file or the new one — never a half-written transcript Codex cannot parse.
fn rewrite_file(path: &Path, from: &str, to: &str) -> std::io::Result<bool> {
    let content = fs::read_to_string(path)?;
    let trailing_newline = content.ends_with('\n');
    let mut changed = false;
    let mut out = String::with_capacity(content.len());

    for (index, line) in content.lines().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        if record_provider(line).as_deref() == Some(from) {
            if let Some(retagged) = retag_line(line, from, to) {
                out.push_str(&retagged);
                changed = true;
                continue;
            }
        }
        // Unmatched, blank, and unparseable lines are copied through byte for byte.
        out.push_str(line);
    }
    if trailing_newline {
        out.push('\n');
    }
    if !changed {
        return Ok(false);
    }

    let temp = path.with_extension("jsonl.retag-tmp");
    {
        let mut handle = fs::File::create(&temp)?;
        handle.write_all(out.as_bytes())?;
        handle.sync_all()?;
    }
    fs::rename(&temp, path)?;
    Ok(true)
}

/// Count and update `threads.model_provider` in one Codex state database.
async fn sqlite_provider_rows(db: &Path, provider: &str) -> Result<i64, sqlx::Error> {
    let url = format!("sqlite://{}?mode=ro", db.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await?;
    // A state database without a `threads` table (or without that column) is simply not one this
    // tool has anything to do with — not an error worth failing the whole run over.
    let count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM threads WHERE model_provider = ?")
            .bind(provider)
            .fetch_one(&pool)
            .await
            .unwrap_or_default();
    pool.close().await;
    Ok(count)
}

async fn sqlite_retag(db: &Path, from: &str, to: &str) -> Result<u64, sqlx::Error> {
    let url = format!("sqlite://{}", db.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await?;
    let updated =
        match sqlx::query("UPDATE threads SET model_provider = ? WHERE model_provider = ?")
            .bind(to)
            .bind(from)
            .execute(&pool)
            .await
        {
            Ok(result) => result.rows_affected(),
            Err(_) => 0,
        };
    pool.close().await;
    Ok(updated)
}

/// A provider census across BOTH stores, so an operator can see what `--from` values exist.
pub async fn census(codex_home: &Path) -> (Vec<(String, usize)>, Vec<(String, usize)>) {
    let mut jsonl: HashMap<String, usize> = HashMap::new();
    for path in session_files(codex_home) {
        // One count per FILE, not per record: a rollout has many lines and they all belong to the
        // same session, so counting records would report a number no operator can act on.
        if let Ok(content) = fs::read_to_string(&path) {
            if let Some(provider) = content.lines().find_map(record_provider) {
                *jsonl.entry(provider).or_default() += 1;
            }
        }
    }

    let mut threads: HashMap<String, usize> = HashMap::new();
    for db in state_dbs(codex_home) {
        let url = format!("sqlite://{}?mode=ro", db.display());
        let Ok(pool) = sqlx::sqlite::SqlitePool::connect(&url).await else {
            continue;
        };
        if let Ok(rows) = sqlx::query_as::<_, (Option<String>, i64)>(
            "SELECT model_provider, COUNT(*) FROM threads GROUP BY model_provider",
        )
        .fetch_all(&pool)
        .await
        {
            for (provider, count) in rows {
                *threads
                    .entry(provider.unwrap_or_else(|| "(null)".to_string()))
                    .or_default() += count as usize;
            }
        }
        pool.close().await;
    }

    (sorted_desc(jsonl), sorted_desc(threads))
}

fn sorted_desc(map: HashMap<String, usize>) -> Vec<(String, usize)> {
    let mut rows: Vec<(String, usize)> = map.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows
}

/// Copy everything about to be modified into a timestamped directory.
///
/// Codex owns these files and this tool is rewriting thousands of them in place. The backup is
/// what makes the operation reversible by hand; without it a bad `--from` is unrecoverable.
fn create_backup(
    codex_home: &Path,
    files: &[PathBuf],
    dbs: &[PathBuf],
    stamp: &str,
) -> std::io::Result<PathBuf> {
    let dir = codex_home.join("backups").join(BACKUP_DIR_NAME).join(stamp);
    fs::create_dir_all(&dir)?;
    for db in dbs {
        if let Some(name) = db.file_name() {
            fs::copy(db, dir.join(name))?;
        }
    }
    for file in files {
        let relative = file.strip_prefix(codex_home).unwrap_or(file);
        let destination = dir.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(file, destination)?;
    }
    Ok(dir)
}

/// Re-tag every Codex session record naming `from` so it names `to` instead.
///
/// `apply` false is a dry run: it counts what would change, takes no backup, and writes nothing.
pub async fn retag(
    codex_home: &Path,
    from: &str,
    to: &str,
    apply: bool,
    stamp: &str,
) -> std::io::Result<RetagReport> {
    let mut report = RetagReport::default();

    let files = session_files(codex_home);
    report.jsonl_scanned = files.len();
    let matched: Vec<PathBuf> = files
        .into_iter()
        .filter(|path| file_matches(path, from))
        .collect();
    report.jsonl_matched = matched.len();

    let dbs = state_dbs(codex_home);
    report.sqlite_dbs_scanned = dbs.len();
    for db in &dbs {
        report.sqlite_rows_matched += sqlite_provider_rows(db, from).await.unwrap_or(0) as usize;
    }

    if !apply || (matched.is_empty() && report.sqlite_rows_matched == 0) {
        return Ok(report);
    }

    report.backup = Some(create_backup(codex_home, &matched, &dbs, stamp)?);

    for path in &matched {
        if rewrite_file(path, from, to)? {
            report.jsonl_rewritten += 1;
        }
    }
    for db in &dbs {
        report.sqlite_rows_updated += sqlite_retag(db, from, to).await.unwrap_or(0) as usize;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_line(provider: &str, instructions: &str) -> String {
        serde_json::json!({
            "timestamp": "2026-03-25T23:10:01.090Z",
            "type": "session_meta",
            "payload": {
                "id": "019d2742-81e2-7330-a5eb-fd0beaeee653",
                "model_provider": provider,
                "base_instructions": { "text": instructions },
            }
        })
        .to_string()
    }

    fn write_session(home: &Path, name: &str, body: &str) -> PathBuf {
        let nested = home.join("sessions/2026/03/26");
        fs::create_dir_all(&nested).unwrap();
        let path = nested.join(name);
        fs::write(&path, body).unwrap();
        path
    }

    async fn make_state_db(home: &Path, rows: &[(&str, i64)]) -> PathBuf {
        let db = home.join("state_5.sqlite");
        let url = format!("sqlite://{}?mode=rwc", db.display());
        let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE threads (id INTEGER PRIMARY KEY, model_provider TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for (provider, count) in rows {
            for _ in 0..*count {
                sqlx::query("INSERT INTO threads (model_provider) VALUES (?)")
                    .bind(provider)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        }
        pool.close().await;
        db
    }

    #[tokio::test]
    async fn both_stores_are_retagged_together() {
        let home = tempfile::tempdir().unwrap();
        let turns = "\n{\"type\":\"turn\",\"text\":\"hello\"}";
        let body = format!("{}{turns}", meta_line("codex-lb", "be helpful"));
        let file = write_session(home.path(), "rollout-a.jsonl", &body);
        make_state_db(home.path(), &[("codex-lb", 3), ("sakana", 1)]).await;

        let report = retag(home.path(), "codex-lb", "polyflare", true, "t0")
            .await
            .unwrap();

        assert_eq!(report.jsonl_matched, 1);
        assert_eq!(report.jsonl_rewritten, 1);
        assert_eq!(
            report.sqlite_rows_updated, 3,
            "the threads table drives the resume picker — retagging only the JSONL would look \
             like it worked and change nothing visible"
        );

        let after = fs::read_to_string(&file).unwrap();
        assert!(after.contains("\"model_provider\":\"polyflare\""));
        assert!(after.ends_with(turns), "turn history untouched");
        assert!(after.contains("be helpful"), "prompt text untouched");
    }

    #[tokio::test]
    async fn a_dry_run_reports_the_work_and_changes_nothing() {
        let home = tempfile::tempdir().unwrap();
        let body = meta_line("codex-lb", "x");
        let file = write_session(home.path(), "rollout-a.jsonl", &body);
        make_state_db(home.path(), &[("codex-lb", 2)]).await;

        let report = retag(home.path(), "codex-lb", "polyflare", false, "t0")
            .await
            .unwrap();

        assert_eq!(report.jsonl_matched, 1);
        assert_eq!(report.sqlite_rows_matched, 2);
        assert_eq!(report.jsonl_rewritten, 0);
        assert_eq!(report.sqlite_rows_updated, 0);
        assert!(report.backup.is_none(), "a dry run takes no backup either");
        assert_eq!(fs::read_to_string(&file).unwrap(), body);
    }

    #[tokio::test]
    async fn originals_are_backed_up_before_anything_is_written() {
        let home = tempfile::tempdir().unwrap();
        let body = meta_line("codex-lb", "x");
        write_session(home.path(), "rollout-a.jsonl", &body);
        make_state_db(home.path(), &[("codex-lb", 1)]).await;

        let report = retag(home.path(), "codex-lb", "polyflare", true, "20260803-2100")
            .await
            .unwrap();
        let backup = report.backup.expect("a real run backs up first");

        let saved = backup.join("sessions/2026/03/26/rollout-a.jsonl");
        assert_eq!(
            fs::read_to_string(&saved).unwrap(),
            body,
            "the backup holds the ORIGINAL, pre-retag content"
        );
        assert!(
            backup.join("state_5.sqlite").is_file(),
            "state db saved too"
        );
    }

    #[tokio::test]
    async fn other_providers_are_left_alone() {
        let home = tempfile::tempdir().unwrap();
        let other = write_session(home.path(), "rollout-b.jsonl", &meta_line("sakana", "x"));
        write_session(home.path(), "rollout-a.jsonl", &meta_line("codex-lb", "x"));
        make_state_db(home.path(), &[("codex-lb", 2), ("sakana", 5)]).await;

        let report = retag(home.path(), "codex-lb", "polyflare", true, "t0")
            .await
            .unwrap();
        assert_eq!(report.jsonl_matched, 1);
        assert_eq!(report.sqlite_rows_updated, 2);
        assert!(fs::read_to_string(&other).unwrap().contains("sakana"));

        let (_, threads) = census(home.path()).await;
        assert!(threads.contains(&("sakana".to_string(), 5)));
        assert!(threads.contains(&("polyflare".to_string(), 2)));
    }

    /// A top-level `model_provider` is how some Codex versions write it — missing this shape
    /// would silently leave those sessions behind.
    #[tokio::test]
    async fn a_top_level_provider_field_is_retagged_too() {
        let home = tempfile::tempdir().unwrap();
        let body = serde_json::json!({"model_provider": "codex-lb", "type": "other"}).to_string();
        let file = write_session(home.path(), "rollout-a.jsonl", &body);

        retag(home.path(), "codex-lb", "polyflare", true, "t0")
            .await
            .unwrap();
        assert!(fs::read_to_string(&file)
            .unwrap()
            .contains("\"model_provider\":\"polyflare\""));
    }

    /// Why the replacement is first-occurrence-only: prompt text that quotes the token is prose,
    /// not configuration, and must survive untouched.
    #[tokio::test]
    async fn a_provider_token_quoted_in_prompt_text_survives() {
        let home = tempfile::tempdir().unwrap();
        let quoted = "set \"model_provider\":\"codex-lb\" in your config";
        let file = write_session(
            home.path(),
            "rollout-a.jsonl",
            &meta_line("codex-lb", quoted),
        );

        retag(home.path(), "codex-lb", "polyflare", true, "t0")
            .await
            .unwrap();

        let after = fs::read_to_string(&file).unwrap();
        let value: serde_json::Value = serde_json::from_str(after.trim()).unwrap();
        assert_eq!(value["payload"]["model_provider"], "polyflare");
        assert_eq!(
            value["payload"]["base_instructions"]["text"], quoted,
            "quoted prose must still name the original provider"
        );
    }

    #[tokio::test]
    async fn retagging_twice_is_a_no_op_the_second_time() {
        let home = tempfile::tempdir().unwrap();
        let file = write_session(home.path(), "rollout-a.jsonl", &meta_line("codex-lb", "x"));
        make_state_db(home.path(), &[("codex-lb", 2)]).await;

        retag(home.path(), "codex-lb", "polyflare", true, "t0")
            .await
            .unwrap();
        let once = fs::read_to_string(&file).unwrap();

        let second = retag(home.path(), "codex-lb", "polyflare", true, "t1")
            .await
            .unwrap();
        assert_eq!(second.jsonl_matched, 0);
        assert_eq!(second.sqlite_rows_matched, 0);
        assert_eq!(fs::read_to_string(&file).unwrap(), once);
    }

    /// An operator's own snapshot sits beside the live database and must not be rewritten — it is
    /// the thing they would restore FROM if this tool got it wrong.
    #[test]
    fn only_the_live_state_databases_are_selected() {
        assert!(is_state_db("state_5.sqlite"));
        assert!(is_state_db("state_12.sqlite"));
        assert!(!is_state_db("state_5.backup-20260725.sqlite"));
        assert!(!is_state_db("state_.sqlite"));
        assert!(!is_state_db("state_5.sqlite-wal"));
        assert!(!is_state_db("logs_2.sqlite"));
    }

    #[tokio::test]
    async fn a_snapshot_beside_the_live_database_is_left_alone() {
        let home = tempfile::tempdir().unwrap();
        make_state_db(home.path(), &[("codex-lb", 2)]).await;
        fs::copy(
            home.path().join("state_5.sqlite"),
            home.path().join("state_5.backup-20260725.sqlite"),
        )
        .unwrap();

        let report = retag(home.path(), "codex-lb", "polyflare", true, "t0")
            .await
            .unwrap();
        assert_eq!(report.sqlite_dbs_scanned, 1, "the snapshot is not a target");
        assert_eq!(report.sqlite_rows_updated, 2);
    }

    #[tokio::test]
    async fn unparseable_lines_are_preserved_verbatim() {
        let home = tempfile::tempdir().unwrap();
        let body = format!("not json at all\n{}\n", meta_line("codex-lb", "x"));
        let file = write_session(home.path(), "rollout-a.jsonl", &body);

        retag(home.path(), "codex-lb", "polyflare", true, "t0")
            .await
            .unwrap();

        let after = fs::read_to_string(&file).unwrap();
        assert!(after.starts_with("not json at all\n"));
        assert!(after.contains("\"model_provider\":\"polyflare\""));
        assert!(after.ends_with('\n'), "trailing newline preserved");
    }
}
