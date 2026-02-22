use crate::tracking;
use crate::utils::truncate;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::process::Command;

#[derive(Debug, Deserialize)]
struct SqlfluffViolation {
    code: String,
    #[serde(default)]
    fixes: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct SqlfluffFile {
    filepath: String,
    violations: Vec<SqlfluffViolation>,
}

pub fn run(args: &[String], verbose: u8) -> Result<()> {
    let timer = tracking::TimedExecution::start();

    // Detect subcommand: lint (default) or passthrough (fix, format, version, ...)
    let is_lint = args.is_empty()
        || args[0] == "lint"
        || (!args[0].starts_with('-')
            && ![
                "fix", "format", "version", "parse", "render", "dialect", "rules",
            ]
            .contains(&args[0].as_str()));

    let user_has_format = args
        .iter()
        .any(|a| a == "--format" || a.starts_with("--format="));

    let mut cmd = Command::new("sqlfluff");

    if is_lint {
        cmd.arg("lint");
        if !user_has_format {
            cmd.arg("--format").arg("json");
        }

        // Skip "lint" if it was explicitly the first arg
        let start_idx = if !args.is_empty() && args[0] == "lint" {
            1
        } else {
            0
        };
        for arg in &args[start_idx..] {
            cmd.arg(arg);
        }

        // Default to current directory if no path/file specified
        if args
            .iter()
            .skip(start_idx)
            .all(|a| a.starts_with('-') || a.contains('='))
        {
            cmd.arg(".");
        }
    } else {
        for arg in args {
            cmd.arg(arg);
        }
    }

    if verbose > 0 {
        eprintln!("Running: sqlfluff {}", args.join(" "));
    }

    let output = cmd
        .output()
        .context("Failed to run sqlfluff. Is it installed? Try: pip install sqlfluff")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let raw = format!("{}\n{}", stdout, stderr);

    let filtered = if is_lint && !user_has_format && !stdout.trim().is_empty() {
        filter_sqlfluff_lint_json(&stdout)
    } else {
        raw.trim().to_string()
    };

    println!("{}", filtered);

    timer.track(
        &format!("sqlfluff {}", args.join(" ")),
        &format!("rtk sqlfluff {}", args.join(" ")),
        &raw,
        &filtered,
    );

    // Preserve exit code for CI/CD
    if !output.status.success() {
        std::process::exit(output.status.code().unwrap_or(1));
    }

    Ok(())
}

/// Filter `sqlfluff lint --format json` output — group violations by rule and file.
pub fn filter_sqlfluff_lint_json(output: &str) -> String {
    let files: Result<Vec<SqlfluffFile>, _> = serde_json::from_str(output);

    let files = match files {
        Ok(f) => f,
        Err(e) => {
            return format!(
                "SQLFluff lint (JSON parse failed: {})\n{}",
                e,
                truncate(output, 500)
            );
        }
    };

    let files_with_violations: Vec<&SqlfluffFile> =
        files.iter().filter(|f| !f.violations.is_empty()).collect();

    let total_violations: usize = files_with_violations
        .iter()
        .map(|f| f.violations.len())
        .sum();

    if total_violations == 0 {
        return "✓ SQLFluff: No violations found".to_string();
    }

    let total_files = files_with_violations.len();
    let fixable_count: usize = files_with_violations
        .iter()
        .flat_map(|f| &f.violations)
        .filter(|v| !v.fixes.is_empty())
        .count();

    // Group violations by rule code
    let mut by_rule: HashMap<String, usize> = HashMap::new();
    for file in &files_with_violations {
        for v in &file.violations {
            *by_rule.entry(v.code.clone()).or_insert(0) += 1;
        }
    }

    // Sort files by violation count descending
    let mut file_counts: Vec<(&str, usize)> = files_with_violations
        .iter()
        .map(|f| (f.filepath.as_str(), f.violations.len()))
        .collect();
    file_counts.sort_by(|a, b| b.1.cmp(&a.1));

    // Build compact output
    let mut result = String::new();
    result.push_str(&format!(
        "SQLFluff: {} violations in {} files",
        total_violations, total_files
    ));
    if fixable_count > 0 {
        result.push_str(&format!(" ({} fixable)", fixable_count));
    }
    result.push('\n');
    result.push_str("═══════════════════════════════════════\n");

    // Top rules
    let mut rule_counts: Vec<_> = by_rule.iter().collect();
    rule_counts.sort_by(|a, b| b.1.cmp(a.1));

    result.push_str("Top rules:\n");
    for (rule, count) in rule_counts.iter().take(10) {
        result.push_str(&format!("  {} ({}x)\n", rule, count));
    }
    result.push('\n');

    // Top files with per-file rule breakdown
    result.push_str("Top files:\n");
    for (filepath, count) in file_counts.iter().take(10) {
        result.push_str(&format!(
            "  {} ({} violations)\n",
            compact_path(filepath),
            count
        ));

        let file = files_with_violations
            .iter()
            .find(|f| f.filepath.as_str() == *filepath)
            .expect("file must exist in filtered list");

        let mut file_rules: HashMap<String, usize> = HashMap::new();
        for v in &file.violations {
            *file_rules.entry(v.code.clone()).or_insert(0) += 1;
        }
        let mut file_rule_counts: Vec<_> = file_rules.iter().collect();
        file_rule_counts.sort_by(|a, b| b.1.cmp(a.1));

        for (rule, cnt) in file_rule_counts.iter().take(3) {
            result.push_str(&format!("    {} ({})\n", rule, cnt));
        }
    }

    if file_counts.len() > 10 {
        result.push_str(&format!("\n... +{} more files\n", file_counts.len() - 10));
    }

    if fixable_count > 0 {
        result.push_str(&format!(
            "\n💡 Run `sqlfluff fix` to auto-fix {} violations\n",
            fixable_count
        ));
    }

    result.trim().to_string()
}

/// Compact file path for dbt/SQL projects, preserving meaningful directory prefixes.
fn compact_path(path: &str) -> String {
    let path = path.replace('\\', "/");

    // Absolute paths: extract from known dbt directory roots
    for prefix in &[
        "models",
        "tests",
        "macros",
        "seeds",
        "snapshots",
        "analyses",
    ] {
        let needle = format!("/{}/", prefix);
        if let Some(pos) = path.rfind(&needle) {
            return format!("{}/{}", prefix, &path[pos + needle.len()..]);
        }
    }

    // Relative paths already starting with a known dbt root — keep as-is
    for prefix in &[
        "models/",
        "tests/",
        "macros/",
        "seeds/",
        "snapshots/",
        "analyses/",
    ] {
        if path.starts_with(prefix) {
            return path;
        }
    }

    // Fall back to just the filename
    if let Some(pos) = path.rfind('/') {
        path[pos + 1..].to_string()
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_tokens(text: &str) -> usize {
        text.split_whitespace().count()
    }

    // ── happy path ──────────────────────────────────────────────────────────

    #[test]
    fn test_filter_no_violations_empty_array() {
        let result = filter_sqlfluff_lint_json("[]");
        assert!(result.contains("✓ SQLFluff"), "expected success tick");
        assert!(result.contains("No violations found"));
    }

    #[test]
    fn test_filter_no_violations_all_clean_files() {
        let input = r#"[{"filepath": "models/staging/stg_orders.sql", "violations": []}]"#;
        let result = filter_sqlfluff_lint_json(input);
        assert!(result.contains("✓ SQLFluff"));
        assert!(result.contains("No violations found"));
    }

    #[test]
    fn test_filter_with_violations_counts() {
        let input = r#"[
  {
    "filepath": "models/staging/stg_customers.sql",
    "violations": [
      {"line_no": 1, "line_pos": 1, "code": "LT09", "description": "Select wildcard used.", "fixes": []},
      {"line_no": 5, "line_pos": 1, "code": "LT12", "description": "Trailing newline missing.", "fixes": [{"edit_type": "create_after"}]}
    ]
  },
  {
    "filepath": "models/intermediate/int_orders.sql",
    "violations": [
      {"line_no": 3, "line_pos": 1, "code": "LT09", "description": "Select wildcard used.", "fixes": []}
    ]
  }
]"#;
        let result = filter_sqlfluff_lint_json(input);
        assert!(result.contains("3 violations"), "should show 3 violations");
        assert!(result.contains("2 files"), "should show 2 files");
        assert!(result.contains("1 fixable"), "should count 1 fixable");
        assert!(result.contains("LT09"), "should list top rule");
        assert!(result.contains("LT12"), "should list second rule");
        assert!(result.contains("stg_customers.sql"), "should show top file");
        assert!(result.contains("int_orders.sql"), "should show second file");
    }

    #[test]
    fn test_filter_fixable_hint_shown() {
        let input = r#"[
  {
    "filepath": "models/staging/stg_orders.sql",
    "violations": [
      {"line_no": 1, "line_pos": 1, "code": "LT12", "description": "Trailing newline.", "fixes": [{"edit_type": "create_after"}]}
    ]
  }
]"#;
        let result = filter_sqlfluff_lint_json(input);
        assert!(
            result.contains("sqlfluff fix"),
            "should suggest fix command"
        );
    }

    #[test]
    fn test_filter_no_fixable_no_hint() {
        let input = r#"[
  {
    "filepath": "models/staging/stg_orders.sql",
    "violations": [
      {"line_no": 1, "line_pos": 1, "code": "LT09", "description": "Select wildcard.", "fixes": []}
    ]
  }
]"#;
        let result = filter_sqlfluff_lint_json(input);
        assert!(
            !result.contains("sqlfluff fix"),
            "should NOT show fix hint when nothing is fixable"
        );
    }

    // ── real sqlfluff JSON schema (regression: start_line_no not line_no) ────

    #[test]
    fn test_filter_real_sqlfluff_json_schema() {
        // Real output from `sqlfluff lint --format json` (v3+).
        // Fields are start_line_no/start_line_pos, NOT line_no/line_pos.
        // statistics and timings fields at file level must be tolerated.
        let input = r#"[{"filepath": "models/intermediate/mariadb/int_mariadb_announces.sql", "violations": [{"start_line_no": 183, "start_line_pos": 9, "code": "RF01", "description": "Reference 'level_id' refers to table/view not found in the FROM clause or found in ancestor statement.", "name": "references.from", "warning": false, "fixes": [], "start_file_pos": 4449, "end_line_no": 183, "end_line_pos": 17, "end_file_pos": 4457}, {"start_line_no": 185, "start_line_pos": 9, "code": "RF01", "description": "Reference 'level_name' refers to table/view not found in the FROM clause or found in ancestor statement.", "name": "references.from", "warning": false, "fixes": [], "start_file_pos": 4486, "end_line_no": 185, "end_line_pos": 19, "end_file_pos": 4496}], "statistics": {"source_chars": 9356, "templated_chars": 9507}, "timings": {"templating": 0.93}}]"#;
        let result = filter_sqlfluff_lint_json(input);
        assert!(
            !result.contains("JSON parse failed"),
            "should parse real sqlfluff JSON without error"
        );
        assert!(result.contains("2 violations"), "should count 2 violations");
        assert!(result.contains("RF01"), "should show rule code");
        assert!(
            result.contains("int_mariadb_announces.sql"),
            "should show filename"
        );
    }

    // ── error handling ───────────────────────────────────────────────────────

    #[test]
    fn test_filter_json_parse_error() {
        let result = filter_sqlfluff_lint_json("not valid json");
        assert!(
            result.contains("JSON parse failed"),
            "should report parse failure"
        );
    }

    // ── compact_path ─────────────────────────────────────────────────────────

    #[test]
    fn test_compact_path_absolute_models() {
        assert_eq!(
            compact_path("/Users/foo/project/models/staging/stg_orders.sql"),
            "models/staging/stg_orders.sql"
        );
    }

    #[test]
    fn test_compact_path_absolute_macros() {
        assert_eq!(
            compact_path("/home/user/project/macros/utils.sql"),
            "macros/utils.sql"
        );
    }

    #[test]
    fn test_compact_path_relative_models() {
        assert_eq!(
            compact_path("models/staging/stg_orders.sql"),
            "models/staging/stg_orders.sql"
        );
    }

    #[test]
    fn test_compact_path_no_known_prefix() {
        assert_eq!(compact_path("some/deep/path/file.sql"), "file.sql");
    }

    #[test]
    fn test_compact_path_filename_only() {
        assert_eq!(compact_path("file.sql"), "file.sql");
    }

    // ── token savings ─────────────────────────────────────────────────────────

    #[test]
    fn test_token_savings_at_least_60_percent() {
        // Realistic sqlfluff JSON output with 10 violations across 3 files
        let input = r#"[
  {
    "filepath": "models/staging/stg_customers.sql",
    "violations": [
      {"line_no": 1, "line_pos": 1, "code": "LT09", "description": "Select wildcard (*) used in select statement. Use explicit column names instead.", "fixes": [], "start_file_pos": 0, "end_file_pos": 6},
      {"line_no": 5, "line_pos": 1, "code": "LT12", "description": "Files must end with a single trailing newline.", "fixes": [{"edit_type": "create_after", "content": "\n"}], "start_file_pos": 100, "end_file_pos": 100},
      {"line_no": 10, "line_pos": 5, "code": "AM04", "description": "Query produces an unknown number of result columns. Specify a column list in the SELECT clause.", "fixes": [], "start_file_pos": 200, "end_file_pos": 220},
      {"line_no": 15, "line_pos": 1, "code": "LT09", "description": "Select wildcard (*) used in select statement. Use explicit column names instead.", "fixes": [], "start_file_pos": 300, "end_file_pos": 306},
      {"line_no": 20, "line_pos": 1, "code": "RF04", "description": "Column name is a reserved word in one or more dialects.", "fixes": [], "start_file_pos": 400, "end_file_pos": 410}
    ]
  },
  {
    "filepath": "models/intermediate/int_order_items.sql",
    "violations": [
      {"line_no": 2, "line_pos": 1, "code": "LT09", "description": "Select wildcard (*) used in select statement. Use explicit column names instead.", "fixes": [], "start_file_pos": 0, "end_file_pos": 6},
      {"line_no": 8, "line_pos": 1, "code": "AM04", "description": "Query produces an unknown number of result columns. Specify a column list in the SELECT clause.", "fixes": [], "start_file_pos": 100, "end_file_pos": 120},
      {"line_no": 12, "line_pos": 1, "code": "LT12", "description": "Files must end with a single trailing newline.", "fixes": [{"edit_type": "create_after", "content": "\n"}], "start_file_pos": 200, "end_file_pos": 200}
    ]
  },
  {
    "filepath": "models/marts/fct_orders.sql",
    "violations": [
      {"line_no": 1, "line_pos": 1, "code": "LT09", "description": "Select wildcard (*) used in select statement. Use explicit column names instead.", "fixes": [], "start_file_pos": 0, "end_file_pos": 6},
      {"line_no": 3, "line_pos": 1, "code": "RF04", "description": "Column name is a reserved word in one or more dialects.", "fixes": [], "start_file_pos": 100, "end_file_pos": 110}
    ]
  }
]"#;
        let result = filter_sqlfluff_lint_json(input);
        let input_tokens = count_tokens(input);
        let output_tokens = count_tokens(&result);
        let savings = 100.0 - (output_tokens as f64 / input_tokens as f64 * 100.0);
        assert!(
            savings >= 60.0,
            "SQLFluff filter: expected ≥60% savings, got {:.1}% (in={} out={})",
            savings,
            input_tokens,
            output_tokens
        );
    }
}
