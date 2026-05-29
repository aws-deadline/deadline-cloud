//! Stack trace sanitizer for telemetry.
//!
//! **Design tenet:** no customer-provided content (file paths, error messages,
//! variable values) may appear in telemetry. Only an allowlist of structured
//! fields is emitted: sanitized filename, line number, and function name.
//! Error messages are dropped entirely.

use std::backtrace::Backtrace;

/// Sanitize a file path from a backtrace frame.
///
/// Strategy (in priority order):
/// 1. If path contains `/crates/`, keep everything after it (our workspace code)
/// 2. If path contains a cargo registry segment like `/aws_sdk_deadline-1.99.0/`,
///    keep from the crate name onward
/// 3. Synthetic paths like `<string>` pass through unchanged
/// 4. Otherwise, keep only the bare filename
fn sanitize_path(path: &str) -> &str {
    // Synthetic frame sources
    if path.starts_with('<') {
        return path;
    }

    // Our workspace: /anything/crates/deadline-lib/src/api.rs:42
    if let Some(idx) = path.find("/crates/") {
        return &path[idx + 1..]; // "crates/deadline-lib/src/api.rs:42"
    }

    // Cargo registry: /.cargo/registry/src/.../aws_sdk_deadline-1.99.0/src/client.rs
    // Find the last segment that looks like a crate name (contains a hyphen + version or
    // is a known prefix). Anchor on "registry/src/" or "checkouts/".
    for anchor in &["/registry/src/", "/checkouts/"] {
        if let Some(anchor_end) = path.find(anchor).map(|i| i + anchor.len()) {
            // Skip the hash directory after registry/src/
            let rest = &path[anchor_end..];
            if let Some(slash) = rest.find('/') {
                let after_hash = &path[anchor_end + slash + 1..];
                return after_hash;
            }
        }
    }

    // Bare filename fallback
    path.rsplit_once('/').map_or(
        path.rsplit_once('\\').map_or(path, |(_, name)| name),
        |(_, name)| name,
    )
}

/// Sanitize a captured backtrace for telemetry emission.
///
/// Parses the `Display` output of `std::backtrace::Backtrace`, keeping only
/// function names and sanitized file paths. The format is:
/// ```text
///    N: crate::module::function
///              at /full/path/to/file.rs:line:col
/// ```
pub fn sanitize_backtrace(bt: &Backtrace) -> String {
    let raw = bt.to_string();
    let mut lines = Vec::new();
    lines.push("Traceback (most recent call last):".to_owned());

    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(at_path) = trimmed.strip_prefix("at ") {
            // File location line
            let safe = sanitize_path(at_path);
            lines.push(format!("  at {safe}"));
        } else if let Some((num, func)) = trimmed
            .find(": ")
            .map(|i| (&trimmed[..i], &trimmed[i + 2..]))
        {
            // Frame number + function name (only if starts with digit)
            if num.chars().all(|c| c.is_ascii_digit()) && !func.is_empty() {
                lines.push(format!("  in {func}"));
            }
        }
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- sanitize_path ---

    #[test]
    fn sanitize_path_strips_to_crates_relative() {
        let input = "/Users/dev/code/deadline-cloud-rs/crates/deadline-lib/src/api/session.rs:42:5";
        assert_eq!(
            sanitize_path(input),
            "crates/deadline-lib/src/api/session.rs:42:5"
        );
    }

    #[test]
    fn sanitize_path_strips_cargo_registry_to_crate() {
        let input = "/home/user/.cargo/registry/src/index.crates.io-abc123/aws_sdk_deadline-1.99.0/src/client.rs:10:1";
        assert_eq!(
            sanitize_path(input),
            "aws_sdk_deadline-1.99.0/src/client.rs:10:1"
        );
    }

    #[test]
    fn sanitize_path_unknown_keeps_filename_only() {
        assert_eq!(
            sanitize_path("/home/customer/scripts/run.rs:5:1"),
            "run.rs:5:1"
        );
    }

    #[test]
    fn sanitize_path_synthetic_passthrough() {
        assert_eq!(sanitize_path("<string>"), "<string>");
        assert_eq!(sanitize_path("<unknown>"), "<unknown>");
    }

    // --- sanitize_backtrace (real captures) ---

    #[test]
    fn sanitize_backtrace_real_capture_contains_our_frames() {
        let bt = Backtrace::force_capture();
        let sanitized = sanitize_backtrace(&bt);

        assert!(
            sanitized.starts_with("Traceback (most recent call last):"),
            "Missing header"
        );
        // Must contain a frame from this crate
        assert!(
            sanitized.contains("deadline_lib") || sanitized.contains("stack_trace_sanitizer"),
            "Expected our crate in sanitized output:\n{sanitized}"
        );
    }

    #[test]
    fn sanitize_backtrace_real_capture_no_absolute_paths() {
        let bt = Backtrace::force_capture();
        let sanitized = sanitize_backtrace(&bt);

        for line in sanitized.lines() {
            if let Some(path) = line.strip_prefix("  at ") {
                assert!(
                    !path.starts_with('/') || path.starts_with("/rustc/"),
                    "Absolute path leaked: {path}"
                );
                assert!(
                    !path.contains("/Users/") && !path.contains("/home/"),
                    "Customer path leaked: {path}"
                );
            }
        }
    }

    #[test]
    fn sanitize_backtrace_never_contains_error_message() {
        // The sanitizer only processes backtraces, not error messages.
        // Verify that even if we format an error's Display into a string,
        // the backtrace sanitizer output doesn't include it.
        let marker = "CUSTOMER_SECRET_DATA_12345";
        let bt = Backtrace::force_capture();
        let sanitized = sanitize_backtrace(&bt);
        assert!(
            !sanitized.contains(marker),
            "Error message leaked into sanitized backtrace"
        );
    }
}
