// Common utilities for the deadline CLI.

use regex::Regex;
use std::sync::LazyLock;

static RE_REF_DEF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^\s*\[[^\]]+\]:\s*\S+.*$").unwrap());
static RE_INLINE_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").unwrap());
static RE_REF_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\[[^\]]*\]").unwrap());
static RE_BOLD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\*\*([^*]+)\*\*").unwrap());
static RE_BOLD_UNDER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"__([^_]+)__").unwrap());
static RE_ITALIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)(?P<pre>[^*\n])\*(?P<inner>[^*\n]+)\*").unwrap());
static RE_BLANK_LINES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\n{3,}").unwrap());

/// Strip markdown syntax for clean terminal display.
pub fn strip_markdown_for_terminal(text: &str) -> String {
    // Remove reference-style link definitions
    let text = RE_REF_DEF.replace_all(text, "");
    // Convert inline links [text](url) -> text (url)
    let text = RE_INLINE_LINK.replace_all(&text, "$1 ($2)");
    // Convert reference links [text][ref] -> text
    let text = RE_REF_LINK.replace_all(&text, "$1");
    // Strip bold
    let text = RE_BOLD.replace_all(&text, "$1");
    let text = RE_BOLD_UNDER.replace_all(&text, "$1");
    // Strip italic (not list markers at line start)
    let text = RE_ITALIC.replace_all(&text, "$pre$inner");
    // Clean up blank lines from removed definitions
    let text = RE_BLANK_LINES.replace_all(&text, "\n\n");

    text.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_markdown_inline_link_becomes_text_and_url() {
        assert_eq!(
            strip_markdown_for_terminal("[Deadline Cloud](https://example.com)"),
            "Deadline Cloud (https://example.com)"
        );
    }

    #[test]
    fn strip_markdown_reference_link_becomes_text() {
        let input = "[Deadline Cloud][dc]\n\n[dc]: https://example.com";
        let result = strip_markdown_for_terminal(input);
        assert!(result.contains("Deadline Cloud"));
        assert!(!result.contains("[dc]"));
        assert!(!result.contains("[dc]: https://example.com"));
    }

    #[test]
    fn strip_markdown_bold_markers_removed() {
        assert_eq!(strip_markdown_for_terminal("**bold text**"), "bold text");
    }

    #[test]
    fn strip_markdown_italic_markers_removed() {
        assert_eq!(
            strip_markdown_for_terminal("some *italic* words"),
            "some italic words"
        );
    }

    #[test]
    fn strip_markdown_plain_text_unchanged() {
        assert_eq!(
            strip_markdown_for_terminal("no markdown here"),
            "no markdown here"
        );
    }
}
