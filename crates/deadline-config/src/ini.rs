// Minimal INI parser/writer for the Deadline Cloud config file.
//
// Uses IndexMap for insertion-order-preserving output when writing back
// to disk, matching Python's ConfigParser behavior.

use indexmap::IndexMap;
use std::fmt;

/// An INI config: a map of section names → (key → value).
/// Section names are case-sensitive. Keys are case-insensitive
/// (lowercased on storage and lookup, matching Python's `ConfigParser`).
/// Insertion order is preserved for both sections and keys.
#[derive(Debug, Clone, Default)]
pub struct IniConfig {
    sections: IndexMap<String, IndexMap<String, String>>,
}

impl IniConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse INI text. Returns an error if a key appears before any section header.
    /// Supports `=` and `:` as key-value delimiters (matching Python's `ConfigParser`).
    /// Supports multiline values via leading whitespace continuation lines.
    pub fn parse(text: &str) -> Result<Self, IniParseError> {
        let mut sections: IndexMap<String, IndexMap<String, String>> = IndexMap::new();
        let mut current_section: Option<String> = None;
        // Track the last key inserted for multiline continuation
        let mut last_key: Option<(String, String)> = None; // (section, key)

        for (line_num, raw_line) in text.lines().enumerate() {
            // Check for continuation line (leading whitespace) before trimming
            if let Some((ref sec, ref key)) = last_key
                && !raw_line.is_empty()
                    && (raw_line.starts_with(' ') || raw_line.starts_with('\t'))
                {
                    let continuation = raw_line.trim();
                    if !continuation.is_empty() {
                        if let Some(section_map) = sections.get_mut(sec)
                            && let Some(val) = section_map.get_mut(key) {
                                val.push('\n');
                                val.push_str(continuation);
                            }
                        continue;
                    }
                }

            let line = raw_line.trim();

            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                last_key = None;
                continue;
            }

            if line.starts_with('[') {
                if let Some(end) = line.find(']') {
                    let name = line[1..end].to_string();
                    sections.entry(name.clone()).or_default();
                    current_section = Some(name);
                    last_key = None;
                } else {
                    return Err(IniParseError {
                        line: line_num + 1,
                        message: "unclosed section header".into(),
                    });
                }
                continue;
            }

            // Find delimiter: `=` or `:` (prefer `=` if both present, matching ConfigParser)
            let delim_pos = line.find('=').or_else(|| line.find(':'));

            if let Some(pos) = delim_pos {
                let key = line[..pos].trim().to_lowercase();
                let value = line[pos + 1..].trim().to_owned();

                match &current_section {
                    Some(section) => {
                        sections.get_mut(section).unwrap().insert(key.clone(), value);
                        last_key = Some((section.clone(), key));
                    }
                    None => {
                        return Err(IniParseError {
                            line: line_num + 1,
                            message: "key-value pair before any section header".into(),
                        });
                    }
                }
            } else {
                last_key = None;
            }
        }

        Ok(Self { sections })
    }

    /// Get a value from a section. Returns `None` if section or key doesn't exist.
    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(section)
            .and_then(|s| s.get(&key.to_lowercase()))
            .map(String::as_str)
    }

    /// Set a value in a section, creating the section if needed.
    /// Existing keys update in-place; new keys append to end.
    pub fn set(&mut self, section: &str, key: &str, value: &str) {
        self.sections
            .entry(section.to_owned())
            .or_default()
            .insert(key.to_lowercase(), value.to_owned());
    }

    /// Check if a section exists.
    pub fn has_section(&self, section: &str) -> bool {
        self.sections.contains_key(section)
    }

    /// Iterate over all section names.
    pub fn sections(&self) -> impl Iterator<Item = &str> {
        self.sections.keys().map(String::as_str)
    }

    /// Iterate over all sections with their key-value maps.
    pub fn iter_sections(&self) -> &IndexMap<String, IndexMap<String, String>> {
        &self.sections
    }
}

/// Serialize the config back to INI format.
impl fmt::Display for IniConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (section, keys) in &self.sections {
            if !first {
                writeln!(f)?;
            }
            first = false;
            writeln!(f, "[{section}]")?;
            for (key, value) in keys {
                writeln!(f, "{key} = {value}")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct IniParseError {
    pub line: usize,
    pub message: String,
}

impl fmt::Display for IniParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for IniParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_ini() {
        let ini = IniConfig::parse("[section]\nkey = value\n").unwrap();
        assert_eq!(ini.get("section", "key"), Some("value"));
    }

    #[test]
    fn parse_multiple_sections() {
        let text = "[a]\nx = 1\n[b]\ny = 2\n";
        let ini = IniConfig::parse(text).unwrap();
        assert_eq!(ini.get("a", "x"), Some("1"));
        assert_eq!(ini.get("b", "y"), Some("2"));
    }

    #[test]
    fn parse_section_with_spaces() {
        let ini = IniConfig::parse("[profile-myprofile defaults]\nfarm_id = farm-123\n").unwrap();
        assert_eq!(
            ini.get("profile-myprofile defaults", "farm_id"),
            Some("farm-123")
        );
    }

    #[test]
    fn parse_error_key_before_section() {
        let result = IniConfig::parse("key = value\n");
        assert!(result.is_err());
    }

    #[test]
    fn roundtrip() {
        let mut ini = IniConfig::new();
        ini.set("defaults", "aws_profile_name", "(default)");
        ini.set("settings", "log_level", "WARNING");
        let text = ini.to_string();
        let parsed = IniConfig::parse(&text).unwrap();
        assert_eq!(parsed.get("defaults", "aws_profile_name"), Some("(default)"));
        assert_eq!(parsed.get("settings", "log_level"), Some("WARNING"));
    }

    #[test]
    fn missing_section_returns_none() {
        let ini = IniConfig::new();
        assert_eq!(ini.get("nope", "key"), None);
    }

    #[test]
    fn missing_key_returns_none() {
        let ini = IniConfig::parse("[s]\na = b\n").unwrap();
        assert_eq!(ini.get("s", "nope"), None);
    }

    // ── INI key case insensitivity ───────────────────────

    #[test]
    fn parse_mixed_case_keys_lowercased() {
        let ini = IniConfig::parse("[section]\nFarm_Id = farm-123\n").unwrap();
        assert_eq!(ini.get("section", "farm_id"), Some("farm-123"));
    }

    #[test]
    fn set_mixed_case_key_stored_lowercase() {
        let mut ini = IniConfig::new();
        ini.set("defaults", "AWS_Profile_Name", "myprofile");
        assert_eq!(ini.get("defaults", "aws_profile_name"), Some("myprofile"));
    }

    #[test]
    fn get_case_insensitive_lookup() {
        let ini = IniConfig::parse("[s]\nkey = val\n").unwrap();
        assert_eq!(ini.get("s", "KEY"), Some("val"));
        assert_eq!(ini.get("s", "Key"), Some("val"));
    }

    // ── INI colon delimiter support ──────────────────────

    #[test]
    fn parse_colon_delimiter_accepted() {
        let ini = IniConfig::parse("[section]\nfarm_id : farm-123\n").unwrap();
        assert_eq!(ini.get("section", "farm_id"), Some("farm-123"));
    }

    #[test]
    fn parse_colon_and_equals_mixed() {
        let text = "[defaults]\nfarm_id = farm-abc\nqueue_id : queue-xyz\n";
        let ini = IniConfig::parse(text).unwrap();
        assert_eq!(ini.get("defaults", "farm_id"), Some("farm-abc"));
        assert_eq!(ini.get("defaults", "queue_id"), Some("queue-xyz"));
    }

    // ── INI section/key ordering preserved ───────────────

    #[test]
    fn roundtrip_preserves_section_order() {
        let text = "[zebra]\nz = 1\n\n[alpha]\na = 2\n\n[middle]\nm = 3\n";
        let ini = IniConfig::parse(text).unwrap();
        let output = ini.to_string();
        let section_positions: Vec<usize> = ["[zebra]", "[alpha]", "[middle]"]
            .iter()
            .map(|s| output.find(s).unwrap_or_else(|| panic!("{s} not found")))
            .collect();
        assert!(section_positions[0] < section_positions[1], "zebra should come before alpha");
        assert!(section_positions[1] < section_positions[2], "alpha should come before middle");
    }

    #[test]
    fn roundtrip_preserves_key_order() {
        let text = "[section]\nzebra = 1\nalpha = 2\nmiddle = 3\n";
        let ini = IniConfig::parse(text).unwrap();
        let output = ini.to_string();
        let key_positions: Vec<usize> = ["zebra = 1", "alpha = 2", "middle = 3"]
            .iter()
            .map(|s| output.find(s).unwrap_or_else(|| panic!("{s} not found")))
            .collect();
        assert!(key_positions[0] < key_positions[1], "zebra should come before alpha");
        assert!(key_positions[1] < key_positions[2], "alpha should come before middle");
    }

    #[test]
    fn set_existing_key_preserves_position() {
        let text = "[section]\nzebra = 1\nmiddle = 2\nalpha = 3\n";
        let mut ini = IniConfig::parse(text).unwrap();
        ini.set("section", "middle", "updated");
        let output = ini.to_string();
        let positions: Vec<usize> = ["zebra = 1", "middle = updated", "alpha = 3"]
            .iter()
            .map(|s| output.find(s).unwrap_or_else(|| panic!("{s} not found")))
            .collect();
        assert!(positions[0] < positions[1] && positions[1] < positions[2]);
    }

    #[test]
    fn set_new_key_appends_to_end() {
        let text = "[section]\nzebra = 1\n";
        let mut ini = IniConfig::parse(text).unwrap();
        ini.set("section", "alpha", "2");
        let output = ini.to_string();
        let zebra_pos = output.find("zebra = 1").unwrap();
        let alpha_pos = output.find("alpha = 2").unwrap();
        assert!(zebra_pos < alpha_pos, "new key should append after existing");
    }

    // ── INI multiline value support ──────────────────────

    #[test]
    fn parse_multiline_continuation() {
        let text = "[section]\nkey = line1\n  line2\n  line3\n";
        let ini = IniConfig::parse(text).unwrap();
        let val = ini.get("section", "key").unwrap();
        assert!(val.contains("line1"), "should contain first line");
        assert!(val.contains("line2"), "should contain continuation line2");
        assert!(val.contains("line3"), "should contain continuation line3");
    }

    #[test]
    fn parse_multiline_with_tab_continuation() {
        let text = "[section]\nkey = line1\n\tline2\n";
        let ini = IniConfig::parse(text).unwrap();
        let val = ini.get("section", "key").unwrap();
        assert!(val.contains("line1"));
        assert!(val.contains("line2"));
    }

    #[test]
    fn parse_multiline_stops_at_next_key() {
        let text = "[section]\nfirst = a\n  b\nsecond = c\n";
        let ini = IniConfig::parse(text).unwrap();
        let first = ini.get("section", "first").unwrap();
        assert!(first.contains('a') && first.contains('b'));
        assert_eq!(ini.get("section", "second"), Some("c"));
    }
}
