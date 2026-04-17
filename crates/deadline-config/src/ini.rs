// Minimal INI parser/writer for the Deadline Cloud config file.
//
// Uses BTreeMap for deterministic (sorted) output when writing back to disk.

use std::collections::BTreeMap;
use std::fmt;

/// An INI config: a map of section names → (key → value).
/// Section names are case-sensitive. Keys are case-insensitive
/// (lowercased on storage and lookup, matching Python's ConfigParser).
#[derive(Debug, Clone, Default)]
pub struct IniConfig {
    sections: BTreeMap<String, BTreeMap<String, String>>,
}

impl IniConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse INI text. Returns an error if a key appears before any section header.
    pub fn parse(text: &str) -> Result<Self, IniParseError> {
        let mut sections = BTreeMap::new();
        let mut current_section: Option<String> = None;

        for (line_num, raw_line) in text.lines().enumerate() {
            let line = raw_line.trim();

            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }

            if line.starts_with('[') {
                if let Some(end) = line.find(']') {
                    let name = line[1..end].to_string();
                    sections.entry(name.clone()).or_insert_with(BTreeMap::new);
                    current_section = Some(name);
                } else {
                    return Err(IniParseError {
                        line: line_num + 1,
                        message: "unclosed section header".into(),
                    });
                }
                continue;
            }

            if let Some(eq_pos) = line.find('=') {
                let key = line[..eq_pos].trim().to_lowercase();
                let value = line[eq_pos + 1..].trim().to_string();

                match &current_section {
                    Some(section) => {
                        sections.get_mut(section).unwrap().insert(key, value);
                    }
                    None => {
                        return Err(IniParseError {
                            line: line_num + 1,
                            message: "key-value pair before any section header".into(),
                        });
                    }
                }
            }
        }

        Ok(Self { sections })
    }

    /// Get a value from a section. Returns `None` if section or key doesn't exist.
    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(section)
            .and_then(|s| s.get(&key.to_lowercase()))
            .map(|v| v.as_str())
    }

    /// Set a value in a section, creating the section if needed.
    pub fn set(&mut self, section: &str, key: &str, value: &str) {
        self.sections
            .entry(section.to_string())
            .or_default()
            .insert(key.to_lowercase(), value.to_string());
    }

    /// Check if a section exists.
    pub fn has_section(&self, section: &str) -> bool {
        self.sections.contains_key(section)
    }

    /// Iterate over all section names.
    pub fn sections(&self) -> impl Iterator<Item = &str> {
        self.sections.keys().map(|s| s.as_str())
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

    // ── AUDIT-004: INI key case insensitivity ───────────────────────

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
}
