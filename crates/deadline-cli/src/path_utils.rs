/// Path summarization and sequence detection for CLI output formatting.
use std::collections::{BTreeMap, BTreeSet};

use deadline_lib::attachments::progress_tracker::human_readable_file_size;

// ---------------------------------------------------------------------------
// Numbered path detection and sequence grouping
// ---------------------------------------------------------------------------

/// A path that may contain a trailing number like `frame_001.png`.
#[derive(Debug)]
struct NumberedPath {
    path: String,
    /// The three parts: (prefix, `number_str`, extension). None if not numbered.
    parts: Option<(String, String, String)>,
    /// Grouping key — the path with the number replaced by `#`.
    grouping: String,
    /// Minimum padding width (e.g., 3 for "001"). -1 if not numbered.
    padding_min: i32,
    /// Maximum padding width (length of the number string). -1 if not numbered.
    padding_max: i32,
    /// The parsed number, if the path is numbered.
    number: Option<i64>,
}

impl NumberedPath {
    fn new(path: &str) -> Self {
        if let Some((prefix, num_str, ext)) = parse_numbered_path(path) {
            let padding_min = if num_str.starts_with('0') {
                num_str.len() as i32
            } else {
                1
            };
            let padding_max = num_str.len() as i32;
            let number = num_str.parse::<i64>().ok();
            let grouping = format!("{prefix}#.{ext}");
            Self {
                path: path.to_owned(),
                parts: Some((prefix, num_str, ext)),
                grouping,
                padding_min,
                padding_max,
                number,
            }
        } else {
            Self {
                grouping: path.to_owned(),
                path: path.to_owned(),
                parts: None,
                padding_min: -1,
                padding_max: -1,
                number: None,
            }
        }
    }
}

/// Parse a path into (prefix, `number_string`, extension) if it contains a trailing number.
fn parse_numbered_path(path: &str) -> Option<(String, String, String)> {
    let (base, ext) = if let Some(dot_pos) = path.rfind('.') {
        let after_dot = &path[dot_pos..];
        if after_dot.contains('/') || after_dot.contains('\\') {
            (path, "")
        } else {
            (&path[..dot_pos], &path[dot_pos + 1..])
        }
    } else {
        (path, "")
    };

    let digit_start = base
        .bytes()
        .rposition(|b| !b.is_ascii_digit())
        .map_or(0, |pos| pos + 1);

    if digit_start >= base.len() {
        return None;
    }

    let prefix = &base[..digit_start];
    let num_str = &base[digit_start..];

    if num_str.is_empty() {
        return None;
    }

    Some((prefix.to_owned(), num_str.to_owned(), ext.to_owned()))
}

/// Divide a group of numbered paths with the same grouping key into
/// sub-groups with consistent padding.
fn divide_numbered_path_group(group: &mut Vec<NumberedPath>) -> BTreeMap<String, BTreeSet<i64>> {
    let mut result = BTreeMap::new();

    while !group.is_empty() {
        if group.len() <= 2 {
            for np in group.drain(..) {
                result.insert(np.path.clone(), BTreeSet::new());
            }
            break;
        }

        let padding = group
            .iter()
            .map(|np| np.padding_min)
            .max()
            .expect("group is non-empty");
        let pattern = if padding > 1 {
            format!("%0{padding}d")
        } else {
            "%d".to_owned()
        };

        let (consistent, remaining): (Vec<_>, Vec<_>) =
            group.drain(..).partition(|np| np.padding_max >= padding);

        if !consistent.is_empty() {
            let parts = consistent[0].parts.as_ref().expect("value set above");
            let pattern_path = format!(
                "{}{}{}",
                parts.0,
                pattern,
                if parts.2.is_empty() {
                    String::new()
                } else {
                    format!(".{}", parts.2)
                }
            );
            let numbers: BTreeSet<i64> = consistent.iter().filter_map(|np| np.number).collect();
            result.insert(pattern_path, numbers);
        }

        *group = remaining;
    }

    result
}

/// A summary of a path or sequence of paths.
#[derive(Debug, Clone, PartialEq)]
pub struct PathSummary {
    /// The path, or a printf-style pattern if `index_set` is non-empty.
    pub path: String,
    /// The set of sequence indexes (empty if not a sequence).
    pub index_set: BTreeSet<i64>,
    /// Number of files represented.
    pub file_count: usize,
    /// Total size in bytes, if sizes were provided.
    pub total_size: Option<u64>,
}

impl PathSummary {
    fn new_file(path: String) -> Self {
        Self {
            path,
            index_set: BTreeSet::new(),
            file_count: 1,
            total_size: None,
        }
    }

    fn new_sequence(path: String, index_set: BTreeSet<i64>) -> Self {
        let file_count = index_set.len();
        Self {
            path,
            index_set,
            file_count,
            total_size: None,
        }
    }
}

/// Identify numbered sequences within a list of paths.
pub fn summarize_paths_by_sequence(paths: &[&str]) -> Vec<PathSummary> {
    if paths.is_empty() {
        return vec![];
    }

    let mut raw_groups: BTreeMap<String, Vec<NumberedPath>> = BTreeMap::new();
    for path in paths {
        let np = NumberedPath::new(path);
        raw_groups.entry(np.grouping.clone()).or_default().push(np);
    }

    let mut grouped: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    for (_key, mut group) in raw_groups {
        grouped.extend(divide_numbered_path_group(&mut group));
    }

    grouped
        .into_iter()
        .map(|(path, index_set)| {
            if index_set.is_empty() {
                PathSummary::new_file(path)
            } else {
                PathSummary::new_sequence(path, index_set)
            }
        })
        .collect()
}

/// Create a human-readable summary of a list of file paths.
///
/// Groups files by common directory prefix and limits output to
/// `max_entries` lines.
#[allow(clippy::implicit_hasher, reason = "only used with default HashMap")]
pub fn summarize_path_list(
    paths: &[&str],
    max_entries: usize,
    total_size_by_path: Option<&std::collections::HashMap<String, u64>>,
) -> String {
    if paths.is_empty() {
        return String::new();
    }

    let mut by_dir: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut dir_sizes: BTreeMap<String, u64> = BTreeMap::new();
    let mut file_sizes: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for path in paths {
        let p = std::path::Path::new(path);
        let dir = p
            .parent()
            .map(|d| d.to_string_lossy().to_string())
            .unwrap_or_default();
        let name = p
            .file_name()
            .map_or_else(|| path.to_string(), |n| n.to_string_lossy().to_string());
        by_dir.entry(dir.clone()).or_default().push(name.clone());
        if let Some(sizes) = total_size_by_path
            && let Some(&size) = sizes.get(*path)
        {
            *dir_sizes.entry(dir.clone()).or_default() += size;
            file_sizes.insert(format!("{dir}/{name}"), size);
        }
    }

    let mut dir_order: Vec<String> = by_dir.keys().cloned().collect();
    if total_size_by_path.is_some() {
        dir_order.sort_by(|a, b| {
            dir_sizes
                .get(b)
                .unwrap_or(&0)
                .cmp(dir_sizes.get(a).unwrap_or(&0))
        });
    } else {
        dir_order.sort_by(|a, b| {
            by_dir
                .get(b)
                .map_or(0, Vec::len)
                .cmp(&by_dir.get(a).map_or(0, Vec::len))
        });
    }

    let mut lines = Vec::new();

    for dir in &dir_order {
        let files = &by_dir[dir];
        let dir_display = if dir.is_empty() { "." } else { dir.as_str() };
        let total = files.len();
        let file_word = if total == 1 { "file" } else { "files" };

        let file_refs: Vec<&str> = files.iter().map(String::as_str).collect();
        let summaries = summarize_paths_by_sequence(&file_refs);

        let size_suffix = if let Some(&dir_size) = dir_sizes.get(dir) {
            format!(", {}", human_readable_file_size(dir_size))
        } else {
            String::new()
        };

        if summaries.len() == 1 && summaries[0].index_set.is_empty() && total == 1 {
            lines.push(format!(
                "{dir_display}/{} (1 file{size_suffix})\n",
                files[0]
            ));
        } else {
            lines.push(format!(
                "{dir_display}/ ({total} {file_word}{size_suffix}):\n"
            ));
            let show = summaries.len().min(max_entries.saturating_sub(lines.len()));
            for summary in &summaries[..show] {
                let child_size = file_sizes
                    .get(&format!("{dir}/{}", summary.path))
                    .map(|&s| format!(", {}", human_readable_file_size(s)))
                    .unwrap_or_default();
                if summary.index_set.is_empty() {
                    lines.push(format!("  {} (1 file{child_size})\n", summary.path));
                } else {
                    let seq = int_set_to_range_expr(&summary.index_set);
                    let count = summary.file_count;
                    let fw = if count == 1 { "file" } else { "files" };
                    lines.push(format!(
                        "  {} ({count} {fw}, sequence {seq})\n",
                        summary.path
                    ));
                }
            }
            if summaries.len() > show {
                let remaining: usize = summaries[show..].iter().map(|s| s.file_count).sum();
                lines.push(format!(
                    "  ... and {} more ({remaining} files)\n",
                    summaries.len() - show
                ));
            }
        }

        if lines.len() >= max_entries {
            break;
        }
    }

    lines.join("")
}

/// Format a set of integers as a range expression (e.g., {1,2,3,5} → "1-3,5").
pub fn int_set_to_range_expr(int_set: &BTreeSet<i64>) -> String {
    if int_set.is_empty() {
        return String::new();
    }

    let values: Vec<i64> = int_set.iter().copied().collect();
    let mut components = Vec::new();
    let mut start = values[0];
    let mut end = values[0];

    for &val in &values[1..] {
        if val == end + 1 {
            end = val;
        } else {
            if start == end {
                components.push(format!("{start}"));
            } else {
                components.push(format!("{start}-{end}"));
            }
            start = val;
            end = val;
        }
    }
    if start == end {
        components.push(format!("{start}"));
    } else {
        components.push(format!("{start}-{end}"));
    }

    components.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_of_numbered_files() {
        let paths = vec![
            "frame_001.png",
            "frame_002.png",
            "frame_003.png",
            "frame_004.png",
            "frame_005.png",
            "frame_006.png",
            "frame_007.png",
            "frame_008.png",
            "frame_009.png",
            "frame_010.png",
        ];
        let result = summarize_paths_by_sequence(&paths);
        assert_eq!(result.len(), 1, "should be grouped into one sequence");
        assert_eq!(result[0].file_count, 10);
        assert!(!result[0].index_set.is_empty());
        assert!(result[0].index_set.contains(&1));
        assert!(result[0].index_set.contains(&10));
    }

    #[test]
    fn mixed_numbered_and_non_numbered() {
        let paths = vec![
            "frame_001.png",
            "frame_002.png",
            "frame_003.png",
            "readme.txt",
        ];
        let result = summarize_paths_by_sequence(&paths);
        assert_eq!(result.len(), 2);
        let sequence = result.iter().find(|s| !s.index_set.is_empty()).unwrap();
        let individual = result.iter().find(|s| s.index_set.is_empty()).unwrap();
        assert_eq!(sequence.file_count, 3);
        assert_eq!(individual.path, "readme.txt");
        assert_eq!(individual.file_count, 1);
    }

    #[test]
    fn zero_padded_sequence_reflects_padding() {
        let paths = vec!["frame_001.png", "frame_002.png", "frame_010.png"];
        let result = summarize_paths_by_sequence(&paths);
        assert_eq!(result.len(), 1);
        assert!(
            result[0].path.contains("%03d"),
            "expected %03d padding in pattern, got: {}",
            result[0].path
        );
    }

    #[test]
    fn variable_width_number_sequence() {
        let paths = vec!["sequence_v1", "sequence_v2", "sequence_v907"];
        let result = summarize_paths_by_sequence(&paths);
        assert_eq!(result.len(), 1);
        assert!(
            result[0].path.contains("%d"),
            "expected %d in pattern, got: {}",
            result[0].path
        );
        assert!(result[0].index_set.contains(&907));
    }

    #[test]
    fn non_numbered_file_listed_individually() {
        let paths = vec!["no_number.txt"];
        let result = summarize_paths_by_sequence(&paths);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "no_number.txt");
        assert!(result[0].index_set.is_empty());
        assert_eq!(result[0].file_count, 1);
    }

    #[test]
    fn range_expr_consecutive() {
        let set: BTreeSet<i64> = (1..=5).collect();
        assert_eq!(int_set_to_range_expr(&set), "1-5");
    }

    #[test]
    fn range_expr_with_gaps() {
        let set: BTreeSet<i64> = [1, 2, 3, 5, 7, 8, 9, 10].into_iter().collect();
        assert_eq!(int_set_to_range_expr(&set), "1-3,5,7-10");
    }
}
