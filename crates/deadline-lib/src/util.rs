//! Shared utilities used across multiple modules.

use crate::api::errors::DeadlineError;
use std::path::{Component, Path, PathBuf};

/// Shorthand for constructing a `DeadlineError::OperationError`.
pub fn op_err(msg: String) -> DeadlineError {
    DeadlineError::OperationError(msg)
}

/// Lexically normalize a path: resolve `.` and `..` components without
/// touching the filesystem.
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut parts: Vec<Component> = Vec::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(parts.last(), Some(Component::Normal(_))) {
                    parts.pop();
                } else {
                    parts.push(c);
                }
            }
            _ => parts.push(c),
        }
    }
    if parts.is_empty() {
        PathBuf::from(".")
    } else {
        parts.iter().collect()
    }
}
