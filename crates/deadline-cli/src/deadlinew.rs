//! Windowless variant of the `deadline` CLI binary.
//!
//! On Windows, `#![windows_subsystem = "windows"]` suppresses the console
//! window. Behavior is otherwise identical to `deadline`.
#![windows_subsystem = "windows"]

fn main() {
    deadline_cli::cli_main();
}
