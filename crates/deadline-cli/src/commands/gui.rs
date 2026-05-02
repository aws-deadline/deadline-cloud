//! Shared infrastructure for GUI CLI commands that spawn a Python subprocess.
//!
//! Both `bundle gui-submit` and `config gui` use this module to find Python
//! and launch the Qt-based GUI entry point.

use std::path::PathBuf;
use std::process::Command;

use super::config::CliError;

/// Find a Python 3 interpreter.
///
/// Search order:
/// 1. `DEADLINE_PYTHON` env var (explicit override)
/// 2. `_internal/Python` relative to the binary (installer layout)
/// 3. `python3` on PATH
/// 4. `python` on PATH
pub(crate) fn find_python() -> Result<PathBuf, CliError> {
    // 1. Explicit env var
    if let Ok(p) = std::env::var("DEADLINE_PYTHON") {
        let path = PathBuf::from(&p);
        if path.exists() {
            return Ok(path);
        }
        return Err(CliError::Operation(format!(
            "DEADLINE_PYTHON is set to '{p}' but the file does not exist."
        )));
    }

    // 2. Bundled Python next to the binary (installer layout)
    if let Ok(exe) = std::env::current_exe()
        && let Some(exe_dir) = exe.parent() {
            let bundled = exe_dir.join("_internal").join("Python");
            if bundled.exists() {
                return Ok(bundled);
            }
        }

    // 3. System Python on PATH
    if let Ok(p) = which("python3") {
        return Ok(p);
    }
    if let Ok(p) = which("python") {
        return Ok(p);
    }

    Err(CliError::Operation(
        "Python 3.9+ is required for GUI commands. \
         Install Python and ensure it is on PATH, or set the DEADLINE_PYTHON \
         environment variable."
            .into(),
    ))
}

/// Launch the Python GUI entry point and return its stdout.
pub(crate) fn launch_gui(
    python: &PathBuf,
    command: &str,
    params_json: &str,
    install_gui: bool,
) -> Result<String, CliError> {
    let mut cmd = Command::new(python);
    cmd.args(["-m", "deadline.client.ui._gui_entry", command]);
    cmd.args(["--params-json", params_json]);
    if install_gui {
        cmd.arg("--install-gui");
    }

    let output = cmd.output().map_err(|e| {
        CliError::Operation(format!("Failed to launch Python GUI process: {e}"))
    })?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        let code = output.status.code().unwrap_or(1);
        let msg = if !stderr.is_empty() {
            stderr.trim().to_owned()
        } else if !stdout.is_empty() {
            stdout.trim().to_owned()
        } else {
            format!("Python GUI process exited with code {code}")
        };
        return Err(CliError::Operation(msg));
    }

    Ok(stdout)
}

/// Minimal `which` implementation — find an executable on PATH.
fn which(name: &str) -> Result<PathBuf, ()> {
    let path_var = std::env::var("PATH").map_err(|_| ())?;
    let sep = if cfg!(windows) { ';' } else { ':' };
    for dir in path_var.split(sep) {
        let candidate = PathBuf::from(dir).join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
        // Windows: try with .exe extension
        if cfg!(windows) {
            let with_exe = candidate.with_extension("exe");
            if with_exe.is_file() {
                return Ok(with_exe);
            }
        }
    }
    Err(())
}
