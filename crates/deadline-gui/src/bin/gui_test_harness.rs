//! GUI test harness binary.
//!
//! Calls `deadline-gui` public API directly without going through `deadline-cli`.
//! Used by xa11y tests in `crates/deadline-gui/tests/ui/` to verify GUI
//! behavior in isolation.
//!
//! Usage:
//!   gui-test-harness config
//!   gui-test-harness submit --params-json '{"job_bundle_dir": "/path", ...}'

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: gui-test-harness <config|submit> [--params-json '{{...}}']");
        std::process::exit(2);
    }

    match args[1].as_str() {
        "config" => {
            deadline_gui::show_config_dialog();
        }
        "submit" => {
            let params_json = extract_flag(&args, "--params-json").unwrap_or_default();
            let params: serde_json::Value =
                serde_json::from_str(&params_json).unwrap_or(serde_json::json!({}));

            let submit_params = deadline_gui::SubmitDialogParams {
                job_bundle_dir: params
                    .get("job_bundle_dir")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                browse: params
                    .get("browse")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                output: params
                    .get("output")
                    .and_then(|v| v.as_str())
                    .unwrap_or("verbose")
                    .to_string(),
                known_asset_paths: params
                    .get("known_asset_paths")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
                submitter_info: params.get("submitter_info").cloned(),
                job_parameters: params
                    .get("job_parameters")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default(),
                name: params
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            };

            let result = deadline_gui::show_submit_dialog(&submit_params);
            print!("{result}");
        }
        other => {
            eprintln!("Unknown command: {other}");
            std::process::exit(2);
        }
    }
}

fn extract_flag(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}
