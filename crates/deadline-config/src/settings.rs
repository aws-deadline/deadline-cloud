// Settings definitions for AWS Deadline Cloud configuration.
//
// Each setting has a name (e.g. "defaults.farm_id"), a default value,
// an optional dependency on another setting, and an optional section_format
// that controls how its value is embedded into INI section names.

/// Metadata for a single configuration setting.
pub struct SettingDef {
    pub default: &'static str,
    /// The setting this one depends on (controls INI section nesting).
    pub depend: Option<&'static str>,
    /// Format string for embedding this setting's value into section names.
    /// e.g. `"profile-{}"` means value `X` becomes `profile-X`.
    pub section_format: Option<&'static str>,
    pub description: &'static str,
}

/// All known settings, in the same order as the Python SETTINGS dict.
pub static SETTINGS: &[(&str, SettingDef)] = &[
    (
        "deadline-cloud-monitor.path",
        SettingDef {
            default: "",
            depend: None,
            section_format: None,
            description: "The filesystem path to Deadline Cloud monitor, set during login process.",
        },
    ),
    (
        "defaults.aws_profile_name",
        SettingDef {
            default: "(default)",
            depend: None,
            section_format: Some("profile-{}"),
            description: "The AWS profile name to use by default.",
        },
    ),
    (
        "settings.job_history_dir",
        SettingDef {
            default: "~/.deadline/job_history/{aws_profile_name}",
            depend: Some("defaults.aws_profile_name"),
            section_format: None,
            description: "The directory in which to place the job submission history for this AWS profile name.",
        },
    ),
    (
        "defaults.farm_id",
        SettingDef {
            default: "",
            depend: Some("defaults.aws_profile_name"),
            section_format: Some("{}"),
            description: "The Farm ID to use by default.",
        },
    ),
    (
        "settings.storage_profile_id",
        SettingDef {
            default: "",
            depend: Some("defaults.farm_id"),
            section_format: None,
            description: "The storage profile that this workstation conforms to.",
        },
    ),
    (
        "defaults.queue_id",
        SettingDef {
            default: "",
            depend: Some("defaults.farm_id"),
            section_format: Some("{}"),
            description: "The Queue ID to use by default.",
        },
    ),
    (
        "defaults.job_id",
        SettingDef {
            default: "",
            depend: Some("defaults.queue_id"),
            section_format: None,
            description: "The Job ID to use by default.",
        },
    ),
    (
        "settings.auto_accept",
        SettingDef {
            default: "false",
            depend: None,
            section_format: None,
            description: "Automatically accept the default choice for any interactive prompts.",
        },
    ),
    (
        "settings.conflict_resolution",
        SettingDef {
            default: "NOT_SELECTED",
            depend: None,
            section_format: None,
            description: "How to handle downloads if a file already exists.",
        },
    ),
    (
        "settings.log_level",
        SettingDef {
            default: "WARNING",
            depend: None,
            section_format: None,
            description: "The logging level to use in the CLI and GUIs.",
        },
    ),
    (
        "telemetry.opt_out",
        SettingDef {
            default: "false",
            depend: None,
            section_format: None,
            description: "If set to 'true', don't record any telemetry events.",
        },
    ),
    (
        "telemetry.identifier",
        SettingDef {
            default: "",
            depend: None,
            section_format: None,
            description: "A randomly generated identifier used to record telemetry events.",
        },
    ),
    (
        "defaults.job_attachments_file_system",
        SettingDef {
            default: "COPIED",
            depend: Some("defaults.farm_id"),
            section_format: None,
            description: "The file system mode to use for job attachments when running jobs.",
        },
    ),
    (
        "settings.s3_max_pool_connections",
        SettingDef {
            default: "50",
            depend: None,
            section_format: None,
            description: "The maximum number of connections to keep in the S3 connection pool.",
        },
    ),
    (
        "settings.small_file_threshold_multiplier",
        SettingDef {
            default: "20",
            depend: None,
            section_format: None,
            description: "Multiplier for calculating the small file size threshold.",
        },
    ),
    (
        "settings.known_asset_paths",
        SettingDef {
            default: "",
            depend: None,
            section_format: None,
            description: "A list of paths that should not generate warnings when outside storage profile locations.",
        },
    ),
    (
        "settings.locale",
        SettingDef {
            default: "",
            depend: None,
            section_format: None,
            description: "The locale to use for the UI. If empty, uses the system locale.",
        },
    ),
    (
        "settings.force_s3_check",
        SettingDef {
            default: "false",
            depend: None,
            section_format: None,
            description: "Controls S3 verification behavior for job attachments.",
        },
    ),
];

/// Look up a setting definition by name. Returns `None` if unknown.
pub fn find_setting(name: &str) -> Option<&'static SettingDef> {
    SETTINGS.iter().find(|(n, _)| *n == name).map(|(_, def)| def)
}
