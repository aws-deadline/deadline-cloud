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

/// All known settings, in definition order.
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
            description: "The AWS profile name to use by default. Set to '' to use the default credentials. Other settings are saved with the profile.",
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
            description: "The storage profile that this workstation conforms to. It specifies where shared file systems are mounted, and where named job attachments should go.",
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
            description: "The Job ID to use by default. This gets updated by job submission, so is normally the most recently submitted job.",
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
            description: "How to handle downloads if a file already exists",
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
            description: "A randomly generated identifier used to record telemetry events for this configuration.",
        },
    ),
    (
        "defaults.job_attachments_file_system",
        SettingDef {
            default: "COPIED",
            depend: Some("defaults.farm_id"),
            section_format: None,
            description: "The file system mode to use for job attachments when running jobs. COPIED means to download a copy of the attachment data, VIRTUAL means to use a virtual file system for lazy loading.",
        },
    ),
    (
        "settings.s3_max_pool_connections",
        SettingDef {
            default: "50",
            depend: None,
            section_format: None,
            description: "The maximum number of connections to keep in the connection pool used by the S3's upload/download operations. If this value is not set, the default value of 50 is used. (Note: It's recommended setting this value above 10 to avoid 'Connection pool is full' warnings during the uploads/downloads.)",
        },
    ),
    (
        "settings.small_file_threshold_multiplier",
        SettingDef {
            default: "20",
            depend: None,
            section_format: None,
            description: "When uploading job attachments, the file size threshold is set to separate 'large' files from 'small' files so that 'large' files can be processed serially. This multiplier is used to calculate the size threshold. (Small files are defined as those smaller than or equal to the chunk size multiplied by this factor.)",
        },
    ),
    (
        "settings.known_asset_paths",
        SettingDef {
            default: "",
            depend: None,
            section_format: None,
            description: "A list of paths that should not generate warnings when outside storage profile locations, separated by the OS path list separator (semicolon on Windows, colon on Linux/macOS).",
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
            description: "Controls S3 verification behavior for job attachments. When 'true', always verify files exist in S3 via HEAD request before skipping upload (most reliable but slower, skips cache integrity check since every file is verified). When 'false' or unset, use local cache with periodic integrity sampling against S3 (balanced default).",
        },
    ),
    (
        "settings.allow_bundle_hooks",
        SettingDef {
            default: "false",
            depend: None,
            section_format: None,
            description: "Allow execution of hooks defined in job bundle hooks.yaml/hooks.json files. When 'true', bundle hooks will run with a confirmation prompt (unless auto_accept is enabled). When 'false', bundle hooks are ignored.",
        },
    ),
    (
        "settings.allow_environment_hooks",
        SettingDef {
            default: "false",
            depend: None,
            section_format: None,
            description: "Allow execution of hooks from DEADLINE_HOOKS_DIR environment variable. When 'true', hooks from the directory specified by DEADLINE_HOOKS_DIR will run. When 'false', environment hooks are ignored.",
        },
    ),
    (
        "settings.submitter_update_notification",
        SettingDef {
            default: "true",
            depend: None,
            section_format: None,
            description: "Enable update notification checks for DCC submitter integrations. When 'true', the submitter checks for newer versions at startup. When 'false', update checks are skipped.",
        },
    ),
    (
        "settings.max_retries_per_task",
        SettingDef {
            default: "5",
            depend: None,
            section_format: None,
            description: "The default maximum number of times a task will retry before it is marked as failed.",
        },
    ),
    (
        "settings.max_failed_tasks_count",
        SettingDef {
            default: "20",
            depend: None,
            section_format: None,
            description: "The default maximum number of tasks that can fail before the job is marked as failed.",
        },
    ),
];

/// Look up a setting definition by name. Returns `None` if unknown.
pub fn find_setting(name: &str) -> Option<&'static SettingDef> {
    SETTINGS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, def)| def)
}
