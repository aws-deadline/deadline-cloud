use aws_config::SdkConfig;
use aws_sdk_deadline::Client as DeadlineClient;
use aws_sdk_sts::Client as StsClient;
use deadline_config::config_file;
use deadline_config::ini::IniConfig;

fn get_setting(name: &str, config: Option<&IniConfig>) -> String {
    match config {
        Some(c) => config_file::get_setting_with_config(name, c).unwrap_or_default(),
        None => config_file::get_setting(name).unwrap_or_default(),
    }
}

fn resolve_profile(config: Option<&IniConfig>) -> Option<String> {
    let name = get_setting("defaults.aws_profile_name", config);
    match name.as_str() {
        "(default)" | "default" | "" => None,
        _ => Some(name),
    }
}

async fn load_sdk_config(config: Option<&IniConfig>) -> SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(profile) = resolve_profile(config) {
        loader = loader.profile_name(profile);
    }
    loader.load().await
}

pub async fn deadline_client(config: Option<&IniConfig>) -> DeadlineClient {
    let sdk_config = load_sdk_config(config).await;
    let mut builder = aws_sdk_deadline::config::Builder::from(&sdk_config);
    if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_DEADLINE") {
        builder = builder.endpoint_url(url);
    }
    DeadlineClient::from_conf(builder.build())
}

pub async fn sts_client(config: Option<&IniConfig>) -> StsClient {
    let sdk_config = load_sdk_config(config).await;
    let mut builder = aws_sdk_sts::config::Builder::from(&sdk_config);
    if let Ok(url) = std::env::var("AWS_ENDPOINT_URL_STS") {
        builder = builder.endpoint_url(url);
    }
    StsClient::from_conf(builder.build())
}

pub fn display_profile_name(config: Option<&IniConfig>) -> String {
    let name = get_setting("defaults.aws_profile_name", config);
    if name.is_empty() { "(default)".to_string() } else { name }
}
