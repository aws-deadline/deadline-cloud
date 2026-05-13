const CONFIG_ROOT: &str = ".deadline";
const COMPONENT_NAME: &str = "job_attachments";

pub fn default_cache_dir() -> Option<String> {
    std::env::var("HOME").ok().map(|home| {
        std::path::Path::new(&home)
            .join(CONFIG_ROOT)
            .join(COMPONENT_NAME)
            .to_string_lossy()
            .into_owned()
    })
}

// --- Re-exports from openjd-snapshots ---

pub use openjd_snapshots::HashCache;
pub use openjd_snapshots::S3CheckCache;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cache_dir_uses_home() {
        if let Some(home) = std::env::var_os("HOME") {
            let expected = std::path::Path::new(&home)
                .join(".deadline")
                .join("job_attachments");
            let result = default_cache_dir();
            assert_eq!(result, Some(expected.to_string_lossy().into_owned()));
        }
    }
}
