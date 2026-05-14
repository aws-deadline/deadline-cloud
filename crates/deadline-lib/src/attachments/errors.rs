/// Error types for the job attachments subsystem.
use std::fmt;

/// Errors from the job attachments subsystem.
#[derive(Debug)]
pub enum JobAttachmentsError {
    AssetSync(String),
    S3Client {
        action: String,
        status_code: u16,
        bucket: String,
        key: String,
        message: Option<String>,
    },
    S3BotoCore {
        action: String,
        details: String,
    },
    MissingSettings(String),
    MissingBucket(String),
    MissingRootPrefix(String),
    MalformedAttachment(String),
    AssetOutsideRoot(String),
    MisconfiguredInputs(String),
    ManifestDecode(String),
    MissingManifest(String),
    MissingAssetRoot(String),
    Cancelled {
        message: String,
    },
    PathOutsideDirectory(String),
    VfsExecutableMissing(String),
    VfsFailedToMount(String),
    VfsOsUserNotSet(String),
    UnsupportedHashAlgorithm(String),
    ManifestCreation(String),
    ManifestOutdated(String),
}

impl fmt::Display for JobAttachmentsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AssetSync(msg)
            | Self::MissingSettings(msg)
            | Self::MissingBucket(msg)
            | Self::MissingRootPrefix(msg)
            | Self::MalformedAttachment(msg)
            | Self::AssetOutsideRoot(msg)
            | Self::MisconfiguredInputs(msg)
            | Self::ManifestDecode(msg)
            | Self::MissingManifest(msg)
            | Self::MissingAssetRoot(msg)
            | Self::Cancelled { message: msg }
            | Self::PathOutsideDirectory(msg)
            | Self::VfsExecutableMissing(msg)
            | Self::VfsFailedToMount(msg)
            | Self::VfsOsUserNotSet(msg)
            | Self::UnsupportedHashAlgorithm(msg)
            | Self::ManifestCreation(msg)
            | Self::ManifestOutdated(msg) => write!(f, "{msg}"),
            Self::S3Client {
                action,
                status_code,
                bucket,
                key,
                message,
            } => {
                write!(
                    f,
                    "Error {action} in bucket '{bucket}', Target key or prefix: '{key}', HTTP Status Code: {status_code}"
                )?;
                if let Some(msg) = message {
                    write!(f, ", {msg}")?;
                }
                Ok(())
            }
            Self::S3BotoCore { action, details } => {
                write!(
                    f,
                    "An issue occurred with AWS service request while {action}: {details}\n\
                     This could be due to temporary issues with AWS, internet connection, or your AWS credentials. \
                     Please verify your credentials and network connection. If the problem persists, try again later \
                     or contact support for further assistance."
                )
            }
        }
    }
}

impl std::error::Error for JobAttachmentsError {}
