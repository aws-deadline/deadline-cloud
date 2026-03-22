/// Error types for the AWS Deadline Cloud client library.

#[derive(Debug)]
pub enum DeadlineError {
    OperationError(String),
    OperationCanceled(String),
    OperationTimedOut(String),
    NonValidInput(String),
}

#[derive(Debug)]
pub enum JobAttachmentsError {
    AssetSync(String),
    S3Client { action: String, status_code: u16, bucket: String, key: String, message: Option<String> },
    S3BotoCore { action: String, details: String },
    MissingSettings(String),
    MissingBucket(String),
    MissingRootPrefix(String),
    MalformedAttachment(String),
    AssetOutsideRoot(String),
    MisconfiguredInputs(String),
    ManifestDecode(String),
    MissingManifest(String),
    MissingAssetRoot(String),
    Cancelled { message: String },
    PathOutsideDirectory(String),
    VfsExecutableMissing(String),
    VfsFailedToMount(String),
    VfsOsUserNotSet(String),
    UnsupportedHashAlgorithm(String),
    ManifestCreation(String),
    ManifestOutdated(String),
}
