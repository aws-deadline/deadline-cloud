/// Error types for the AWS Deadline Cloud client library.
use std::fmt;

/// Errors from Deadline Cloud client operations.
///
/// The CLI error handler prints `OperationError` messages verbatim to the user.
/// Specialized variants carry default messages appropriate to their context.
#[derive(Debug)]
pub enum DeadlineError {
    /// Generic operation error — message is printed verbatim by the CLI.
    OperationError(String),
    /// Operation was canceled (default: "Operation canceled").
    OperationCanceled(String),
    /// Operation timed out (default: "Operation timed out").
    OperationTimedOut(String),
    /// CreateJob waiter was interrupted (default: "Operation canceled while waiting for CreateJob to finish").
    CreateJobWaiterCanceled(String),
    /// User explicitly requested cancellation (default: "Operation canceled by user").
    UserInitiatedCancel(String),
    /// User input is not valid.
    NonValidInput(String),
}

impl DeadlineError {
    pub fn operation_canceled() -> Self {
        Self::OperationCanceled("Operation canceled".into())
    }

    pub fn operation_timed_out() -> Self {
        Self::OperationTimedOut("Operation timed out".into())
    }

    pub fn create_job_waiter_canceled() -> Self {
        Self::CreateJobWaiterCanceled(
            "Operation canceled while waiting for CreateJob to finish".into(),
        )
    }

    pub fn user_initiated_cancel() -> Self {
        Self::UserInitiatedCancel("Operation canceled by user".into())
    }
}

impl fmt::Display for DeadlineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperationError(msg)
            | Self::OperationCanceled(msg)
            | Self::OperationTimedOut(msg)
            | Self::CreateJobWaiterCanceled(msg)
            | Self::UserInitiatedCancel(msg)
            | Self::NonValidInput(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for DeadlineError {}

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
                write!(f, "S3 {action} failed ({status_code}) bucket={bucket} key={key}")?;
                if let Some(msg) = message {
                    write!(f, ": {msg}")?;
                }
                Ok(())
            }
            Self::S3BotoCore { action, details } => {
                write!(f, "S3 {action}: {details}")
            }
        }
    }
}

impl std::error::Error for JobAttachmentsError {}

#[cfg(test)]
mod tests {
    use super::*;

    // §51 case 1: OperationError("msg") → display returns "msg"
    #[test]
    fn operation_error_displays_message_verbatim() {
        let err = DeadlineError::OperationError("something went wrong".into());
        assert_eq!(err.to_string(), "something went wrong");
    }

    // §51 case 2: OperationCanceled default → "Operation canceled"
    #[test]
    fn operation_canceled_default_message() {
        let err = DeadlineError::operation_canceled();
        assert_eq!(err.to_string(), "Operation canceled");
    }

    // §51 case 3: OperationCanceled("custom") → "custom"
    #[test]
    fn operation_canceled_custom_message() {
        let err = DeadlineError::OperationCanceled("custom".into());
        assert_eq!(err.to_string(), "custom");
    }

    // §51 case 4: OperationTimedOut default → "Operation timed out"
    #[test]
    fn operation_timed_out_default_message() {
        let err = DeadlineError::operation_timed_out();
        assert_eq!(err.to_string(), "Operation timed out");
    }

    // §51 case 5: CreateJobWaiterCanceled default → mentions "waiting for CreateJob"
    #[test]
    fn create_job_waiter_canceled_default_message() {
        let err = DeadlineError::create_job_waiter_canceled();
        let msg = err.to_string();
        assert!(
            msg.contains("waiting for CreateJob"),
            "expected message to mention 'waiting for CreateJob', got: {msg}"
        );
    }

    // §51 case 6: UserInitiatedCancel default → "Operation canceled by user"
    #[test]
    fn user_initiated_cancel_default_message() {
        let err = DeadlineError::user_initiated_cancel();
        assert_eq!(err.to_string(), "Operation canceled by user");
    }

    // §51 case 7: All cancel/timeout variants are DeadlineError variants
    // In Rust, enum variants are inherently part of the enum — this test
    // verifies they can all be handled as &dyn Error (the trait object
    // equivalent of a base exception class).
    #[test]
    fn all_variants_implement_error_trait() {
        let errors: Vec<Box<dyn std::error::Error>> = vec![
            Box::new(DeadlineError::OperationError("a".into())),
            Box::new(DeadlineError::operation_canceled()),
            Box::new(DeadlineError::operation_timed_out()),
            Box::new(DeadlineError::create_job_waiter_canceled()),
            Box::new(DeadlineError::user_initiated_cancel()),
        ];
        // All can be used as &dyn Error — the Rust equivalent of
        // "all are subtypes of DeadlineOperationError"
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
    }
}
