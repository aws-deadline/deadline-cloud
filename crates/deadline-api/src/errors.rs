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
            | Self::UserInitiatedCancel(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for DeadlineError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_error_displays_message_verbatim() {
        let err = DeadlineError::OperationError("something went wrong".into());
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn operation_canceled_default_message() {
        let err = DeadlineError::operation_canceled();
        assert_eq!(err.to_string(), "Operation canceled");
    }

    #[test]
    fn operation_canceled_custom_message() {
        let err = DeadlineError::OperationCanceled("custom".into());
        assert_eq!(err.to_string(), "custom");
    }

    #[test]
    fn operation_timed_out_default_message() {
        let err = DeadlineError::operation_timed_out();
        assert_eq!(err.to_string(), "Operation timed out");
    }

    #[test]
    fn create_job_waiter_canceled_default_message() {
        let err = DeadlineError::create_job_waiter_canceled();
        let msg = err.to_string();
        assert!(
            msg.contains("waiting for CreateJob"),
            "expected message to mention 'waiting for CreateJob', got: {msg}"
        );
    }

    #[test]
    fn user_initiated_cancel_default_message() {
        let err = DeadlineError::user_initiated_cancel();
        assert_eq!(err.to_string(), "Operation canceled by user");
    }

    #[test]
    fn all_variants_implement_error_trait() {
        let errors: Vec<Box<dyn std::error::Error>> = vec![
            Box::new(DeadlineError::OperationError("a".into())),
            Box::new(DeadlineError::operation_canceled()),
            Box::new(DeadlineError::operation_timed_out()),
            Box::new(DeadlineError::create_job_waiter_canceled()),
            Box::new(DeadlineError::user_initiated_cancel()),
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
    }
}
