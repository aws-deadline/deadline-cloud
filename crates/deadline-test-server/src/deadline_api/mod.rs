//! Wiremock mock helpers for Deadline Cloud API responses.
//!
//! ## Mock response data rules
//!
//! **Background:** Our `api.rs` functions use `ResponseBodyCapture` to
//! extract raw JSON from AWS SDK responses. However, the SDK deserializes
//! the HTTP response into its typed output struct *before* the interceptor
//! runs. If deserialization fails, the SDK returns
//! `SdkError::ServiceError` with "Unknown: No message" and our code never
//! sees the raw bytes. This means mock response bodies must be valid
//! enough for the SDK's deserializer, even though we only use the raw JSON.
//!
//! **What the SDK tolerates (safe to omit from mocks):**
//!
//! - **Required scalar fields** (`String`, `i32`, `DateTime`, enums).
//!   The SDK generates a `correct_errors()` function for every type that
//!   fills in defaults: empty string, 0, epoch timestamp, or
//!   `Unknown("no value was set")` for enums. So fields like `createdAt`,
//!   `createdBy`, `lifecycleStatus`, and `priority` can be omitted — the
//!   SDK won't fail, it will just fill in placeholder values.
//! - **Optional fields** (`Option<T>` in the SDK) — always safe to omit.
//! - **Extra/unknown fields** — silently skipped by the SDK deserializer.
//!   Adding new fields to the real API will never break existing mocks.
//!
//! **What breaks deserialization (must be structurally correct):**
//!
//! - **Union types** must use the tagged object format. For example,
//!   `TaskParameterValue` is a union with variants `int`, `float`,
//!   `string`, `path`. The mock must use `{ "Frame": { "int": "1" } }`,
//!   NOT `{ "Frame": "1" }`. A bare string where the SDK expects a union
//!   object causes a deserialization failure.
//! - **Struct-typed fields** must be JSON objects, not scalars.
//! - **Wrong JSON types** — e.g., a string where a number is expected.
//!
//! **How to check field types:**
//!
//! 1. Look up the operation in the [AWS Deadline Cloud API Reference](
//!    https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/Welcome.html).
//!    Check whether a response field is a "structure" or "union" — those
//!    need the correct nested object shape.
//! 2. If still unsure, check the generated SDK type in
//!    `~/.cargo/registry/src/*/aws-sdk-deadline-*/src/types/`. Fields
//!    that are `Option<T>` are safe to omit. Fields that are bare `T`
//!    are required but auto-defaulted by `correct_errors()`. Union types
//!    show up as enums with struct variants.

pub mod farms;
pub mod queues;
pub mod fleets;
pub mod jobs;
pub mod workers;
pub mod sessions;
pub mod queue_resources;
pub mod telemetry;
pub mod errors;
pub mod sts;
pub mod cloudwatch;
