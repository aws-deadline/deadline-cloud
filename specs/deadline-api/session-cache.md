# Session Cache

## Overview

`session.rs` manages a global `SessionCache` that holds cached `SdkConfig`
instances and queue credential configurations.

## Cache Structure

```
static SESSION: LazyLock<Mutex<SessionCache>>

SessionCache {
    cached_config: Option<SdkConfig>,       // base SDK config
    cached_profile: Option<Option<String>>,  // which profile it was built for
    cached_queue_configs: HashMap<(String, String), SdkConfig>,  // (farm, queue) → scoped config
    context: SessionContext,                 // user-agent metadata
}
```

One `SdkConfig` cached per profile. If the profile changes (different `--profile`
flag), the cache is invalidated and rebuilt. Queue configs are cached by
`(farm_id, queue_id)` and invalidated alongside the base session.

## SessionContext

Tracks caller identity for User-Agent enrichment:
- `submitter_name`, `submitter_version` — set by GUI/DCC plugins via FFI
- `cli_command_name` — set before each CLI command dispatch

Format: `app/deadline-api#<version> submitter/<name>#<ver> cli-command/<cmd>`

## QueueUserCredentialProvider

Implements the SDK's `ProvideCredentials` trait. Calls `AssumeQueueRoleForUser`
to get temporary credentials. The SDK automatically refreshes when credentials
expire.

Error messages include actionable guidance:
- Throttling → "Please retry"
- Internal error → "Please wait and retry"
- Other → "Contact your administrator"

## Profile Resolution

`resolve_profile()` reads `defaults.aws_profile_name` from config.
`"(default)"`, `"default"`, and `""` all map to `None` (default credential chain).
Any other value becomes `Some(profile_name)`.

## Endpoint Override

Reads `AWS_ENDPOINT_URL_DEADLINE` and `AWS_ENDPOINT_URL_STS` environment
variables. Applied when building service clients. Used by tests to point
at the wiremock stub server.

The Deadline SDK prepends `management.` or `scheduling.` to the endpoint
hostname (Smithy host prefix). In tests, the stub server receives requests
at `management.localhost:PORT`. The `.localhost` TLD resolves to 127.0.0.1
per RFC 6761.

## Invalidation

`invalidate_session_cache()` clears all cached configs and queue configs.
Called by `auth::logout()` after successful logout. Next API call triggers
a full credential re-resolution.
