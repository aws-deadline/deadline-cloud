# Session Cache

## Overview

The session cache is a process-wide store that avoids re-creating AWS SDK
clients on every API call. It holds a base SDK config (built from the
user's AWS profile) and a map of queue-scoped configs (for DCM users
accessing S3 or CloudWatch via queue role assumption).

The base config is keyed by profile name — if the user switches profiles,
the cache is invalidated and rebuilt. Queue configs are keyed by
`(farm_id, queue_id)` and invalidated alongside the base session.

## SessionContext

Tracks caller identity for User-Agent enrichment:
- `submitter_name`, `submitter_version` — set by GUI/DCC plugins via FFI
- `cli_command_name` — set before each CLI command dispatch

Format: `app/deadline-client#<version> submitter/<name>#<ver> cli-command/<cmd>`

## Queue Credential Provider

The queue credential provider implements the SDK's `ProvideCredentials`
trait. It calls `AssumeQueueRoleForUser` to get temporary credentials,
and the SDK automatically refreshes when they expire.

Error messages include actionable guidance:
- Throttling → "Please retry the operation later"
- Internal error → error message from service (no retry guidance)
- Other → "Contact your administrator"

## Profile Resolution

Session functions take `profile: Option<&str>` directly — callers extract
the profile from config before calling. The sentinel values `"(default)"`,
`"default"`, and `""` all map to `None` (default credential chain). Any
other value becomes a named profile. The helper
`session::resolve_profile_name(&config)` performs this extraction.

## Endpoint Override

Reads `AWS_ENDPOINT_URL_DEADLINE` and `AWS_ENDPOINT_URL_STS` environment
variables. Applied when building service clients. Used by tests to point
at the wiremock stub server.

The Deadline SDK prepends `management.` or `scheduling.` to the endpoint
hostname (Smithy host prefix). In tests, the stub server receives requests
at `management.localhost:PORT`. The `.localhost` TLD resolves to 127.0.0.1
per RFC 6761.

## Invalidation

The cache is cleared on logout and on explicit refresh. The next API call
triggers a full credential re-resolution.

## Concurrency

The session cache uses a `tokio::sync::Mutex`. Public functions minimize
lock hold duration to avoid blocking concurrent callers during network I/O:

- **Cache hit:** Lock acquired briefly to clone the cached config.
- **Cache miss:** Lock acquired to check → released → config loaded
  (network I/O) → lock re-acquired to store.

This means concurrent callers on cache miss may both load the config
independently. The last writer wins, which is safe because both load
the same profile and produce identical configs.

Client construction (building `DeadlineClient`, `StsClient`) happens
entirely outside the lock. Only the raw `SdkConfig` is cached.
