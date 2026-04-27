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

Format: `app/deadline-api#<version> submitter/<name>#<ver> cli-command/<cmd>`

## Queue Credential Provider

The queue credential provider implements the SDK's `ProvideCredentials`
trait. It calls `AssumeQueueRoleForUser` to get temporary credentials,
and the SDK automatically refreshes when they expire.

Error messages include actionable guidance:
- Throttling → "Please retry"
- Internal error → "Please wait and retry"
- Other → "Contact your administrator"

## Profile Resolution

`"(default)"`, `"default"`, and `""` all map to the default credential
chain (no named profile). Any other value becomes a named profile.

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
