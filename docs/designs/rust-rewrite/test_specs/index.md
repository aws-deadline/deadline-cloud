# AWS Deadline Cloud Client — Behavioral Test Specification

> Reference implementation: Python (`../../deadline-cloud-python`)
> Target implementation: Rust (`../../deadline-cloud-rs`)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

## Section Index

| #  | Section                                      | Status         | File |
|----|----------------------------------------------|----------------|------|
| 1 | Config: get/set/clear/read/write settings | ✅ Complete (63 cases) | [config.md](config.md) |
| 2 | Config: profile resolution & best profile | ✅ Complete (18 cases) | [config.md](config.md) |
| 3 | Session: AWS session/client management | ✅ Complete (28 cases) | [session.md](session.md) |
| 4 | Session: auth status & credential source | ✅ Complete (16 cases) | [session.md](session.md) |
| 5 | Session: queue user credentials | ✅ Complete (20 cases) | [session.md](session.md) |
| 6 | API: login/logout | ✅ Complete (18 cases) | [api_resource_management.md](api_resource_management.md) |
| 7 | API: list (farms/queues/jobs/fleets/storage) | ✅ Complete (22 cases) | [api_resource_management.md](api_resource_management.md) |
| 8 | API: queue parameters | ✅ Complete (10 cases) | [api_resource_management.md](api_resource_management.md) |
| 9 | API: queue credentials (assume role) | ✅ Complete (8 cases) | [api_resource_management.md](api_resource_management.md) |
| 10 | API: storage profile for queue | ✅ Complete (5 cases) | [api_resource_management.md](api_resource_management.md) |
| 11 | API: submit job bundle | ✅ Complete (42 cases) | [api_job_lifecycle.md](api_job_lifecycle.md) |
| 12 | API: job monitoring & logs | ✅ Complete (35 cases) | [api_job_lifecycle.md](api_job_lifecycle.md) |
| 13 | API: diagnostics (get/list/search) | ✅ Complete (20 cases) | [api_job_lifecycle.md](api_job_lifecycle.md) |
| 14 | API: telemetry | ✅ Complete (24 cases) | [api_job_lifecycle.md](api_job_lifecycle.md) |
| 15 | Job bundle: loading & parsing | ✅ Complete (24 cases) | [job_bundle.md](job_bundle.md) |
| 16 | Job bundle: parameters | ✅ Complete (118 cases) | [job_bundle.md](job_bundle.md) |
| 17 | Job bundle: submission & asset references | ✅ Complete (22 cases) | [job_bundle.md](job_bundle.md) |
| 18 | Job bundle: history directory | ✅ Complete (10 cases) | [job_bundle.md](job_bundle.md) |
| 19 | Job attachments: models & data classes | ✅ Complete (30 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 20 | Job attachments: hashing & manifest creation | ✅ Complete (24 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 21 | Job attachments: upload | ✅ Complete (28 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 22 | Job attachments: download | ✅ Complete (34 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 23 | Job attachments: asset sync | ✅ Complete (28 cases) | [job_attachments_orchestration.md](job_attachments_orchestration.md) |
| 24 | Job attachments: caches (hash & S3 check) | ✅ Complete (29 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 25 | Job attachments: manifest formats & decode | ✅ Complete (32 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 26 | Job attachments: path mapping | ✅ Complete (19 cases) | [job_attachments_orchestration.md](job_attachments_orchestration.md) |
| 27 | Job attachments: glob & diff | ✅ Complete (22 cases) | [job_attachments_orchestration.md](job_attachments_orchestration.md) |
| 28 | Job attachments: VFS | ✅ Complete (89 cases) | [job_attachments_orchestration.md](job_attachments_orchestration.md) |
| 29 | Job attachments: OS file permissions | ✅ Complete (39 cases) | [job_attachments_orchestration.md](job_attachments_orchestration.md) |
| 30 | Job attachments: public API (attachment) | ✅ Complete (36 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 31 | Job attachments: public API (manifest) | ✅ Complete (45 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 32 | Job attachments: progress tracking | ✅ Complete (17 cases) | [job_attachments_orchestration.md](job_attachments_orchestration.md) |
| 33 | Job attachments: exceptions | ✅ Complete (8 cases) | [job_attachments_orchestration.md](job_attachments_orchestration.md) |
| 34 | Job attachments: AWS client helpers | ✅ Complete (37 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 35 | Job attachments: incremental downloads | ✅ Complete (61 cases) | [job_attachments_data_transfer.md](job_attachments_data_transfer.md) |
| 36 | Common: path_utils | ✅ Complete (14 cases) | [common.md](common.md) |
| 37 | CLI: root group & common utilities | ✅ Complete (58 cases) | [cli.md](cli.md) |
| 38 | CLI: deadline config | ✅ Complete (10 cases) | [cli.md](cli.md) |
| 39 | CLI: deadline auth | ✅ Complete (7 cases) | [cli.md](cli.md) |
| 40 | CLI: deadline farm | ✅ Complete (7 cases) | [cli.md](cli.md) |
| 41 | CLI: deadline fleet | ✅ Complete (8 cases) | [cli.md](cli.md) |
| 42 | CLI: deadline queue | ✅ Complete (26 cases) | [cli.md](cli.md) |
| 43 | CLI: deadline worker | ✅ Complete (7 cases) | [cli.md](cli.md) |
| 44 | CLI: deadline job | ✅ Complete (28 cases) | [cli.md](cli.md) |
| 45 | CLI: deadline bundle | ✅ Complete (14 cases) | [cli.md](cli.md) |
| 46 | CLI: deadline attachment | ✅ Complete (15 cases) | [cli.md](cli.md) |
| 47 | CLI: deadline manifest | ✅ Complete (26 cases) | [cli.md](cli.md) |
| 48 | CLI: deadline handle-web-url | ✅ Complete (14 cases) | [cli.md](cli.md) |
| 49 | CLI: deadline mcp-server | ✅ Complete (4 cases) | [cli.md](cli.md) |
| 50 | MCP server | ✅ Complete (39 cases) | [mcp.md](mcp.md) |
| 51 | Exceptions (client) | ✅ Complete (7 cases) | [common.md](common.md) |
| 52 | SubmitterInfo data structure | ✅ Complete (4 cases) | [common.md](common.md) |

### Planned (new crates)

| # | Section | Status | Phase |
|---|---------|--------|-------|
| 53+ | Worker agent: session management, attachment sync | Not started | Phase 2 |
| TBD | GUI FFI: C ABI contract, JSON exchange, callbacks | Not started | Phase 3 |
| TBD | MCP server: tool registration, telemetry, error handling | Not started | Phase 5 |
