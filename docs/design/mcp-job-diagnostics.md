# MCP Job Diagnostics Tools Design

## Overview

Extend the Deadline Cloud MCP server with tools to diagnose failed jobs: find failures, retrieve sessions, and fetch CloudWatch logs.

## Problem Statement

Users need to diagnose failed jobs but current MCP tools lack:
1. Finding failed jobs in a queue
2. Getting detailed job/session/step/task information  
3. Retrieving session logs from CloudWatch

## Debugging Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Job Failure Debugging Flow                          │
└─────────────────────────────────────────────────────────────────────────┘

User: "Why did my job fail?"
           │
           ▼
┌─────────────────────┐
│  search_jobs        │  Find failed jobs in queue
│  (status=FAILED)    │
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│  Manual Flow:       │
│  get_job            │
│       ▼             │
│  list_steps         │
│       ▼             │
│  list_tasks         │
│       ▼             │
│  list_sessions      │
│       ▼             │
│  get_session_logs   │
└─────────────────────┘
```

## Tools Summary

| Tool | Purpose | Key Inputs | Key Outputs |
|------|---------|------------|-------------|
| `search_jobs` | Find jobs by status/name | queueIds, taskRunStatus | Job list with status |
| `get_job` | Get job details | jobId | Status, task counts |
| `list_steps` | List job steps | jobId | Steps with status |
| `list_tasks` | List step tasks | jobId, stepId | Tasks with runStatus |
| `list_sessions` | List job sessions | jobId | Session summaries |
| `get_session` | Get session details | sessionId | Log config, worker info |

---

## Tier 1: Primitive APIs

### search_jobs

Find jobs with optional filters.

```python
def search_jobs(
    farm_id: str,
    queue_ids: List[str],
    task_run_status: Optional[str] = None,  # PENDING|READY|RUNNING|FAILED|SUCCEEDED
    name_contains: Optional[str] = None,
    page_size: Optional[int] = 25,          # 1-100
    item_offset: Optional[int] = 0,         # 0-10000
) -> Dict[str, Any]
```

**Output:**
```json
{"jobs": [...], "totalResults": 10, "nextItemOffset": 25}
```

### get_job

Get detailed job information.

```python
def get_job(farm_id: str, queue_id: str, job_id: str) -> Dict[str, Any]
```

**Output:** Job details including name, status, taskRunStatusCounts, timestamps.

### list_steps

List all steps for a job.

```python
def list_steps(farm_id: str, queue_id: str, job_id: str) -> Dict[str, Any]
```

**Output:**
```json
{"steps": [{"stepId": "...", "name": "...", "taskRunStatus": "...", "taskRunStatusCounts": {...}}]}
```

### list_tasks

List all tasks for a step.

```python
def list_tasks(farm_id: str, queue_id: str, job_id: str, step_id: str) -> Dict[str, Any]
```

**Output:**
```json
{"tasks": [{"taskId": "...", "runStatus": "...", "parameters": {...}}]}
```

### list_sessions

List all sessions for a job.

```python
def list_sessions(farm_id: str, queue_id: str, job_id: str) -> Dict[str, Any]
```

**Output:**
```json
{"sessions": [{"sessionId": "...", "lifecycleStatus": "...", "workerId": "..."}]}
```

### get_session

Get detailed session information.

```python
def get_session(farm_id: str, queue_id: str, job_id: str, session_id: str) -> Dict[str, Any]
```

**Output:** Session details including lifecycleStatus, log configuration, worker info.

---

## Usage Examples

### Find failed jobs
```
User: "Show me all failed jobs"
Tool: search_jobs(queueIds=["queue-xxx"], taskRunStatus="FAILED")
```

### Diagnose a failure
```
User: "Why did job-111 fail?"
1. get_job(jobId="job-111") → Get job status
2. list_steps(jobId="job-111") → Find failed steps
3. list_tasks(jobId="job-111", stepId="step-xxx") → Find failed tasks
4. list_sessions(jobId="job-111") → Get sessions
5. get_session_logs(sessionId="session-xxx") → Get logs
```

---

## Appendix

### A. Deadline Cloud APIs Used

| API | Purpose |
|-----|---------|
| `GetJob` | Get job status and task counts |
| `GetSession` | Get log stream configuration |
| `ListSessions` | Find sessions for job |
| `ListSteps` | Find failed steps |
| `ListTasks` | Find failed tasks |
| `SearchJobs` | Filter jobs by status |

### B. CloudWatch Logs

Session logs location:
- Log Group: `/aws/deadline/{farmId}/{queueId}`
- Log Stream: `{sessionId}`

### C. File Structure

```
src/deadline/client/api/
├── _mcp.py             # get_job, get_session, list_sessions, list_steps, list_tasks, search_jobs
└── _job_monitoring.py  # get_session_logs
```

### D. Registry Configuration

```python
TOOL_REGISTRY = {
    "get_job": {"func": api.get_job, "param_names": ["farm_id", "queue_id", "job_id"]},
    "get_session": {"func": api.get_session, "param_names": ["farm_id", "queue_id", "job_id", "session_id"]},
    "list_sessions": {"func": api.list_sessions, "param_names": ["farm_id", "queue_id", "job_id", "max_results"]},
    "list_steps": {"func": api.list_steps, "param_names": ["farm_id", "queue_id", "job_id", "max_results"]},
    "list_tasks": {"func": api.list_tasks, "param_names": ["farm_id", "queue_id", "job_id", "step_id", "max_results"]},
    "search_jobs": {"func": api.search_jobs, "param_names": ["farm_id", "queue_ids", "task_run_status", "name_contains", "page_size", "item_offset"]},
}
```

### E. Security

- Uses existing authentication via `get_boto3_client`
- Queue credentials via `get_queue_user_boto3_session` for CloudWatch
- No new IAM permissions required

### F. Testing Strategy

**Unit Tests:**
- Mock boto3 responses for each API
- Test pagination, error handling
- Test `diagnose_failed_job` with various failure scenarios

**Integration Tests:**
- Submit intentionally failing job, then diagnose

### G. Future Enhancements

- Worker diagnostics (`get_worker`, `list_workers`)
- Log filtering/grep capability
- Time-based job search
- Batch diagnostics
- Export to file

### H. References

- [AWS Deadline Cloud API Reference](https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/Welcome.html)
- [SearchJobs API](https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_SearchJobs.html)
- [GetSession API](https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_GetSession.html)
