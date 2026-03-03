# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from mcp.server.fastmcp import FastMCP

from .utils import register_api_tools

INSTRUCTIONS = """
# AWS Deadline Cloud MCP Server

This server provides tools for interacting with AWS Deadline Cloud render farm management service.

## Debugging Failed Jobs Workflow

When debugging failed Deadline Cloud jobs, follow this sequence:

1. **Find failed jobs**: Use `deadline_search_jobs` with `task_run_status="FAILED"` to find jobs in a failed state
2. **Get job details**: Use `deadline_get_job` to get full job info including taskRunStatusCounts
3. **List steps**: Use `deadline_list_steps` to identify which steps failed (look for FAILED in taskRunStatusCounts)
4. **List tasks**: Use `deadline_list_tasks` for the failed step to find specific failed task IDs
5. **List sessions**: Use `deadline_list_sessions` to get session IDs for the job
6. **Get session info**: Use `deadline_get_session` to get the CloudWatch log configuration (includes logGroupName and logStreamName)
7. **Get logs**: Use `deadline_get_session_logs` to fetch session logs. If no logs are returned, the logs may be at a different position in the stream. Use the AWS CLI with `--start-from-head` to read from the beginning:
   ```
   aws logs get-log-events \
     --log-group-name "/aws/deadline/{farm_id}/{queue_id}" \
     --log-stream-name "{session_id}" \
     --start-from-head \
     --limit 100 \
     --region {region}
   ```

## Key Concepts

- **Farm**: Top-level resource containing queues and fleets
- **Queue**: Where jobs are submitted and scheduled
- **Job**: A unit of work containing steps and tasks
- **Step**: A stage within a job (e.g., render, composite)
- **Task**: Individual work item within a step
- **Session**: Worker execution context with associated logs

## Configuration

The server uses the Deadline Cloud configuration from `~/.deadline/config`. Set default farm_id and queue_id there to avoid passing them to every call.
"""

app = FastMCP("deadline-cloud", instructions=INSTRUCTIONS)

register_api_tools(app, prefix="deadline_")


def main():
    app.run()
