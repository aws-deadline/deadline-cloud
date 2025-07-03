# AWS Deadline Cloud MCP Server Implementation Guide

## Architecture Overview

The AWS Deadline Cloud MCP Server uses FastMCP within the MCP Python SDK to provide a simplified, decorator-based approach to tool registration while maintaining full MCP protocol compliance.

## Implementation Pattern

### Server Setup
```python
from mcp.server.fastmcp import FastMCP
import mcp.server.stdio
from mcp.server import InitializationOptions

# Create FastMCP app instance
app = FastMCP("deadline-cloud")

class DeadlineCloudMCPServer:
    def __init__(self):
        self.app = app
    
    async def run(self):
        """Start the MCP server with stdio transport"""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.app.run(read_stream, write_stream, InitializationOptions())
```

### Tool Registration

#### Simple Tools (No Parameters)
```python
@app.tool()
def list_farms() -> str:
    """List all available AWS Deadline Cloud farms"""
    from deadline.client import api
    try:
        farms = api.list_farms()
        return json.dumps(farms, indent=2)
    except Exception as e:
        return f"Error listing farms: {str(e)}"

@app.tool()
def auth_status() -> str:
    """Check current authentication status"""
    from deadline.client import api
    try:
        # Implementation using existing deadline auth system
        status = api.get_auth_status()
        return json.dumps(status, indent=2)
    except Exception as e:
        return f"Error checking auth status: {str(e)}"
```

#### Tools with Parameters
```python
@app.tool()
def get_farm(farm_id: str) -> str:
    """Get detailed information about a specific farm
    
    Args:
        farm_id: The ID of the farm to retrieve
    """
    from deadline.client import api
    try:
        farm = api.get_farm(farmId=farm_id)
        return json.dumps(farm, indent=2)
    except Exception as e:
        return f"Error getting farm {farm_id}: {str(e)}"

@app.tool()
def list_queues(farm_id: str, status: str = "ACTIVE") -> str:
    """List queues in a specific farm
    
    Args:
        farm_id: The ID of the farm
        status: Filter by queue status (default: ACTIVE)
    """
    from deadline.client import api
    try:
        queues = api.list_queues(farmId=farm_id, status=status)
        return json.dumps(queues, indent=2)
    except Exception as e:
        return f"Error listing queues for farm {farm_id}: {str(e)}"
```

#### Complex Tools with Multiple Parameters
```python
@app.tool()
def submit_job(
    farm_id: str,
    queue_id: str,
    bundle_path: str,
    job_name: str = None,
    priority: int = 50
) -> str:
    """Submit a job bundle to a queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue
        bundle_path: Path to the job bundle directory
        job_name: Optional name for the job
        priority: Job priority (0-100, default: 50)
    """
    from deadline.client import api
    try:
        result = api.submit_job_bundle(
            farmId=farm_id,
            queueId=queue_id,
            bundlePath=bundle_path,
            jobName=job_name,
            priority=priority
        )
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error submitting job: {str(e)}"
```

## Error Handling Pattern

All tools should follow this error handling pattern:

```python
@app.tool()
def example_tool(param: str) -> str:
    """Example tool with proper error handling"""
    from deadline.client import api
    try:
        # Validate parameters
        if not param:
            return json.dumps({"error": "Parameter 'param' is required"})
        
        # Call deadline API
        result = api.some_operation(param=param)
        
        # Return formatted result
        return json.dumps(result, indent=2)
        
    except api.AuthenticationError as e:
        return json.dumps({"error": f"Authentication failed: {str(e)}"})
    except api.PermissionError as e:
        return json.dumps({"error": f"Permission denied: {str(e)}"})
    except api.NotFoundError as e:
        return json.dumps({"error": f"Resource not found: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Unexpected error: {str(e)}"})
```

## Tool Organization

Tools are organized by functional area:

### Farm Management (`src/deadline/mcp/tools/farm.py`)
- `list_farms()`
- `get_farm(farm_id: str)`
- `create_farm(name: str, description: str = None)`
- `update_farm(farm_id: str, **kwargs)`
- `delete_farm(farm_id: str)`

### Queue Management (`src/deadline/mcp/tools/queue.py`)
- `list_queues(farm_id: str)`
- `get_queue(farm_id: str, queue_id: str)`
- `create_queue(farm_id: str, name: str, **kwargs)`
- `update_queue(farm_id: str, queue_id: str, **kwargs)`
- `delete_queue(farm_id: str, queue_id: str)`

### Job Management (`src/deadline/mcp/tools/job.py`)
- `list_jobs(farm_id: str, queue_id: str)`
- `get_job(farm_id: str, queue_id: str, job_id: str)`
- `submit_job(farm_id: str, queue_id: str, bundle_path: str, **kwargs)`
- `cancel_job(farm_id: str, queue_id: str, job_id: str)`
- `get_job_logs(farm_id: str, queue_id: str, job_id: str)`

## Testing Tools

### Manual Testing
```bash
# Start the MCP server
deadline-mcp

# Test with MCP client (in another terminal)
echo '{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}' | deadline-mcp
```

### Integration Testing
```python
import pytest
from deadline.mcp.server import DeadlineCloudMCPServer

@pytest.mark.asyncio
async def test_list_farms_tool():
    server = DeadlineCloudMCPServer()
    # Test tool functionality
    pass
```

## Best Practices

1. **Consistent Return Format**: Always return JSON strings
2. **Comprehensive Error Handling**: Catch and format all exceptions
3. **Parameter Validation**: Validate inputs before API calls
4. **Documentation**: Include detailed docstrings for all tools
5. **Type Hints**: Use proper type annotations
6. **Logging**: Add appropriate logging for debugging

## Next Steps

1. Implement remaining tools following this pattern
2. Add comprehensive error handling
3. Create integration tests
4. Add logging and debugging support
5. Optimize performance for large datasets
