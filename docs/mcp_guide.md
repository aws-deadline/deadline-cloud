# AWS Deadline Cloud MCP Server Guide

**MCP ([Model Context Protocol](https://modelcontextprotocol.io/docs/getting-started/intro))** is an open standard that enables AI assistants to securely connect to external data sources and tools. It acts a bridge that allows Large Language Models (LLMs) like Claude, GPT, or other AI assistants to interact with applications and services through natural language.

With the AWS Deadline Cloud MCP Server, you can use natural language for various Deadline Cloud workflows such as submitting a job, reading job/farm/queue information etc.

## User Guide

1. **Install the server** 
  ```bash
   pip install 'deadline[mcp]'
   ```
2. **Verify the MCP server command is available:**
   ```bash
   deadline mcp-server --help
   ```
   You should see:
   ```
   Usage: deadline mcp-server [OPTIONS]

   Start the AWS Deadline Cloud MCP (Model Context Protocol) server.

   The MCP server provides LLM tools with access to AWS Deadline Cloud
   operations through the Model Context Protocol. This allows AI assistants to
   interact with Deadline Cloud services on your behalf.

   The server will run until interrupted with Ctrl+C or Ctrl+D.
   ```
3. **Configure AWS credentials:**
   - Standard AWS credentials (AWS Profiles, environment variables)
   - Or Deadline Cloud monitor credentials:
     ```bash
     deadline auth login
     ```
   - Verify authentication status:
     ```bash
     deadline auth status
     ```
4. **Configure your MCP client** to connect to the server 
    Add to your MCP configuration (e.g., `./settings/mcp.json`):
    ```json
    {
      "mcpServers": {
        "deadline-cloud": {
          "command": "deadline",
          "args": ["mcp-server"],
          "disabled": false,
          "autoApprove": []
        }
      }
    }
    ```
5. **Start having conversations** with your AI assistant about your rendering workflows

## Example prompts

```
- "List all my AWS Deadline Cloud farms"
- "Show me the queues in my farm"
- "List the jobs in my queue"
- "Submit the render job in /path/to/my-job-bundle"
- "Submit a job with priority 80 to my render queue"
- "Show me the status of job job-3a907bac684841f69fc344867ee166de"
- "Get the logs for session session-abc123 to see why my task failed"
- "Show me the CloudWatch logs for session session-xyz789"
- "Download output from job job-3a907bac684841f69fc344867ee166de"
- "Download output from step step-render in job job-3a907bac684841f69fc344867ee166de"
```

## Available Tools

The MCP server provides access to all allowlisted/configured Deadline Cloud API functions through automatic registration:

- `deadline_list_farms()`: List available farms
- `deadline_list_queues()`: List queues in a farm  
- `deadline_list_jobs()`: List jobs in a queue
- `deadline_list_fleets()`: List fleets in a farm
- `deadline_list_storage_profiles_for_queue()`: List storage profiles for a queue

- `deadline_check_authentication_status()`: Check current authentication status
- `deadline_get_session_logs()`: Get CloudWatch logs for a specific session
- `deadline_submit_job()`: Submit an Open Job Description job bundle to AWS Deadline Cloud
- `deadline_download_job_output()`: Download job output files from AWS Deadline Cloud


## Developer Guide

The MCP server exposes public Deadline Cloud operations as tools that AI assistants can call directly. Tools are defined in `config.py` where each entry maps a tool name to its corresponding function and parameters. 

## Project Structure

```
src/deadline/mcp/
├── server.py                # Main server with FastMCP setup and auto-registration
├── config.py                # Tool configuration definitions
├── utils.py                 # Auto-registration utilities
└── tools/                   # Tool modules
    └── job.py               # Job management tools (submit, download)
```

### Adding New MCP Tools

#### Step 1: Use public api operations

Prefer consistency with existing CLI patterns by using functions from the public API module `deadline.client.api.*` when available. Use direct boto3 calls when no wrapper exists or when the wrapper doesn't provide the needed functionality. Tools like `submit_job` and `download_job_output` are exceptions because they require job attachment functionality that are not available in the public API.

```python
# ✅ Good: Use existing public API
from deadline.client.api import list_farms, list_queues

# ❌ Bad: Don't use internal modules
from deadline.client._internal.some_module import internal_function
```

#### Step 2: Configure the MCP Tool

Add your new tool to the `API_TOOLS_CONFIG` in `src/deadline/mcp/config.py`:

```python
API_TOOLS_CONFIG: dict[str, ToolConfig] = {
    # ... existing tools ...
    "your_new_tool": {
        "func": api.your_new_function,  # Must be from deadline.client.api
        "params": ["param1", "param2", "optional_param"],  # List all parameters, or None if no params
    },
}
```

#### Step 3: Test the Integration

1. **Unit Tests**: Add tests to `test/unit/deadline_mcp/test_mcp.py`
2. **Integration Tests**: Add tests to `test/integ/deadline_mcp/test_mcp_server_integration.py`
3. **Manual Testing**: Test with real MCP clients
