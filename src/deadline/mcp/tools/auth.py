"""
Authentication Tools for AWS Deadline Cloud MCP Server

These tools correspond to the `deadline auth` CLI commands.
"""

import json
from deadline.client import api
from ..server import mcp


@mcp.tool()
def auth_status() -> str:
    """Check current authentication status"""
    try:
        # Implementation using existing deadline auth system
        status = api.check_authentication_status()
        
        # Convert the AwsAuthenticationStatus object to a dict
        status_dict = {
            "status": str(status.status) if hasattr(status, 'status') else "unknown",
            "source": str(status.source) if hasattr(status, 'source') else "unknown",
            "api_available": status.api_available if hasattr(status, 'api_available') else False
        }
        
        return json.dumps(status_dict, indent=2)
    except Exception as e:
        return f"Error checking auth status: {str(e)}"
