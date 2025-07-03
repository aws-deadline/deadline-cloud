"""
AWS Deadline Cloud MCP Tools

This package contains all the MCP tools organized by functional area.
Each module registers its tools with the FastMCP server instance.
"""

# Import all tool modules to register them with FastMCP
# The order doesn't matter as FastMCP handles tool registration automatically
from . import auth
from . import farm  
from . import queue

__all__ = ["auth", "farm", "queue"]
