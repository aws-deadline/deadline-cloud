"""
Farm Management Tools for AWS Deadline Cloud MCP Server

These tools correspond to the `deadline farm` CLI commands.
"""

import json
from deadline.client import api
from ..server import mcp


@mcp.tool()
def list_farms() -> str:
    """List all available AWS Deadline Cloud farms"""
    try:
        result = api.list_farms()
        farms = result.get('farms', [])
        
        if not farms:
            return "No farms found. Make sure you have AWS credentials configured and access to AWS Deadline Cloud."
        
        # Format the response
        farm_list = []
        for farm in farms:
            farm_info = f"• {farm.get('displayName', 'Unknown')} (ID: {farm.get('farmId', 'Unknown')})"
            if 'description' in farm:
                farm_info += f"\n  Description: {farm['description']}"
            farm_list.append(farm_info)
        
        response = f"Found {len(farms)} farm(s):\n\n" + "\n\n".join(farm_list)
        return response
        
    except ImportError as e:
        return f"AWS Deadline Cloud client not properly installed: {str(e)}"
    except Exception as e:
        return f"Error listing farms: {str(e)}\n\nMake sure you have:\n1. AWS credentials configured\n2. Access to AWS Deadline Cloud\n3. Run 'deadline auth status' to check authentication"


@mcp.tool()
def get_farm(farm_id: str) -> str:
    """Get detailed information about a specific farm
    
    Args:
        farm_id: The ID of the farm to retrieve
    """
    try:
        farm = api.get_farm(farmId=farm_id)
        return json.dumps(farm, indent=2)
    except Exception as e:
        return f"Error getting farm {farm_id}: {str(e)}"


@mcp.tool()
def create_farm(display_name: str, description: str = None) -> str:
    """Create a new farm
    
    Args:
        display_name: The display name for the farm
        description: Optional description for the farm
    """
    try:
        kwargs = {"displayName": display_name}
        if description:
            kwargs["description"] = description
        
        farm = api.create_farm(**kwargs)
        return json.dumps(farm, indent=2)
    except Exception as e:
        return f"Error creating farm '{display_name}': {str(e)}"


@mcp.tool()
def update_farm(farm_id: str, display_name: str = None, description: str = None) -> str:
    """Update an existing farm
    
    Args:
        farm_id: The ID of the farm to update
        display_name: New display name for the farm (optional)
        description: New description for the farm (optional)
    """
    try:
        kwargs = {"farmId": farm_id}
        if display_name:
            kwargs["displayName"] = display_name
        if description:
            kwargs["description"] = description
        
        farm = api.update_farm(**kwargs)
        return json.dumps(farm, indent=2)
    except Exception as e:
        return f"Error updating farm {farm_id}: {str(e)}"


@mcp.tool()
def delete_farm(farm_id: str) -> str:
    """Delete a farm
    
    Args:
        farm_id: The ID of the farm to delete
    """
    try:
        result = api.delete_farm(farmId=farm_id)
        return json.dumps({"success": True, "message": f"Farm {farm_id} deleted successfully"}, indent=2)
    except Exception as e:
        return f"Error deleting farm {farm_id}: {str(e)}"
