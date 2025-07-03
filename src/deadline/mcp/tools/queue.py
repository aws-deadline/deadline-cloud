"""
Queue Management Tools for AWS Deadline Cloud MCP Server

These tools correspond to the `deadline queue` CLI commands.
"""

import json
from deadline.client import api
from ..server import mcp


@mcp.tool()
def list_queues(farm_id: str, status: str = None) -> str:
    """List queues in a specific farm
    
    Args:
        farm_id: The ID of the farm
        status: Optional filter by queue status (e.g., ACTIVE, IDLE, SCHEDULING)
    """
    try:
        kwargs = {"farmId": farm_id}
        if status:
            kwargs["status"] = status
            
        result = api.list_queues(**kwargs)
        queues = result.get('queues', [])
        
        if not queues:
            status_msg = f" with status '{status}'" if status else ""
            return f"No queues found in farm {farm_id}{status_msg}."
        
        # Format the response
        queue_list = []
        for queue in queues:
            queue_info = f"• {queue.get('displayName', 'Unknown')} (ID: {queue.get('queueId', 'Unknown')})"
            if 'status' in queue:
                queue_info += f"\n  Status: {queue['status']}"
            if 'description' in queue:
                queue_info += f"\n  Description: {queue['description']}"
            queue_list.append(queue_info)
        
        status_msg = f" with status '{status}'" if status else ""
        response = f"Found {len(queues)} queue(s) in farm {farm_id}{status_msg}:\n\n" + "\n\n".join(queue_list)
        return response
        
    except Exception as e:
        return f"Error listing queues for farm {farm_id}: {str(e)}"


@mcp.tool()
def get_queue(farm_id: str, queue_id: str) -> str:
    """Get detailed information about a specific queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue to retrieve
    """
    try:
        queue = api.get_queue(farmId=farm_id, queueId=queue_id)
        return json.dumps(queue, indent=2)
    except Exception as e:
        return f"Error getting queue {queue_id} in farm {farm_id}: {str(e)}"


@mcp.tool()
def create_queue(
    farm_id: str,
    display_name: str,
    description: str = None,
    default_budget_action: str = None,
    role_arn: str = None
) -> str:
    """Create a new queue in a farm
    
    Args:
        farm_id: The ID of the farm
        display_name: The display name for the queue
        description: Optional description for the queue
        default_budget_action: Optional default budget action (STOP_SCHEDULING_AND_COMPLETE_TASKS, STOP_SCHEDULING_AND_CANCEL_TASKS)
        role_arn: Optional IAM role ARN for the queue
    """
    try:
        kwargs = {
            "farmId": farm_id,
            "displayName": display_name
        }
        if description:
            kwargs["description"] = description
        if default_budget_action:
            kwargs["defaultBudgetAction"] = default_budget_action
        if role_arn:
            kwargs["roleArn"] = role_arn
        
        queue = api.create_queue(**kwargs)
        return json.dumps(queue, indent=2)
    except Exception as e:
        return f"Error creating queue '{display_name}' in farm {farm_id}: {str(e)}"


@mcp.tool()
def update_queue(
    farm_id: str,
    queue_id: str,
    display_name: str = None,
    description: str = None,
    default_budget_action: str = None,
    role_arn: str = None
) -> str:
    """Update an existing queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue to update
        display_name: New display name for the queue (optional)
        description: New description for the queue (optional)
        default_budget_action: New default budget action (optional)
        role_arn: New IAM role ARN for the queue (optional)
    """
    try:
        kwargs = {
            "farmId": farm_id,
            "queueId": queue_id
        }
        if display_name:
            kwargs["displayName"] = display_name
        if description:
            kwargs["description"] = description
        if default_budget_action:
            kwargs["defaultBudgetAction"] = default_budget_action
        if role_arn:
            kwargs["roleArn"] = role_arn
        
        queue = api.update_queue(**kwargs)
        return json.dumps(queue, indent=2)
    except Exception as e:
        return f"Error updating queue {queue_id} in farm {farm_id}: {str(e)}"


@mcp.tool()
def delete_queue(farm_id: str, queue_id: str) -> str:
    """Delete a queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue to delete
    """
    try:
        result = api.delete_queue(farmId=farm_id, queueId=queue_id)
        return json.dumps({"success": True, "message": f"Queue {queue_id} deleted successfully from farm {farm_id}"}, indent=2)
    except Exception as e:
        return f"Error deleting queue {queue_id} from farm {farm_id}: {str(e)}"
