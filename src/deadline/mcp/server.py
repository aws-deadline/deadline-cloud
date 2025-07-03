#!/usr/bin/env python3
"""
AWS Deadline Cloud MCP Server

A Model Context Protocol server that provides LLM tools with access to AWS Deadline Cloud operations.
Built with FastMCP for simplified development and maintenance.
"""

import sys
import json
from datetime import datetime
from deadline.client import api

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("MCP dependencies not installed. Please install with: pip install 'deadline[mcp]'", file=sys.stderr)
    sys.exit(1)


# Create the FastMCP server instance
app = FastMCP("deadline-cloud")


def json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def safe_json_dumps(data, **kwargs):
    """Safely serialize data to JSON, handling datetime objects."""
    return json.dumps(data, default=json_serializer, **kwargs)


# Authentication Tools
@app.tool()
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
        
        return safe_json_dumps(status_dict, indent=2)
    except Exception as e:
        return f"Error checking auth status: {str(e)}"


# Farm Management Tools
@app.tool()
def list_farms() -> str:
    """List all available AWS Deadline Cloud farms"""
    try:
        result = api.list_farms()
        farms = result.get('farms', [])
        
        if not farms:
            return "No farms found. Make sure you have AWS credentials configured and access to AWS Deadline Cloud."
        
        return safe_json_dumps(result, indent=2)
    except Exception as e:
        return f"Error listing farms: {str(e)}"


@app.tool()
def get_farm(farm_id: str) -> str:
    """Get detailed information about a specific farm
    
    Args:
        farm_id: The ID of the farm to retrieve
    """
    try:
        # The deadline client API doesn't have a direct get_farm function
        # We'll use list_farms and filter by farm_id
        result = api.list_farms()
        farms = result.get('farms', [])
        
        for farm in farms:
            if farm.get('farmId') == farm_id:
                return safe_json_dumps(farm, indent=2)
        
        return f"Farm with ID {farm_id} not found"
    except Exception as e:
        return f"Error getting farm {farm_id}: {str(e)}"


@app.tool()
def create_farm(display_name: str, description: str = None) -> str:
    """Create a new farm
    
    Args:
        display_name: The display name for the farm
        description: Optional description for the farm
    """
    try:
        # The deadline client API doesn't have a create_farm function
        # This would require direct boto3 calls to the Deadline Cloud service
        return "Error: Farm creation is not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error creating farm: {str(e)}"


@app.tool()
def update_farm(farm_id: str, display_name: str = None, description: str = None) -> str:
    """Update an existing farm
    
    Args:
        farm_id: The ID of the farm to update
        display_name: New display name for the farm
        description: New description for the farm
    """
    try:
        # The deadline client API doesn't have an update_farm function
        return "Error: Farm updates are not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error updating farm {farm_id}: {str(e)}"


@app.tool()
def delete_farm(farm_id: str) -> str:
    """Delete a farm
    
    Args:
        farm_id: The ID of the farm to delete
    """
    try:
        # The deadline client API doesn't have a delete_farm function
        return "Error: Farm deletion is not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error deleting farm {farm_id}: {str(e)}"


# Queue Management Tools
@app.tool()
def list_queues(farm_id: str) -> str:
    """List queues in a specific farm
    
    Args:
        farm_id: The ID of the farm
    """
    try:
        result = api.list_queues(farmId=farm_id)
        queues = result.get('queues', [])
        
        if not queues:
            return f"No queues found in farm {farm_id}."
        
        return safe_json_dumps(result, indent=2)
    except Exception as e:
        return f"Error listing queues for farm {farm_id}: {str(e)}"


@app.tool()
def get_queue(farm_id: str, queue_id: str) -> str:
    """Get detailed information about a specific queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue to retrieve
    """
    try:
        # The deadline client API doesn't have a direct get_queue function
        # We'll use list_queues and filter by queue_id
        result = api.list_queues(farmId=farm_id)
        queues = result.get('queues', [])
        
        for queue in queues:
            if queue.get('queueId') == queue_id:
                return safe_json_dumps(queue, indent=2)
        
        return f"Queue with ID {queue_id} not found in farm {farm_id}"
    except Exception as e:
        return f"Error getting queue {queue_id} in farm {farm_id}: {str(e)}"


@app.tool()
def create_queue(farm_id: str, display_name: str, description: str = None) -> str:
    """Create a new queue in a farm
    
    Args:
        farm_id: The ID of the farm
        display_name: The display name for the queue
        description: Optional description for the queue
    """
    try:
        # The deadline client API doesn't have a create_queue function
        return "Error: Queue creation is not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error creating queue in farm {farm_id}: {str(e)}"


@app.tool()
def update_queue(farm_id: str, queue_id: str, display_name: str = None, description: str = None) -> str:
    """Update an existing queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue to update
        display_name: New display name for the queue
        description: New description for the queue
    """
    try:
        # The deadline client API doesn't have an update_queue function
        return "Error: Queue updates are not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error updating queue {queue_id} in farm {farm_id}: {str(e)}"


@app.tool()
def delete_queue(farm_id: str, queue_id: str) -> str:
    """Delete a queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue to delete
    """
    try:
        # The deadline client API doesn't have a delete_queue function
        return "Error: Queue deletion is not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error deleting queue {queue_id} in farm {farm_id}: {str(e)}"


# Job Management Tools
@app.tool()
def list_jobs(farm_id: str, queue_id: str) -> str:
    """List jobs in a specific queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue
    """
    try:
        result = api.list_jobs(farmId=farm_id, queueId=queue_id)
        jobs = result.get('jobs', [])
        
        if not jobs:
            return f"No jobs found in queue {queue_id} of farm {farm_id}."
        
        return safe_json_dumps(result, indent=2)
    except Exception as e:
        return f"Error listing jobs for queue {queue_id} in farm {farm_id}: {str(e)}"


@app.tool()
def get_job(farm_id: str, queue_id: str, job_id: str) -> str:
    """Get detailed information about a specific job
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue
        job_id: The ID of the job to retrieve
    """
    try:
        # Use boto3 client directly since there's no get_job in the deadline client API
        deadline = api.get_boto3_client("deadline")
        result = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
        return safe_json_dumps(result, indent=2)
    except Exception as e:
        return f"Error getting job {job_id} in queue {queue_id} of farm {farm_id}: {str(e)}"


@app.tool()
def submit_job(farm_id: str, queue_id: str, bundle_path: str, job_name: str = None, priority: int = 50) -> str:
    """Submit a job bundle to a queue
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue
        bundle_path: Path to the job bundle directory
        job_name: Optional name for the job
        priority: Job priority (0-100, default: 50)
    """
    try:
        # The deadline client API doesn't have a direct submit_job function
        # This would require using the job bundle submission functionality
        return "Error: Job submission through MCP is not yet implemented. Use 'deadline bundle submit' command instead."
    except Exception as e:
        return f"Error submitting job: {str(e)}"


@app.tool()
def cancel_job(farm_id: str, queue_id: str, job_id: str) -> str:
    """Cancel a running job
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue
        job_id: The ID of the job to cancel
    """
    try:
        # The deadline client API doesn't have a direct cancel_job function
        return "Error: Job cancellation is not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error cancelling job {job_id}: {str(e)}"


@app.tool()
def get_job_logs(farm_id: str, queue_id: str, job_id: str) -> str:
    """Retrieve job execution logs
    
    Args:
        farm_id: The ID of the farm
        queue_id: The ID of the queue
        job_id: The ID of the job to get logs for
    """
    try:
        # The deadline client API doesn't have a direct get_job_logs function
        return "Error: Job log retrieval is not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error getting logs for job {job_id}: {str(e)}"


# Fleet Management Tools
@app.tool()
def list_fleets(farm_id: str) -> str:
    """List fleets in a specific farm
    
    Args:
        farm_id: The ID of the farm
    """
    try:
        result = api.list_fleets(farmId=farm_id)
        fleets = result.get('fleets', [])
        
        if not fleets:
            return f"No fleets found in farm {farm_id}."
        
        return safe_json_dumps(result, indent=2)
    except Exception as e:
        return f"Error listing fleets for farm {farm_id}: {str(e)}"


@app.tool()
def get_fleet(farm_id: str, fleet_id: str) -> str:
    """Get detailed information about a specific fleet
    
    Args:
        farm_id: The ID of the farm
        fleet_id: The ID of the fleet to retrieve
    """
    try:
        # Use boto3 client directly since there's no get_fleet in the deadline client API
        deadline = api.get_boto3_client("deadline")
        result = deadline.get_fleet(farmId=farm_id, fleetId=fleet_id)
        return safe_json_dumps(result, indent=2)
    except Exception as e:
        return f"Error getting fleet {fleet_id} in farm {farm_id}: {str(e)}"


@app.tool()
def create_fleet(farm_id: str, display_name: str, description: str = None) -> str:
    """Create a new fleet in a farm
    
    Args:
        farm_id: The ID of the farm
        display_name: The display name for the fleet
        description: Optional description for the fleet
    """
    try:
        # The deadline client API doesn't have a create_fleet function
        return "Error: Fleet creation is not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error creating fleet in farm {farm_id}: {str(e)}"


@app.tool()
def update_fleet(farm_id: str, fleet_id: str, display_name: str = None, description: str = None) -> str:
    """Update an existing fleet
    
    Args:
        farm_id: The ID of the farm
        fleet_id: The ID of the fleet to update
        display_name: New display name for the fleet
        description: New description for the fleet
    """
    try:
        # The deadline client API doesn't have an update_fleet function
        return "Error: Fleet updates are not supported through the deadline client API. Use the AWS CLI or console instead."
    except Exception as e:
        return f"Error updating fleet {fleet_id} in farm {farm_id}: {str(e)}"


# Configuration Tools
@app.tool()
def list_config() -> str:
    """Show current configuration settings"""
    try:
        # Import the config module to get current settings
        from deadline.client.config import config_file
        
        config_data = config_file.read_config()
        
        # Convert config to a more readable format
        config_dict = {}
        for section_name in config_data.sections():
            section = config_data[section_name]
            for key, value in section.items():
                config_dict[f"{section_name}.{key}"] = value
        
        return safe_json_dumps(config_dict, indent=2)
    except Exception as e:
        return f"Error reading configuration: {str(e)}"


@app.tool()
def set_config(setting_name: str, value: str) -> str:
    """Update a configuration value
    
    Args:
        setting_name: The name of the setting to update (e.g., 'defaults.farm_id')
        value: The new value for the setting
    """
    try:
        # The deadline client API doesn't have a direct set_config function
        return "Error: Configuration updates are not supported through the MCP API. Use 'deadline config set' command instead."
    except Exception as e:
        return f"Error setting configuration {setting_name}: {str(e)}"


@app.tool()
def get_credentials() -> str:
    """Export queue credentials for programmatic access"""
    try:
        # The deadline client API doesn't have a direct get_credentials function
        return "Error: Credential export is not supported through the MCP API. Use 'deadline queue export-credentials' command instead."
    except Exception as e:
        return f"Error getting credentials: {str(e)}"


def main():
    """Main entry point for the MCP server."""
    # Start the server
    app.run()


if __name__ == "__main__":
    main()
