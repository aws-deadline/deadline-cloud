#!/usr/bin/env python3
"""
Comprehensive test script for the Deadline Cloud MCP server.
This script tests all available MCP tools without requiring a full MCP client.
"""

import asyncio
import json
import subprocess
import sys
import time
from typing import Dict, Any, List, Optional, Tuple


class MCPTester:
    """Test harness for the Deadline Cloud MCP server."""
    
    def __init__(self):
        self.process = None
        self.request_id = 0
        self.test_results = []
        self.available_tools = []
        self.farm_id = None  # Will be populated from list_farms if available
        self.queue_id = None  # Will be populated from list_queues if available
    
    def get_next_id(self) -> int:
        """Get the next request ID."""
        self.request_id += 1
        return self.request_id
    
    async def start_server(self) -> bool:
        """Start the MCP server process."""
        try:
            self.process = subprocess.Popen(
                ['deadline-mcp'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            # Give the server a moment to start
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            print(f"❌ Failed to start MCP server: {e}")
            return False
    
    def send_request(self, method: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Send a JSON-RPC request and get the response."""
        if not self.process:
            return None
        
        request = {
            "jsonrpc": "2.0",
            "id": self.get_next_id(),
            "method": method,
            "params": params or {}
        }
        
        try:
            self.process.stdin.write(json.dumps(request) + '\n')
            self.process.stdin.flush()
            
            # Read response with timeout
            response_line = self.process.stdout.readline()
            if response_line.strip():
                return json.loads(response_line.strip())
        except Exception as e:
            print(f"❌ Error sending request {method}: {e}")
        
        return None
    
    def send_notification(self, method: str, params: Dict[str, Any] = None):
        """Send a JSON-RPC notification (no response expected)."""
        if not self.process:
            return
        
        notification = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {}
        }
        
        try:
            self.process.stdin.write(json.dumps(notification) + '\n')
            self.process.stdin.flush()
        except Exception as e:
            print(f"❌ Error sending notification {method}: {e}")
    
    def call_tool(self, tool_name: str, arguments: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Call a specific tool."""
        return self.send_request("tools/call", {
            "name": tool_name,
            "arguments": arguments or {}
        })
    
    def extract_tool_result(self, response: Dict[str, Any]) -> str:
        """Extract the text result from a tool call response."""
        if not response:
            return "No response"
        
        if 'error' in response:
            return f"Error: {response['error']}"
        
        content = response.get('result', {}).get('content', [])
        for item in content:
            if item.get('type') == 'text':
                return item.get('text', 'No text content')
        
        return "No text content found"
    
    def record_test(self, test_name: str, success: bool, details: str = ""):
        """Record a test result."""
        self.test_results.append({
            'name': test_name,
            'success': success,
            'details': details
        })
        
        status = "✅" if success else "❌"
        print(f"{status} {test_name}")
        if details:
            print(f"   {details}")
    
    async def test_initialization(self) -> bool:
        """Test server initialization."""
        print("📡 Test 1: Server Initialization")
        
        response = self.send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "comprehensive-test-client",
                "version": "1.0.0"
            }
        })
        
        if response and 'result' in response:
            server_info = response['result'].get('serverInfo', {})
            server_name = server_info.get('name', 'Unknown')
            self.record_test("Server Initialization", True, f"Server: {server_name}")
            
            # Send the initialized notification (required by MCP protocol)
            self.send_notification("notifications/initialized")
            
            # Give the server a moment to process the notification
            await asyncio.sleep(0.5)
            
            return True
        else:
            self.record_test("Server Initialization", False, "No valid response")
            return False
    
    async def test_list_tools(self) -> bool:
        """Test listing available tools."""
        print("\n🔧 Test 2: List Available Tools")
        
        response = self.send_request("tools/list")
        
        if response and 'result' in response:
            tools = response['result'].get('tools', [])
            self.available_tools = [tool.get('name') for tool in tools]
            
            self.record_test("List Tools", True, f"Found {len(tools)} tools")
            for tool in tools:
                print(f"   • {tool.get('name', 'Unknown')}: {tool.get('description', 'No description')}")
            return True
        else:
            self.record_test("List Tools", False, "No tools found")
            return False
    
    async def test_authentication_tools(self):
        """Test authentication-related tools."""
        print("\n🔐 Test 3: Authentication Tools")
        
        # Test auth_status
        if 'auth_status' in self.available_tools:
            response = self.call_tool('auth_status')
            result = self.extract_tool_result(response)
            success = response and 'error' not in response
            self.record_test("auth_status", success, result[:100] + "..." if len(result) > 100 else result)
        else:
            self.record_test("auth_status", False, "Tool not available")
    
    async def test_farm_management_tools(self):
        """Test farm management tools."""
        print("\n🏭 Test 4: Farm Management Tools")
        
        # Test list_farms
        if 'list_farms' in self.available_tools:
            response = self.call_tool('list_farms')
            result = self.extract_tool_result(response)
            success = response and 'error' not in response
            self.record_test("list_farms", success, result[:100] + "..." if len(result) > 100 else result)
            
            # Try to extract a farm ID for further testing
            if success and result:
                try:
                    farms_data = json.loads(result)
                    if isinstance(farms_data, dict) and 'farms' in farms_data:
                        farms = farms_data['farms']
                        if farms and len(farms) > 0:
                            self.farm_id = farms[0].get('farmId')
                            print(f"   📝 Using farm ID for further tests: {self.farm_id}")
                except:
                    pass
        else:
            self.record_test("list_farms", False, "Tool not available")
        
        # Test get_farm (if we have a farm ID)
        if 'get_farm' in self.available_tools and self.farm_id:
            response = self.call_tool('get_farm', {'farm_id': self.farm_id})
            result = self.extract_tool_result(response)
            success = response and 'error' not in response
            self.record_test("get_farm", success, result[:100] + "..." if len(result) > 100 else result)
        elif 'get_farm' in self.available_tools:
            self.record_test("get_farm", False, "No farm ID available for testing")
        
        # Test create_farm (with a test name)
        if 'create_farm' in self.available_tools:
            test_farm_name = f"test-farm-{int(time.time())}"
            response = self.call_tool('create_farm', {
                'display_name': test_farm_name,
                'description': 'Test farm created by MCP test suite'
            })
            result = self.extract_tool_result(response)
            success = response and 'error' not in response
            self.record_test("create_farm", success, result[:100] + "..." if len(result) > 100 else result)
        
        # Test update_farm and delete_farm would require more careful handling
        # to avoid affecting production resources, so we'll skip them in this test
    
    async def test_queue_management_tools(self):
        """Test queue management tools."""
        print("\n📋 Test 5: Queue Management Tools")
        
        # Test list_queues (requires farm_id)
        if 'list_queues' in self.available_tools and self.farm_id:
            response = self.call_tool('list_queues', {'farm_id': self.farm_id})
            result = self.extract_tool_result(response)
            success = response and 'error' not in response
            self.record_test("list_queues", success, result[:100] + "..." if len(result) > 100 else result)
            
            # Try to extract a queue ID for further testing
            if success and result:
                try:
                    queues_data = json.loads(result)
                    if isinstance(queues_data, dict) and 'queues' in queues_data:
                        queues = queues_data['queues']
                        if queues and len(queues) > 0:
                            self.queue_id = queues[0].get('queueId')
                            print(f"   📝 Using queue ID for further tests: {self.queue_id}")
                except:
                    pass
        elif 'list_queues' in self.available_tools:
            self.record_test("list_queues", False, "No farm ID available for testing")
        
        # Test get_queue (if we have both farm_id and queue_id)
        if 'get_queue' in self.available_tools and self.farm_id and self.queue_id:
            response = self.call_tool('get_queue', {
                'farm_id': self.farm_id,
                'queue_id': self.queue_id
            })
            result = self.extract_tool_result(response)
            success = response and 'error' not in response
            self.record_test("get_queue", success, result[:100] + "..." if len(result) > 100 else result)
        elif 'get_queue' in self.available_tools:
            self.record_test("get_queue", False, "Missing farm_id or queue_id for testing")
        
        # Test create_queue (with a test name)
        if 'create_queue' in self.available_tools and self.farm_id:
            test_queue_name = f"test-queue-{int(time.time())}"
            response = self.call_tool('create_queue', {
                'farm_id': self.farm_id,
                'display_name': test_queue_name,
                'description': 'Test queue created by MCP test suite'
            })
            result = self.extract_tool_result(response)
            success = response and 'error' not in response
            self.record_test("create_queue", success, result[:100] + "..." if len(result) > 100 else result)
        elif 'create_queue' in self.available_tools:
            self.record_test("create_queue", False, "No farm ID available for testing")
    
    async def test_job_management_tools(self):
        """Test job management tools (if implemented)."""
        print("\n💼 Test 6: Job Management Tools")
        
        job_tools = ['list_jobs', 'get_job', 'submit_job', 'cancel_job', 'get_job_logs']
        
        for tool_name in job_tools:
            if tool_name in self.available_tools:
                # For job tools, we'll just test that they exist and can be called
                # without causing server crashes (actual testing would require job data)
                if tool_name == 'list_jobs' and self.farm_id and self.queue_id:
                    response = self.call_tool(tool_name, {
                        'farm_id': self.farm_id,
                        'queue_id': self.queue_id
                    })
                    result = self.extract_tool_result(response)
                    success = response and 'error' not in response
                    self.record_test(tool_name, success, result[:100] + "..." if len(result) > 100 else result)
                else:
                    self.record_test(tool_name, False, "Tool available but not tested (requires job data)")
            else:
                self.record_test(tool_name, False, "Tool not implemented yet")
    
    async def test_fleet_management_tools(self):
        """Test fleet management tools (if implemented)."""
        print("\n🚢 Test 7: Fleet Management Tools")
        
        fleet_tools = ['list_fleets', 'get_fleet', 'create_fleet', 'update_fleet']
        
        for tool_name in fleet_tools:
            if tool_name in self.available_tools:
                if tool_name == 'list_fleets' and self.farm_id:
                    response = self.call_tool(tool_name, {'farm_id': self.farm_id})
                    result = self.extract_tool_result(response)
                    success = response and 'error' not in response
                    self.record_test(tool_name, success, result[:100] + "..." if len(result) > 100 else result)
                else:
                    self.record_test(tool_name, False, "Tool available but not tested (requires fleet data)")
            else:
                self.record_test(tool_name, False, "Tool not implemented yet")
    
    async def test_error_handling(self):
        """Test error handling with invalid parameters."""
        print("\n⚠️  Test 8: Error Handling")
        
        # Test calling a tool with invalid parameters
        if 'get_farm' in self.available_tools:
            response = self.call_tool('get_farm', {'farm_id': 'invalid-farm-id'})
            result = self.extract_tool_result(response)
            # We expect this to fail gracefully
            success = 'error' in result.lower() or 'not found' in result.lower()
            self.record_test("Error handling (invalid farm_id)", success, result[:100] + "..." if len(result) > 100 else result)
        
        # Test calling a non-existent tool
        response = self.call_tool('non_existent_tool')
        success = response and 'error' in response
        error_msg = "Tool call properly rejected" if success else "Tool call should have been rejected"
        self.record_test("Error handling (non-existent tool)", success, error_msg)
    
    def print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 60)
        print("🧪 TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for result in self.test_results if result['success'])
        total = len(self.test_results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {total - passed}")
        print(f"Success Rate: {(passed/total)*100:.1f}%")
        
        print("\n📊 Detailed Results:")
        for result in self.test_results:
            status = "✅" if result['success'] else "❌"
            print(f"{status} {result['name']}")
        
        if passed == total:
            print("\n🎉 All tests passed! Your MCP server is working correctly.")
            print("\nNext steps:")
            print("1. Configure your LLM tool (Cline, Claude Desktop, etc.)")
            print("2. Add the server configuration:")
            print('   {"command": "deadline-mcp", "args": []}')
            print("3. Start using natural language to interact with AWS Deadline Cloud!")
        else:
            print(f"\n⚠️  {total - passed} tests failed. Check the error messages above.")
    
    def cleanup(self):
        """Clean up the server process."""
        if self.process:
            self.process.terminate()
            self.process.wait()


async def test_mcp_server():
    """Run comprehensive MCP server tests."""
    print("🧪 AWS Deadline Cloud MCP Server - Comprehensive Test Suite")
    print("=" * 60)
    
    tester = MCPTester()
    
    try:
        # Start the server
        if not await tester.start_server():
            return False
        
        # Run all tests
        await tester.test_initialization()
        await tester.test_list_tools()
        await tester.test_authentication_tools()
        await tester.test_farm_management_tools()
        await tester.test_queue_management_tools()
        await tester.test_job_management_tools()
        await tester.test_fleet_management_tools()
        await tester.test_error_handling()
        
        # Print summary
        tester.print_summary()
        
        # Return success if all critical tests passed
        critical_tests = ['Server Initialization', 'List Tools', 'auth_status', 'list_farms']
        critical_passed = sum(1 for result in tester.test_results 
                            if result['name'] in critical_tests and result['success'])
        
        return critical_passed == len(critical_tests)
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        return False
    
    finally:
        tester.cleanup()


def main():
    """Run the comprehensive MCP server test suite."""
    print("AWS Deadline Cloud MCP Server - Comprehensive Test Suite")
    print("This will test all available MCP tools and functionality.\n")
    
    success = asyncio.run(test_mcp_server())
    
    if not success:
        print("\n❌ Critical tests failed. Check the error messages above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
