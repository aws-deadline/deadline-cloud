# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from mcp.server.fastmcp import FastMCP

from .utils import register_api_tools

app = FastMCP("deadline-cloud")

register_api_tools(app, prefix="deadline_")


def main():
    app.run()
