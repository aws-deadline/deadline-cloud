# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Runs the Deadline CLI. Can be run as a python script file.
Required by pyinstaller, do not delete.
"""

from deadline.client.cli._main import deadline as main

__all__ = ["main"]

if __name__ == "__main__":
    main()
