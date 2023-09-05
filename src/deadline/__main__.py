# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
This file makes "python -m deadline ..." equivalent to "deadline ..."
"""
import sys
from .client.cli import main

# Override the program name to always be "deadline"
sys.exit(main())
