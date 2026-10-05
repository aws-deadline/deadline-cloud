# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Copies the Deadline Cloud service model from the installed botocore into
src/deadline/client/api/_botocore_data, the model used on Python versions that
botocore no longer releases for. Run with a supported Python and the latest botocore:

    pip install --upgrade botocore
    python scripts/update_bundled_service_models.py
"""

import os
import shutil
import sys

import botocore

SERVICE_NAMES = ["deadline"]

DESTINATION = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src",
    "deadline",
    "client",
    "api",
    "_botocore_data",
)


def main() -> None:
    if sys.version_info < (3, 10):
        sys.exit("Run this with a Python version that botocore still releases for.")
    source_root = os.path.join(os.path.dirname(botocore.__file__), "data")
    for service_name in SERVICE_NAMES:
        destination = os.path.join(DESTINATION, service_name)
        shutil.rmtree(destination, ignore_errors=True)
        shutil.copytree(os.path.join(source_root, service_name), destination)
        print(f"Copied {service_name} from botocore {botocore.__version__} to {destination}")


if __name__ == "__main__":
    main()
