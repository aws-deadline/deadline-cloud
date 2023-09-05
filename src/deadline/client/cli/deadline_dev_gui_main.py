# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
EXPERIMENTAL

Runs the Deadline Dev GUI. Can be run as a python script file.
"""
from ..config import get_setting


def main() -> None:
    import argparse
    import logging

    from deadline.client.ui.dev_application import app

    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help="Enable debug logging", action="store_true")
    args = parser.parse_args()

    if args.debug:
        log_level = "DEBUG"
    else:
        log_level = get_setting("settings.log_level")
    logging.basicConfig(level=log_level)

    app()


if __name__ == "__main__":
    main()
