# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Runs the Deadline CLI. Can be run as a python script file.
"""


def main() -> None:
    from deadline.client.cli.deadline_cli import cli

    cli(
        # Override the program name to always be "deadline"
        prog_name="deadline",
    )


if __name__ == "__main__":
    main()
