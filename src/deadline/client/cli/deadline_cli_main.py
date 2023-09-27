# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Runs the Deadline CLI. Can be run as a python script file.
Required by pyinstaller, do not delete.
"""


def main() -> None:
    from deadline.client.cli._deadline_cli import main

    main(
        # Override the program name to always be "deadline"
        prog_name="deadline",
    )


if __name__ == "__main__":
    main()
