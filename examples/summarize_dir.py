#!/usr/bin/env python

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import argparse
import os

from deadline.job_attachments.api import summarize_path_list

"""
This is a sample script that uses the path summarization features to summarize
all the files in a specified directory.

Example usage:

  python summarize_dir.py --max-entries 5 ./mydir
"""


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-entries", type=int, default=10, help="How many entries to limit the summary to."
    )
    parser.add_argument(
        "--file-sizes", default=False, action="store_true", help="Include file sizes."
    )
    parser.add_argument(
        "--skip-dot-paths",
        default=False,
        action="store_true",
        help="Skip directories and files that start with '.'.",
    )
    parser.add_argument(
        "--exclude-totals",
        default=False,
        action="store_true",
        help="Exclude totals from the summary.",
    )
    parser.add_argument(
        "--follow-symlinks",
        default=False,
        action="store_true",
        help="Follows symlinks for directory traversal and file sizes.",
    )
    parser.add_argument("summary_dir", help="The directory to summarize.")

    args = parser.parse_args()

    if not os.path.exists(args.summary_dir) or not os.path.isdir(args.summary_dir):
        print(f"Directory not found: {args.summary_dir}")

    total_size_by_path: dict[str, int] = None
    if args.file_sizes:
        total_size_by_path = {}

    path_list = []

    dirs_to_visit = [args.summary_dir]
    while dirs_to_visit:
        dir = dirs_to_visit.pop()
        for entry in os.scandir(dir):
            if entry.is_dir(follow_symlinks=args.follow_symlinks):
                if not (args.skip_dot_paths and entry.name.startswith(".")):
                    dirs_to_visit.append(entry.path)
            elif entry.is_file(follow_symlinks=args.follow_symlinks):
                if not (args.skip_dot_paths and entry.name.startswith(".")):
                    path_list.append(entry.path)
                    if total_size_by_path is not None:
                        total_size_by_path[entry.path] = entry.stat(
                            follow_symlinks=args.follow_symlinks
                        ).st_size

    if path_list:
        print(
            summarize_path_list(
                path_list,
                total_size_by_path=total_size_by_path,
                max_entries=args.max_entries,
                include_totals=not args.exclude_totals,
            )
        )
    else:
        print(f"No files found in {args.summary_dir}")


if __name__ == "__main__":
    main()
