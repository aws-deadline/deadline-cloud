# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import re
from pathlib import Path

# Unpacking a queue's job attachment settings into JobAttachmentS3Settings(...) raises TypeError
# when the service adds a field. Pass s3BucketName= and rootPrefix= explicitly instead.
_UNPACKED_SETTINGS_RE = re.compile(r"JobAttachmentS3Settings\(\s*\*\*")


def test_no_unpacked_job_attachment_settings():
    root_project_dir = Path(__file__)
    # The root of the project is the directory that contains the test directory.
    while not (root_project_dir / "test").exists():
        root_project_dir = root_project_dir.parent

    source_files = list((root_project_dir / "src").glob("**/*.py"))
    assert source_files, "Found no source files to check"
    offenders = [
        str(path.relative_to(root_project_dir))
        for path in source_files
        if _UNPACKED_SETTINGS_RE.search(path.read_text(encoding="utf8"))
    ]
    assert offenders == [], (
        "Pass s3BucketName= and rootPrefix= to JobAttachmentS3Settings instead of unpacking settings with **"
        f" in: {', '.join(offenders)}"
    )
