# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This script gets the changelog notes for the latest version of this package. It makes the following assumptions
1. A file called CHANGELOG.md is in the current directory that has the changelog
2. The changelog file is formatted in a way such that level 2 headers are:
    a. The only indication of the beginning of a version's changelog notes.
    b. Always begin with `## `
3. The changelog file contains the newest version's changelog notes at the top of the file.

Example CHANGELOG.md:
```
## 1.0.0 (2024-02-06)

### BREAKING CHANGES
* **api**: rename all APIs

## 0.1.0 (2024-02-06)

### Features
* **api**: add new api
```

Running this script on the above CHANGELOG.md should return the following contents:
```
## 1.0.0 (2024-02-06)

### BREAKING CHANGES
* **api**: rename all APIs

```
"""
import re

h2 = r"^##\s.*$"
with open("CHANGELOG.md") as f:
    contents = f.read()
matches = re.findall(h2, contents, re.MULTILINE)
changelog = contents[: contents.find(matches[1]) - 1] if len(matches) > 1 else contents
print(changelog)
