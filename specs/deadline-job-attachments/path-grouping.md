# Path Grouping

## Overview

`prepare_paths_for_upload` in `upload.rs` groups input/output/referenced paths
by asset root based on storage profile locations. This determines which files
go into which manifest and where they're stored in S3.

## Storage Profile Locations

A `StorageProfile` contains a list of `FileSystemLocation` entries, each with:
- `path` — filesystem path
- `name` — display name
- `location_type` — `LOCAL` or `SHARED`

**LOCAL locations** define grouping boundaries. Files under a LOCAL location
are grouped together into one manifest with that location as the asset root.

**SHARED locations** are filtered out entirely. Files under SHARED locations
are already accessible to workers via network mounts — they don't need to be
uploaded.

## Grouping Algorithm

For each input path:
1. Resolve to absolute path (without following symlinks)
2. Filter: if the path is under any SHARED location → skip
3. Find the matching LOCAL location (longest prefix match)
4. If no LOCAL location matches → group under the filesystem root (`/` or drive root)
5. Add to the group's file list

Each group becomes one `AssetRootGroup` containing:
- The asset root path
- Lists of input files, output directories, and referenced paths

## Path Normalization

`normalize_absolute` resolves `.` and `..` components without following
symlinks. This is important because symlinks are rejected on upload — we
need the logical path, not the physical target.

## AssetUploadGroup

The output of `prepare_paths_for_upload`:
- `asset_groups: Vec<(String, AssetRootGroup)>` — keyed by asset root path
- Total counts for logging

Each `AssetRootGroup` contains:
- `input_filenames: Vec<String>` — files to hash and upload
- `output_directories: Vec<String>` — directories where workers write output
- `referenced_paths: Vec<String>` — paths referenced but not uploaded

## Symlink Handling

Symlinks are rejected on upload. Files are checked via `symlink_metadata()`
before being added to a group. Symlinks are skipped with a warning log.
This is a security measure — a symlink could point outside the asset root
after submission, allowing path traversal.
