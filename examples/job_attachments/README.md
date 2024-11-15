# Deadline Cloud Job Attachments CLI "Beta"

## Disclaimer
The APIs and CLIs are current in BETA polishing phase. While functionally correct, we may change parameter names and adjust the user experience for ease of use.

## Purpose

Deadline Cloud offers the [Job Attachments](https://docs.aws.amazon.com/deadline-cloud/latest/userguide/storage-job-attachments.html) feature to transfer files back and forth between your workstation and AWS Deadline Cloud. For an indepth look into the Job Attachments feature, please refer to the Job Attachments [developer guide](https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/submitting-files-with-a-job.html).

Where does Job Attachments come into use in the Deadline Cloud job life cycle? 1) Job submission, 2) Moving data to workers for job execution, 3) Copying job execution output back to the Job Attachments bucket and 4) Downloading job outputs.

This README outlines CLIs and Python APIs to interface with each step in the Job Attachment process.

Concepts:
- Content Addressable Storage (CAS) - [wiki](https://en.wikipedia.org/wiki/Content-addressable_storage)

## TLDR; Quick start

### Note:
- `--profile` When present, the credentials associated with this profile should grant access to the s3 bucket.
- `--json` is available for all CLIs. results are printed as JSON and can be integrated with tools such as `jq` for scripting.

### Creating a Job Attachments manifest "snapshot" to capture files for upload
```
deadline manifest snapshot \
    --root ./path/to/files \
    --destination ./path/for/snapshot/manifest/ \
    --name data-snapshot \
    --include "*.txt" \                               (Optional)
    --exclude "*.tmp"                                 (Optional)
```

### Uploading a manifest file to S3, useful for subsequent job submissions
```
deadline manifest upload \
    --farm-id farm-12345678123456781234567812345678 \
    --queue-id queue-98765432987654329876543298765432 \
    --s3-manifest-prefix where/to/store/manifests \                         (Optional)
    --json \                                                                (Optional)
    --profile \                                                             (Optional)
    /path/for/snapshot/manifest/data-snapshot-2024-11-15T15-46-40.manifest  (Argument, file to upload)
```

### Uploading files captured by Job Attachment "snapshot" to S3
```
deadline attachment upload \
    --manifests "/path/to/job/attachment.manifest" \
    --s3-root-uri s3://my-queue/DeadlineCloud \         (Optional)(Can also supply --farm-id and --queue-id)
    --json                                              (Optional)
    --profile                                           (Optional)
```

### Visually show a "diff" of the current directory content against a previous manifest snapshot
```
deadline manifest diff \
    --root ./path/to/files \
    --manifest /path/for/snapshot/manifest/data-snapshot-2024-11-15T15-46-40.manifest \
    --include "*.txt" \                                   (Optional)
    --exclude "*.tmp" \                                   (Optional)
    --json                                                (Optional)
```

### Creating a "diff" manifest of the current directory content against a previous manifest snapshot
```
deadline manifest snapshot \
    --diff /path/for/snapshot/manifest/data-snapshot-2024-11-15T15-46-40.manifest \
    --root ./path/to/files \
    --destination ./path/for/snapshot/manifest/ \
    --name data-snapshot-diff \
    --include "*.txt" \                               (Optional)
    --exclude "*.tmp"                                 (Optional)
```

### Downloading Job Attachment manifest for a Job (All job inputs), or Step (All job inputs and outputs of the step including dependencies)
```
deadline manifest download \
    --farm-id farm-abcdabcdabcdabcdabcdabcdabcdabcd \
    --queue-id queue-11111111111111111111111111111111 \
    --job-id job-22222222222222222222222222222222 \
    --step-id step-33333333333333333333333333333333 \   (Optional)
    --json \                                            (Optional)
    --profile \                                         (Optional)
    ./download/directory
```

### Download files referenced by the Job Attachment manifest
```
deadline attachment download \
    --manifests "/path/to/job/attachment.manifest" \
    --s3-root-uri s3://my-queue/DeadlineCloud \         (or Farm and Queue)
    --farm-id farm-abcdabcdabcdabcdabcdabcdabcdabcd \   (or S3 Root URI) 
    --queue-id queue-11111111111111111111111111111111 \ (or S3 Root URI)
    --json \                                            (Optional)
    --profile                                           (Optional)
```


## Target Use cases
### 1. Pre-caching files to S3 to speed up job submission. (DCC Submitters, Job Bundles)

Uploading job attachment files at job submission time can be a time consuming process. Upload is limited by the submitter user's network bandwidth, and may fail due to flaky connectivity. For example, an artist on a laptop may have intermittent (or slow) WiFi. Pre-caching assets periodically via a dedicated upload workstation will reduce job submission time. Any unchanged assets will not be re-uploaded to S3, thus allowing for immediate job creation.

### 2. Download Job Outputs back to shared file systems on Job Completion event bridge [events](https://docs.aws.amazon.com/deadline-cloud/latest/userguide/eventbridge-integration.html).

When a job completes successfully, users need to download the results back to their workstation or shared file storage for review. Downloading large output files maybe time consuming. Also, users have asked for a way to programatically copy outputs to folders automatically for archival or review.

### 3. Reproduce and debug job execution failures.

A pain points we have encountered is how to debug a job that is run on the cloud. Sometimes logs do not contain sufficient debugging information. We need a way to reproduce the environment, run some CLI tests to "change, debug and  iterate" rapidly.

# What are the CLIs and APIs?

## Common features

- All CLIs have an equivalent API with the same input signature. 
- All CLIs also support a `--json` output mode to simplify scripting integration.
- All CLIs which interact with AWS cloud resources support a `--profile` to specify the AWS credentials profile.

## CLI Commands

#### `manifest`
- `deadline manifest snapshot`  
Create a Job Attachment manifest for a folder (root).
- `deadline manifest snapshot --diff`
Createa a Job Attachment manifest for a folder (root) incrementally diffed to a manifest.
- `deadline manifest diff`
Render a file folder structure tree showing new, modified and deleted files compared to a manifest.
- `deadline manifest download`
Download a manifest file from S3. Assumes Deadline Queue Credentials to access S3 bucket, or use `--profile` AWS profile specified via input.
- `deadline manifest upload`
Upload a manifest file to S3. Uploaded manifests can be used for subsequent programatic job submission.  Assumes Deadline Queue Credentials to access S3 bucket, or use `--profile` AWS profile specified via input.

#### `attachment`
- `deadline attachment upload`
Uploads all attachment files for one or more manifests. Files already uploaded with the same hash should not be uploaded. This is used in conjunction with `deadline manifest snapshot`
- `deadline attachment download`
Downloads all attachment files for one or more manifests. This is used in conjunction with `deadline manifest download`


### Use case: Pre-caching files to S3 to speed up job submission

When a job is submitted via the `deadline` CLI, the asset manifest file [asset_references.yaml](https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/build-job-bundle-assets.html) define all input files required to process a job. For each file, Job Attachments will upload the file to S3, uniquely named as the hash of the file. Job Attachments will store the metadata of all files in a [manifest](https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/run-jobs-job-attachments.html#job-attachments-in-depth) that is also uploaded to S3. Uploading assets at submission time can be time consuming. It is recommended to pre-cache common assets to Job Attachment S3 to improve job submission experience.

Three CLIs / APIs are offered to support the Job Submission asset management use case. 

#### Creating a Job Attachments manifest.

```
deadline manifest snapshot \
    --root ./path/to/files \
    --destination ./path/for/snapshot/manifest/ \
    --name data-snapshot
```

Example:
```

```

#### Uploading a manifest snapshot to S3
```
deadline manifest upload \
    --farm-id farm-12345678123456781234567812345678 \
    --queue-id queue-98765432987654329876543298765432 \
    --s3-manifest-prefix where/to/store/manifests \                         (Optional)
    /path/for/snapshot/manifest/data-snapshot-2024-11-15T15-46-40.manifest
```

Example:
```

```

#### Finding the "diff" of a manifest visually and creating a "diff" manifest

```
deadline manifest diff \
    --root ./path/to/files \
    --manifest /path/for/snapshot/manifest/data-snapshot-2024-11-15T15-46-40.manifest
```

```
deadline manifest snapshot \
    --root ./path/to/files \
    --destination ./path/for/snapshot/manifest/ \
    --name data-snapshot \
    --diff /path/for/snapshot/manifest/data-snapshot-2024-11-15T15-46-40.manifest
```

#### Uploading the assets of a snapshot to S3

### Use Case: Download Job Outputs back to shared file systems on Job Completion, and downloading Job inputs locally for debugging.

```
deadline manifest download \
    --farm-id farm-abcdabcdabcdabcdabcdabcdabcdabcd \
    --queue-id queue-11111111111111111111111111111111 \
    --job-id job-22222222222222222222222222222222 \
    ./download/directory
```

```
deadline attachment download \
```

Example:
```

```

## APIs

APIs are exposed in the `deadline` `job_attachment` `manifest` and `attachment` namespaces. Note that APIs are currently *BETA*, so APIs are prefixed with `_`. After additional polishing, APIs will be renamed to remove `_`.

Example:
```
from deadline.job_attachments.api.manifest import _manifest_snapshot, _manifest_diff, _manifest_download, _manifest_upload
from deadline.job_attachments.api.attachment import attachment_upload attachment_download
```

## Next Steps & Future development.

The CLIs are currently denoted with "BETA". Any feedback are welcomed to improve the functionality. 

### Debugging Failed Jobs 

One of the pain points we (and users) have encountered is how to debug a job that is run on the cloud. Users need a way to reproduce the job environment, run tests to quickly "change, test and iterate". This can be accomplished through the following steps:
1. Find all the attachment files required to run the specific step of a job
2. Download all the assets to the local debug computer.
3. Run an OpenJobDescription session / command associated with a Job's step.

Today, `deadline manifest download` will support item 1. `deadline attachment download` will support item 2. In the future, a new command will be introduced to produce a shell script to run a specific session's command via OpenJobDescription sessions.