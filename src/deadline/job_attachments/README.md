# Job attachments

[Job attachments][job-attachments] enable you to transfer files back and forth between your workstation and [AWS Deadline Cloud][deadline-cloud], using an Amazon S3 bucket in your AWS account associated with your [AWS Deadline Cloud queues][queue]. 

Job attachments uses your configured S3 bucket as a [content-addressable storage](https://en.wikipedia.org/wiki/Content-addressable_storage), which creates a snapshot of the files used in your job submission in [asset manifests](#asset-manifests), only uploading files that aren't already in S3. This saves you time and bandwidth when iterating on jobs. When an [AWS Deadline Cloud worker agent][worker-agent] starts working on a job with job attachments, it recreates the file system snapshot in the worker agent session directory, and uploads any outputs back to your S3 bucket. 

You can then easily download your outputs with the [deadline cli](../client/) `deadline job download-output` command, or using the [protocol handler](#protocol-handler) to download from a click of a button in the [AWS Deadline Cloud monitor][monitor].

Job attachments also works as an auxiliary storage when used with [AWS Deadline Cloud storage profiles][shared-storage], allowing you to flexibly upload files to your Amazon S3 bucket that aren't on your configured shared storage.

See the [`examples`](../../../examples/) directory for some simple examples on how to use job attachments.

[job-attachments]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/storage-job-attachments.html
[deadline-cloud]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/what-is-deadline-cloud.html
[queue]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/queues.html
[monitor]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/working-with-deadline-monitor.html
[shared-storage]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/storage-shared.html
[worker-agent]: https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/docs/

## Job Attachments Bucket Structure

The basic structure that job attachments uses in your S3 bucket is as follows:

```
RootPrefix/
    Data/
    Manifests/
```

- `RootPrefix` is the top-level prefix that all job attachments files are written to. This is configurable when you associate your S3 bucket with a queue.
- `Data` is where the files are stored, based on a hash of their contents. This is a fixed prefix used by the job attachments library and is non-configurable.
- `Manifests` is where manifests are stored which are associated with job submissions. This is a fixed prefix used by the job attachments library and is non-configurable.

[ja-security]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/security-best-practices.html#job-attachment-queues

## Asset Manifests

When making a job submission, the job attachments library makes a snapshot of all of the files included in the submission. The contents of each file are hashed, and the files are uploaded to the S3 bucket associated with the queue you are submitting to. This way, if the files haven't changed since a previous submission, the hash will be the same and the files will not be re-uploaded. 

These snapshots are encapsulated in one or more [`asset_manifests`](asset_manifests). Asset manifests include the local file path and associated hash of every file included in the submission, plus some metadata such as the file size and last modified time. Asset manifests are uploaded to your job attachments S3 bucket alongside your files.

When starting work, the worker downloads the manifest associated with your job, and recreates the file structure of your submission locally, either downloading all files at once, or as needed if using the [virtual][vfs] job attachments filesystem type. When a task completes, the worker creates a new manifest for any outputs that were specified in the job submission, and uploads the manifest and the outputs back to your S3 bucket.

Manifest files are written to a `manifests` directory within each job bundle that is added to the job history if submitted through the GUI (default: `~/.deadline/job_history`). A corresponding `manifest_s3_mapping` file is created alongside manifests, which specifies each local manifest file with the S3 manifest path in the submitted job's job attachments metadata.

[vfs]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/storage-virtual.html

## Local Cache Files

In order to further improve submission time, there are currently two local [`caches`](caches), which are simple SQLite databases that cache file information locally. These include:

1. [`Hash Cache`](caches/hash_cache.py): a cache recording a file name and corresponding hash of its contents at a specific time. If a file does not exist in the hash cache, or its last modified time is later than the time in the cache, the file will be hashed and the cache updated.

2. [`S3 Check Cache`](caches/s3_check_cache.py): a 'last seen on S3' cache that records the last time that a specific S3 object was seen. For the case of this library, this will just be a hash and a timestamp of the last time that hash was seen in S3. If a hash does not exist in the cache, or the last check time is expired (currently after 30 days), an S3 head object API call will be made to check if the hash exists in your S3 bucket, and if so, will write to the cache.

## Protocol Handler

On Windows and Linux operating systems, you can choose to install the [Deadline client](../client/) protocol handler in order to run AWS Deadline Cloud commands sent from a web browser. Of note is the ability to download job attachments outputs from your jobs through the [AWS Deadline Cloud monitor][downloading-output]. 

You can install the protocol handler by running the command: `deadline handle-web-url --install`

[downloading-output]: https://docs.aws.amazon.com/deadline-cloud/latest/userguide/download-finished-output.html

## Security

When creating a queue, provide the name of an S3 bucket in the same account and region as the queue you are creating, and provide a 'root prefix' name for files to be uploaded to. You also must provide an IAM role that has access to the S3 bucket. See the [security best practices][ja-security] documentation for more information on securely configuring job attachments.
