# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

MOCK_FARM_ID = "farm-0123456789abcdefabcdefabcdefabcd"
MOCK_QUEUE_ID = "queue-0123456789abcdefabcdefabcdefabcd"
MOCK_FLEET_ID = "fleet-7371eafb4bc74c7485c1138947bbe4e6"
MOCK_WORKER_ID = "worker-aaca38a11996485f81d7f819c4f41f2b"
MOCK_BUCKET_NAME = "deadline-job-attachments-mock-bucket"
MOCK_STORAGE_PROFILE_ID = "sp-0123456789abcdefabcdefabcdefabcd"
MOCK_JOB_ID = "job-0123456789abcdefabcdefabcdefabcd"
MOCK_STEP_ID = "step-0123456789abcdefabcdefabcdefabcd"
MOCK_TASK_ID = "task-0123456789abcdefabcdefabcdefabcd-99"
MOCK_PROFILE_NAME = "my-monitor-profile"
MOCK_QUEUES_LIST = [
    {
        "queueId": "queue-0123456789abcdef0123456789abcdef",
        "displayName": "Testing Queue",
        "description": "",
    },
    {
        "queueId": "queue-0123456789abcdef0123456789abcdeg",
        "displayName": "Another Queue",
        "description": "With a description!",
    },
]
MOCK_GET_QUEUE_RESPONSE = {
    "queueId": MOCK_QUEUE_ID,
    "displayName": "Test Queue",
    "description": "",
    "farmId": MOCK_FARM_ID,
    "status": "ACTIVE",
    "logBucketName": MOCK_BUCKET_NAME,
    "jobAttachmentSettings": {
        "s3BucketName": MOCK_BUCKET_NAME,
        "rootPrefix": "AWS Deadline Cloud",
    },
    "sessionRoleArn": "arn:aws:iam::123456789012:role/DeadlineQueueSessionRole",
    "createdAt": "2022-11-22T06:37:35+00:00",
    "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin/user",
    "updatedAt": "2022-11-22T22:26:57+00:00",
    "updatedBy": "0123abcdf-abcd-0123-fa82-0123456abcd1",
}
