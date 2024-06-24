####
# This script demonstrates building on Job Attachments CLI primitives for manifest and attachment handling.
# In this example, we download the manifest for a Job, and the corresponding input files. This can be useful
# to debug the input files of a job.
###

deadline manifest download \
    --farm-id farm-e28af39d92b044f8a34905e2e37af257 \
    --queue-id queue-81a7e66a968c45b5a52a7e8e7edeffca \
    --job-id job-9a3706751fcf43d5ae304bff3f9a00e7 \
    --json \
    .

###
# Output: 
# {
#   downloaded:[
#     [
#       {
#         "s3": "farm-e28af39d92b044f8a34905e2e37af257/queue-81a7e66a968c45b5a52a7e8e7edeffca/Inputs/b3335850cc734bd092f9b2a3af905fca/875ff8acdc14022f4c3315cf6d7b5f06_input",
#         "local": "/Users/leongdl/work/deadline-cloud/-home-ssm-user-deadline-cloud-bundles-blender_car_sample.manifest"
#       }
#     ]
#   ],
#   failed:[
#   ]
# }

# todo: sync outputs CLI loop:
# deadline attachment sync ...

