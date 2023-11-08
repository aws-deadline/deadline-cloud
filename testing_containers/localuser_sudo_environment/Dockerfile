# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

FROM python:3.9-buster

# Set environment variables, and let our tests know that we are in an
# environment that can run the sudo tests
ENV DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_USER=targetuser
ENV DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_GROUP=targetgroup
ENV DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_USER=disjointuser
ENV DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_GROUP=disjointgroup

# Use a docker volume to mount the root of the repo to this directory
WORKDIR /code

# We set up three users for our tests:
#  1) hostuser -- the user that will be running the pytests.
#  2) targetuser -- the user assumed to be running the job in the tests.
#  3) disjointuser -- a user to be used in cross-account testing.
# These accounts belong to the following groups:
#   hostuser: hostuser, targetgroup
#   targetuser: targetuser, targetgroup
#   disjointuser: disjointuser, disjointgroup
RUN apt-get update && apt-get install sudo && \
    rm -rf /var/lib/apt/lists/* && \
    addgroup ${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_GROUP} &&  \
    useradd -ms /bin/bash -G ${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_GROUP} ${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_USER} && \
    useradd -ms /bin/bash -G ${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_GROUP} hostuser && \
    echo "hostuser ALL=(${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_TARGET_USER},hostuser) NOPASSWD: ALL" > /etc/sudoers.d/hostuser && \
    addgroup ${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_GROUP} && \
    useradd -ms /bin/bash -G ${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_GROUP} ${DEADLINE_JOB_ATTACHMENT_TEST_SUDO_DISJOINT_USER} && \
    chmod 777 /code

WORKDIR /home/hostuser

COPY --chown=hostuser:hostuser run_tests.sh /home/hostuser/

USER hostuser

CMD ["/bin/sh", "-c", "./run_tests.sh"]
