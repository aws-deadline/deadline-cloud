# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
MockDeadlineBackend - In-memory Deadline Cloud simulator for testing.

See docs/design/deadline-tests-mock-backend.md for design details.
"""

from __future__ import annotations
import json as _json
import re as _re
import sys as _sys
import threading as _threading
import traceback as _traceback
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler as _BaseHTTPRequestHandler
from http.server import HTTPServer as _HTTPServer
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import parse_qs as _parse_qs
from urllib.parse import urlparse as _urlparse

import botocore.loaders
import botocore.session
from botocore.exceptions import ClientError
from botocore.model import ServiceModel
from botocore.validate import ParamValidator


API_PREFIX = "/2023-10-12"


def route(method: str, path: str, operation: str):
    """Tag a backend method with an HTTP route and botocore operation name.

    The mock HTTP server discovers these annotations to build its routing
    table. The `operation` name is used for response-shape validation against
    the botocore Deadline service model.
    """

    def decorator(fn):
        fn.__http_route__ = (method, f"{API_PREFIX}{path}", operation)
        return fn

    return decorator


def _resource_not_found(resource_type: str, resource_id: str, operation: str) -> ClientError:
    """Create a ResourceNotFoundException like the real Deadline Cloud API."""
    return ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": f"Resource of type {resource_type} with id {resource_id} does not exist.",
            }
        },
        operation,
    )


def _validation_exception(message: str, operation: str) -> ClientError:
    """Create a ValidationException like the real Deadline Cloud API."""
    return ClientError(
        {
            "Error": {
                "Code": "ValidationException",
                "Message": message,
            }
        },
        operation,
    )


class MockDeadlineBackend:
    """In-memory Deadline Cloud backend for testing."""

    def __init__(self, validate_params: bool = True):
        self.farms: dict[str, dict] = {}
        self.queues: dict[tuple, dict] = {}
        self.fleets: dict[tuple, dict] = {}  # (farmId, fleetId)
        self.workers: dict[tuple, dict] = {}  # (farmId, fleetId, workerId)
        self.jobs: dict[tuple, dict] = {}
        self.steps: dict[tuple, dict] = {}
        self.tasks: dict[tuple, dict] = {}
        self.sessions: dict[tuple, dict] = {}
        self.session_actions: dict[tuple, dict] = {}
        self.storage_profiles: dict[tuple, dict] = {}
        self.call_counts: dict[str, int] = {}
        self.batch_call_sizes: dict[str, list[int]] = {}
        self._job_environments: dict[str, list[str]] = {}  # job_id -> [env_name, ...]
        self._step_environments: dict[tuple, list[str]] = {}  # (job_id, step_id) -> [env_name, ...]
        self._id_counter = 0
        self._validate_params = validate_params
        self._validator = None
        self._service_model = None

    def clear(self) -> None:
        """Reset all in-memory state. Lets a single backend + HTTP server be
        reused across tests by clearing per-test data between runs."""
        self.farms.clear()
        self.queues.clear()
        self.fleets.clear()
        self.workers.clear()
        self.jobs.clear()
        self.steps.clear()
        self.tasks.clear()
        self.sessions.clear()
        self.session_actions.clear()
        self.call_counts.clear()
        self.batch_call_sizes.clear()
        self._job_environments.clear()
        self._step_environments.clear()
        self._id_counter = 0
        # Ad-hoc attrs some tests/routes stash on the backend.
        for attr in (
            "queue_fleet_associations",
            "storage_profiles",
            "queue_read_credentials",
            "_injected_failures",
            "create_job_delay",
        ):
            if hasattr(self, attr):
                delattr(self, attr)

    def _get_validator(self) -> tuple[ParamValidator, ServiceModel]:
        """Lazy-load the botocore service model and validator."""
        if self._validator is None:
            session = botocore.session.get_session()
            loader = session.get_component("data_loader")
            api_data = loader.load_service_model("deadline", "service-2")
            self._service_model = ServiceModel(api_data)
            self._validator = ParamValidator()
        return self._validator, self._service_model

    def _validate(self, operation_name: str, params: dict) -> None:
        """Validate params against the Deadline API schema."""
        if not self._validate_params:
            return
        validator, service_model = self._get_validator()
        op_model = service_model.operation_model(operation_name)
        input_shape = op_model.input_shape
        report = validator.validate(params, input_shape)
        if report.has_errors():
            raise ValueError(f"{operation_name}: {report.generate_report()}")

    def _gen_id(self, prefix: str) -> str:
        self._id_counter += 1
        return f"{prefix}-{self._id_counter:032x}"

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _parse_template(self, template: str) -> Any:
        """Parse a job template string using openjd-model and return instantiated Job."""
        import yaml
        from openjd.model import create_job, decode_job_template
        from openjd.model import DecodeValidationError, UnsupportedSchema

        try:
            template_dict = yaml.safe_load(template)
        except yaml.YAMLError as e:
            raise _validation_exception(f"Could not decode job template. Error: '{e}'", "CreateJob")

        try:
            job_template = decode_job_template(
                template=template_dict, supported_extensions=["TASK_CHUNKING"]
            )
            return create_job(job_template=job_template, job_parameter_values={})
        except (DecodeValidationError, UnsupportedSchema, TypeError) as e:
            raise _validation_exception(str(e), "CreateJob")

    # Map openjd-model type values to Deadline API parameter type keys
    _PARAM_TYPE_MAP = {
        "CHUNK[INT]": "chunkInt",
        "INT": "int",
        "STRING": "string",
        "FLOAT": "float",
        "PATH": "path",
    }

    def _normalize_parameter_space(self, space: Any) -> Any:
        """Normalize parameter space for task creation.

        For chunked parameters, ensures NONCONTIGUOUS constraint so we get
        individual values like "1" instead of ranges like "1-1".
        """
        from openjd.model.v2023_09._model import TaskChunksRangeConstraint

        if space is None:
            return None

        new_defs = {}
        for name, param_def in space.taskParameterDefinitions.items():
            if hasattr(param_def, "chunks") and param_def.chunks:
                new_chunks = param_def.chunks.model_copy(
                    update={"rangeConstraint": TaskChunksRangeConstraint.NONCONTIGUOUS}
                )
                new_defs[name] = param_def.model_copy(update={"chunks": new_chunks})
            else:
                new_defs[name] = param_def

        return space.model_copy(update={"taskParameterDefinitions": new_defs})

    def _create_steps_from_template(
        self, farm_id: str, queue_id: str, job_id: str, job: Any, now: datetime
    ) -> None:
        """Create steps and tasks from an instantiated Job."""
        from openjd.model import StepParameterSpaceIterator

        # Store job environments
        self._job_environments[job_id] = [env.name for env in (job.jobEnvironments or [])]

        for step in job.steps:
            step_id = self._gen_id("step")
            self.steps[(farm_id, queue_id, job_id, step_id)] = {
                "farmId": farm_id,
                "queueId": queue_id,
                "jobId": job_id,
                "stepId": step_id,
                "name": step.name,
                "lifecycleStatus": "CREATE_COMPLETE",
                "taskRunStatus": "PENDING",
                "taskRunStatusCounts": {},
                "createdAt": now,
                "createdBy": "mock-user",
            }

            # Store step environments
            self._step_environments[(job_id, step_id)] = [
                env.name for env in (step.stepEnvironments or [])
            ]

            # Normalize parameter space and iterate with 1 value per task
            space = self._normalize_parameter_space(step.parameterSpace)
            iterator = StepParameterSpaceIterator(space=space, chunks_task_count_override=1)
            for task_idx, task_params in enumerate(iterator):
                task_id = f"task-{step_id.split('-')[1]}-{task_idx}"
                # Convert ParameterValue objects to API format
                params = {
                    name: {self._PARAM_TYPE_MAP.get(pv.type.value, pv.type.value.lower()): pv.value}
                    for name, pv in task_params.items()
                }
                self.tasks[(farm_id, queue_id, job_id, step_id, task_id)] = {
                    "farmId": farm_id,
                    "queueId": queue_id,
                    "jobId": job_id,
                    "stepId": step_id,
                    "taskId": task_id,
                    "runStatus": "PENDING",
                    "parameters": params,
                    "createdAt": now,
                    "createdBy": "mock-user",
                }

    # ========== Farm APIs ==========

    @route("POST", "/farms", "CreateFarm")
    def create_farm(self, *, displayName: str, **kwargs) -> dict:
        params = {"displayName": displayName, **kwargs}
        self._validate("CreateFarm", params)
        farm_id = self._gen_id("farm")
        self.farms[farm_id] = {
            "farmId": farm_id,
            "displayName": displayName,
            "createdAt": self._now(),
            "createdBy": "mock-user",
            **kwargs,
        }
        return {"farmId": farm_id}

    @route("GET", "/farms/{farmId}", "GetFarm")
    def get_farm(self, *, farmId: str) -> dict:
        self._validate("GetFarm", {"farmId": farmId})
        if farmId not in self.farms:
            raise _resource_not_found("farm", farmId, "GetFarm")
        return self.farms[farmId]

    @route("GET", "/farms", "ListFarms")
    def list_farms(self, *, maxResults: int = 100, nextToken: str | None = None, **kwargs) -> dict:
        params: dict = {"maxResults": maxResults}
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListFarms", params)
        return {"farms": list(self.farms.values())}

    # ========== Queue APIs ==========

    @route("POST", "/farms/{farmId}/queues", "CreateQueue")
    def create_queue(self, *, farmId: str, displayName: str, **kwargs) -> dict:
        params = {"farmId": farmId, "displayName": displayName, **kwargs}
        self._validate("CreateQueue", params)
        queue_id = self._gen_id("queue")
        self.queues[(farmId, queue_id)] = {
            "queueId": queue_id,
            "farmId": farmId,
            "displayName": displayName,
            "status": kwargs.pop("status", "ACTIVE"),
            "defaultBudgetAction": kwargs.pop("defaultBudgetAction", "NONE"),
            "createdAt": self._now(),
            "createdBy": "mock-user",
            **kwargs,
        }
        return {"queueId": queue_id}

    @route("GET", "/farms/{farmId}/queues/{queueId}", "GetQueue")
    def get_queue(self, *, farmId: str, queueId: str) -> dict:
        self._validate("GetQueue", {"farmId": farmId, "queueId": queueId})
        key = (farmId, queueId)
        if key not in self.queues:
            raise _resource_not_found("queue", queueId, "GetQueue")
        return self.queues[key]

    @route("GET", "/farms/{farmId}/queues", "ListQueues")
    def list_queues(
        self, *, farmId: str, maxResults: int = 100, nextToken: str | None = None, **kwargs
    ) -> dict:
        params: dict = {"farmId": farmId, "maxResults": maxResults}
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListQueues", params)
        return {"queues": [q for (f, _), q in self.queues.items() if f == farmId]}

    @route("GET", "/farms/{farmId}/queue-fleet-associations", "ListQueueFleetAssociations")
    def list_queue_fleet_associations(
        self,
        *,
        farmId: str,
        queueId: str | None = None,
        fleetId: str | None = None,
        nextToken: str | None = None,
        **kwargs,
    ) -> dict:
        params: dict = {"farmId": farmId}
        if queueId is not None:
            params["queueId"] = queueId
        if fleetId is not None:
            params["fleetId"] = fleetId
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListQueueFleetAssociations", params)
        qfas = getattr(self, "queue_fleet_associations", {})
        items = [
            qfa
            for (f, q, fl), qfa in qfas.items()
            if f == farmId
            and (queueId is None or q == queueId)
            and (fleetId is None or fl == fleetId)
        ]
        return {"queueFleetAssociations": items}

    @route(
        "GET", "/farms/{farmId}/queues/{queueId}/storage-profiles", "ListStorageProfilesForQueue"
    )
    def list_storage_profiles_for_queue(
        self, *, farmId: str, queueId: str, nextToken: str | None = None, **kwargs
    ) -> dict:
        params: dict = {"farmId": farmId, "queueId": queueId}
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListStorageProfilesForQueue", params)
        sps = getattr(self, "storage_profiles", {})
        return {
            "storageProfiles": [sp for (f, q, _), sp in sps.items() if f == farmId and q == queueId]
        }

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/storage-profiles/{storageProfileId}",
        "GetStorageProfileForQueue",
    )
    def get_storage_profile_for_queue(
        self, *, farmId: str, queueId: str, storageProfileId: str
    ) -> dict:
        self._validate(
            "GetStorageProfileForQueue",
            {"farmId": farmId, "queueId": queueId, "storageProfileId": storageProfileId},
        )
        sps = getattr(self, "storage_profiles", {})
        key = (farmId, queueId, storageProfileId)
        if key not in sps:
            raise _resource_not_found(
                "storageProfile", storageProfileId, "GetStorageProfileForQueue"
            )
        return sps[key]

    def create_storage_profile(
        self, *, farmId: str, queueId: str, displayName: str, osFamily: str = "LINUX", **kwargs
    ) -> dict:
        sp_id = self._gen_id("sp")
        self.storage_profiles[(farmId, queueId, sp_id)] = {
            "storageProfileId": sp_id,
            "displayName": displayName,
            "osFamily": osFamily,
            **kwargs,
        }
        return {"storageProfileId": sp_id}

    @route("GET", "/farms/{farmId}/queues/{queueId}/read-roles", "AssumeQueueRoleForRead")
    def assume_queue_role_for_read(self, *, farmId: str, queueId: str) -> dict:
        self._validate("AssumeQueueRoleForRead", {"farmId": farmId, "queueId": queueId})
        if (farmId, queueId) not in self.queues:
            raise _resource_not_found("queue", queueId, "AssumeQueueRoleForRead")
        creds = getattr(
            self,
            "queue_read_credentials",
            {
                "accessKeyId": "testing",
                "secretAccessKey": "testing",
                "sessionToken": "testing",
                "expiration": self._now() + timedelta(hours=1),
            },
        )
        return {"credentials": creds}

    @route("GET", "/farms/{farmId}/queues/{queueId}/environments", "ListQueueEnvironments")
    def list_queue_environments(
        self, *, farmId: str, queueId: str, nextToken: str | None = None, **kwargs
    ) -> dict:
        params: dict = {"farmId": farmId, "queueId": queueId}
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListQueueEnvironments", params)
        if (farmId, queueId) not in self.queues:
            raise _resource_not_found("queue", queueId, "ListQueueEnvironments")
        return {"environments": []}

    @route("GET", "/farms/{farmId}/queues/{queueId}/user-roles", "AssumeQueueRoleForUser")
    def assume_queue_role_for_user(self, *, farmId: str, queueId: str) -> dict:
        self._validate("AssumeQueueRoleForUser", {"farmId": farmId, "queueId": queueId})
        if (farmId, queueId) not in self.queues:
            raise _resource_not_found("queue", queueId, "AssumeQueueRoleForUser")
        # Mirror whatever static creds tests configured; default to placeholders.
        creds = getattr(
            self,
            "queue_user_credentials",
            {
                "accessKeyId": "testing",
                "secretAccessKey": "testing",
                "sessionToken": "testing",
                "expiration": self._now() + timedelta(hours=1),
            },
        )
        return {"credentials": creds}

    # ========== Fleet APIs ==========

    _DEFAULT_FLEET_CONFIG = {
        "customerManaged": {
            "mode": "NO_SCALING",
            "workerCapabilities": {
                "vCpuCount": {"min": 1},
                "memoryMiB": {"min": 1024},
                "osFamily": "LINUX",
                "cpuArchitectureType": "x86_64",
            },
        }
    }

    @route("POST", "/farms/{farmId}/fleets", "CreateFleet")
    def create_fleet(self, *, farmId: str, displayName: str, **kwargs) -> dict:
        params = {"farmId": farmId, "displayName": displayName, **kwargs}
        self._validate("CreateFleet", params)
        if farmId not in self.farms:
            raise _resource_not_found("farm", farmId, "CreateFleet")
        fleet_id = self._gen_id("fleet")
        now = self._now()
        self.fleets[(farmId, fleet_id)] = {
            "fleetId": fleet_id,
            "farmId": farmId,
            "displayName": displayName,
            "description": kwargs.get("description", ""),
            "status": "ACTIVE",
            "workerCount": 0,
            "minWorkerCount": kwargs.get("minWorkerCount", 0),
            "maxWorkerCount": kwargs.get("maxWorkerCount", 1),
            "configuration": kwargs.get("configuration", self._DEFAULT_FLEET_CONFIG),
            "roleArn": kwargs.get("roleArn", "arn:aws:iam::000000000000:role/mock"),
            "createdAt": now,
            "createdBy": "mock-user",
        }
        return {"fleetId": fleet_id}

    @route("GET", "/farms/{farmId}/fleets/{fleetId}", "GetFleet")
    def get_fleet(self, *, farmId: str, fleetId: str) -> dict:
        self._validate("GetFleet", {"farmId": farmId, "fleetId": fleetId})
        key = (farmId, fleetId)
        if key not in self.fleets:
            raise _resource_not_found("fleet", fleetId, "GetFleet")
        return dict(self.fleets[key])

    @route("GET", "/farms/{farmId}/fleets", "ListFleets")
    def list_fleets(
        self, *, farmId: str, maxResults: int = 100, nextToken: str | None = None, **kwargs
    ) -> dict:
        params = {"farmId": farmId, "maxResults": maxResults}
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListFleets", params)
        fleets = [dict(f) for k, f in self.fleets.items() if k[0] == farmId]
        return {"fleets": fleets}

    # ========== Worker APIs ==========

    @route("POST", "/farms/{farmId}/fleets/{fleetId}/workers", "CreateWorker")
    def create_worker(self, *, farmId: str, fleetId: str, **kwargs) -> dict:
        params = {"farmId": farmId, "fleetId": fleetId, **kwargs}
        self._validate("CreateWorker", params)
        if (farmId, fleetId) not in self.fleets:
            raise _resource_not_found("fleet", fleetId, "CreateWorker")
        worker_id = self._gen_id("worker")
        now = self._now()
        self.workers[(farmId, fleetId, worker_id)] = {
            "farmId": farmId,
            "fleetId": fleetId,
            "workerId": worker_id,
            "status": kwargs.get("status", "CREATED"),
            "createdAt": now,
            "createdBy": "mock-user",
        }
        return {"workerId": worker_id}

    @route("GET", "/farms/{farmId}/fleets/{fleetId}/workers/{workerId}", "GetWorker")
    def get_worker(self, *, farmId: str, fleetId: str, workerId: str) -> dict:
        self._validate("GetWorker", {"farmId": farmId, "fleetId": fleetId, "workerId": workerId})
        key = (farmId, fleetId, workerId)
        if key not in self.workers:
            raise _resource_not_found("worker", workerId, "GetWorker")
        return dict(self.workers[key])

    @route("POST", "/farms/{farmId}/search/workers", "SearchWorkers")
    def search_workers(
        self,
        *,
        farmId: str,
        fleetIds: list,
        itemOffset: int = 0,
        pageSize: int = 100,
        **kwargs,
    ) -> dict:
        params = {
            "farmId": farmId,
            "fleetIds": fleetIds,
            "itemOffset": itemOffset,
            "pageSize": pageSize,
            **kwargs,
        }
        self._validate("SearchWorkers", params)
        workers = [dict(w) for k, w in self.workers.items() if k[0] == farmId and k[1] in fleetIds]
        total = len(workers)
        page = workers[itemOffset : itemOffset + pageSize]
        result: dict = {"workers": page, "totalResults": total}
        if itemOffset + pageSize < total:
            result["nextItemOffset"] = itemOffset + pageSize
        return result

    # ========== Job APIs ==========

    @route("POST", "/farms/{farmId}/queues/{queueId}/jobs", "CreateJob")
    def create_job(
        self, *, farmId: str, queueId: str, template: str, priority: int = 50, **kwargs
    ) -> dict:
        params = {
            "farmId": farmId,
            "queueId": queueId,
            "template": template,
            "priority": priority,
            **kwargs,
        }
        self._validate("CreateJob", params)

        # Optional delay to make it possible for tests to reliably interact
        # with the progress dialog (e.g. click Cancel) before CreateJob
        # returns. Opt-in via ``backend.create_job_delay = <seconds>``.
        delay = getattr(self, "create_job_delay", 0)
        if delay:
            import time as _time

            _time.sleep(delay)

        # Parse the job template using openjd-model
        job_template = self._parse_template(template)

        job_id = self._gen_id("job")
        now = self._now()

        # Use name from template if not overridden
        job_name = kwargs.get("nameOverride") or getattr(job_template, "name", "Untitled")

        self.jobs[(farmId, queueId, job_id)] = {
            "jobId": job_id,
            "name": job_name,
            "lifecycleStatus": "CREATE_COMPLETE",
            "lifecycleStatusMessage": "",
            "taskRunStatus": "PENDING",
            "priority": priority,
            "createdAt": now,
            "createdBy": "mock-user",
            "startedAt": now,
            **({"attachments": kwargs["attachments"]} if "attachments" in kwargs else {}),
        }

        # Auto-create steps and tasks from template
        self._create_steps_from_template(farmId, queueId, job_id, job_template, now)

        return {"jobId": job_id}

    @route("GET", "/farms/{farmId}/queues/{queueId}/jobs/{jobId}", "GetJob")
    def get_job(self, *, farmId: str, queueId: str, jobId: str) -> dict:
        self._validate("GetJob", {"farmId": farmId, "queueId": queueId, "jobId": jobId})
        key = (farmId, queueId, jobId)
        if key not in self.jobs:
            raise _resource_not_found("job", jobId, "GetJob")
        return self.jobs[key]

    @route("PATCH", "/farms/{farmId}/queues/{queueId}/jobs/{jobId}", "UpdateJob")
    def update_job(self, *, farmId: str, queueId: str, jobId: str, **kwargs) -> dict:
        params = {"farmId": farmId, "queueId": queueId, "jobId": jobId, **kwargs}
        self._validate("UpdateJob", params)
        key = (farmId, queueId, jobId)
        if key not in self.jobs:
            raise _resource_not_found("job", jobId, "UpdateJob")
        target = kwargs.get("targetTaskRunStatus")
        if target:
            self.jobs[key]["taskRunStatus"] = target
            # Mirror target onto tasks so wait/cancel/requeue flows observe the
            # transition without extra simulation.
            for k in list(self.tasks):
                if k[:3] == key:
                    self.tasks[k] = {**self.tasks[k], "runStatus": target}
        return {}

    @route(
        "PATCH",
        "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/steps/{stepId}/tasks/{taskId}",
        "UpdateTask",
    )
    def update_task(
        self, *, farmId: str, queueId: str, jobId: str, stepId: str, taskId: str, **kwargs
    ) -> dict:
        params = {
            "farmId": farmId,
            "queueId": queueId,
            "jobId": jobId,
            "stepId": stepId,
            "taskId": taskId,
            **kwargs,
        }
        self._validate("UpdateTask", params)
        key = (farmId, queueId, jobId, stepId, taskId)
        if key not in self.tasks:
            raise _resource_not_found("task", taskId, "UpdateTask")
        target = kwargs.get("targetRunStatus")
        if target:
            self.tasks[key] = {**self.tasks[key], "runStatus": target}
        return {}

    @route("GET", "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/steps", "ListSteps")
    def list_steps(
        self, *, farmId: str, queueId: str, jobId: str, nextToken: str | None = None, **kwargs
    ) -> dict:
        params: dict = {"farmId": farmId, "queueId": queueId, "jobId": jobId}
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListSteps", params)
        prefix = (farmId, queueId, jobId)
        return {"steps": [s for k, s in self.steps.items() if k[:3] == prefix]}

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/steps/{stepId}/tasks",
        "ListTasks",
    )
    def list_tasks(
        self,
        *,
        farmId: str,
        queueId: str,
        jobId: str,
        stepId: str,
        nextToken: str | None = None,
        **kwargs,
    ) -> dict:
        params: dict = {
            "farmId": farmId,
            "queueId": queueId,
            "jobId": jobId,
            "stepId": stepId,
        }
        if nextToken is not None:
            params["nextToken"] = nextToken
        params.update(kwargs)
        self._validate("ListTasks", params)
        prefix = (farmId, queueId, jobId, stepId)
        return {"tasks": [t for k, t in self.tasks.items() if k[:4] == prefix]}

    @route("POST", "/farms/{farmId}/search/jobs", "SearchJobs")
    def search_jobs(
        self,
        *,
        farmId: str,
        queueIds: list[str],
        itemOffset: int = 0,
        pageSize: int = 100,
        filterExpressions: dict | None = None,
        sortExpressions: list | None = None,
    ) -> dict:
        """Basic search_jobs implementation. For advanced filtering, use mock_search_jobs_for_set."""
        params = {
            "farmId": farmId,
            "queueIds": queueIds,
            "itemOffset": itemOffset,
            "pageSize": pageSize,
        }
        if filterExpressions:
            params["filterExpressions"] = filterExpressions
        if sortExpressions:
            params["sortExpressions"] = sortExpressions
        self._validate("SearchJobs", params)

        # Real Deadline rejects queueIds that don't exist in the farm.
        for qid in queueIds:
            if (farmId, qid) not in self.queues:
                raise _resource_not_found("queue", qid, "SearchJobs")

        jobs = [j for key, j in self.jobs.items() if key[0] == farmId and key[1] in queueIds]
        # Basic sort by createdAt
        jobs = sorted(jobs, key=lambda j: j.get("createdAt", self._now()))

        total = len(jobs)
        result_jobs = jobs[itemOffset : itemOffset + pageSize]

        response = {"jobs": result_jobs, "totalResults": total}
        if itemOffset + pageSize < total:
            response["nextItemOffset"] = itemOffset + pageSize
        return response

    # ========== Step APIs ==========

    @route("GET", "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/steps/{stepId}", "GetStep")
    def get_step(self, *, farmId: str, queueId: str, jobId: str, stepId: str) -> dict:
        self._validate(
            "GetStep", {"farmId": farmId, "queueId": queueId, "jobId": jobId, "stepId": stepId}
        )
        key = (farmId, queueId, jobId, stepId)
        if key not in self.steps:
            raise _resource_not_found("step", stepId, "GetStep")
        return self.steps[key]

    # ========== Task APIs ==========

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/steps/{stepId}/tasks/{taskId}",
        "GetTask",
    )
    def get_task(self, *, farmId: str, queueId: str, jobId: str, stepId: str, taskId: str) -> dict:
        self._validate(
            "GetTask",
            {
                "farmId": farmId,
                "queueId": queueId,
                "jobId": jobId,
                "stepId": stepId,
                "taskId": taskId,
            },
        )
        key = (farmId, queueId, jobId, stepId, taskId)
        if key not in self.tasks:
            raise _resource_not_found("task", taskId, "GetTask")
        return self.tasks[key]

    # ========== Session APIs ==========

    @route("GET", "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/sessions", "ListSessions")
    def list_sessions(
        self, *, farmId: str, queueId: str, jobId: str, nextToken: str | None = None
    ) -> dict:
        params = {"farmId": farmId, "queueId": queueId, "jobId": jobId}
        if nextToken:
            params["nextToken"] = nextToken
        self._validate("ListSessions", params)
        prefix = (farmId, queueId, jobId)
        sessions = [s for key, s in self.sessions.items() if key[:3] == prefix]
        return {"sessions": sessions}

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/sessions/{sessionId}",
        "GetSession",
    )
    def get_session(self, *, farmId: str, queueId: str, jobId: str, sessionId: str) -> dict:
        self._validate(
            "GetSession",
            {"farmId": farmId, "queueId": queueId, "jobId": jobId, "sessionId": sessionId},
        )
        key = (farmId, queueId, jobId, sessionId)
        if key not in self.sessions:
            raise _resource_not_found("session", sessionId, "GetSession")
        return self.sessions[key]

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/session-actions/{sessionActionId}",
        "GetSessionAction",
    )
    def get_session_action(
        self, *, farmId: str, queueId: str, jobId: str, sessionActionId: str
    ) -> dict:
        self._validate(
            "GetSessionAction",
            {
                "farmId": farmId,
                "queueId": queueId,
                "jobId": jobId,
                "sessionActionId": sessionActionId,
            },
        )
        # session id is embedded as the prefix: sessionaction-<sessionuuid>-N
        for key, action in self.session_actions.items():
            if key[:3] == (farmId, queueId, jobId) and key[4] == sessionActionId:
                return action
        raise _resource_not_found("sessionAction", sessionActionId, "GetSessionAction")

    @route(
        "GET",
        "/farms/{farmId}/queues/{queueId}/jobs/{jobId}/session-actions",
        "ListSessionActions",
    )
    def list_session_actions(
        self,
        *,
        farmId: str,
        queueId: str,
        jobId: str,
        sessionId: str | None = None,
        nextToken: str | None = None,
    ) -> dict:
        params: dict = {"farmId": farmId, "queueId": queueId, "jobId": jobId}
        if sessionId:
            params["sessionId"] = sessionId
        if nextToken:
            params["nextToken"] = nextToken
        self._validate("ListSessionActions", params)
        prefix = (farmId, queueId, jobId)
        actions = [
            a
            for key, a in self.session_actions.items()
            if key[:3] == prefix and (sessionId is None or key[3] == sessionId)
        ]
        return {"sessionActions": actions}

    # ========== Simulation Helpers ==========

    def _get_env_ids(self, job_id: str, step_id: str) -> list[str]:
        """Get environment IDs for a step (job envs + step envs)."""
        job_envs = [f"jobenv:{name}" for name in self._job_environments.get(job_id, [])]
        step_envs = [
            f"stepenv:{name}" for name in self._step_environments.get((job_id, step_id), [])
        ]
        return job_envs + step_envs

    def simulate_task_runs(
        self,
        job_id: str,
        step_id: str,
        task_ids: list[str] | None = None,
        *,
        worker_id: str = "worker-1",
        duration_seconds: int = 30,
        env_duration_seconds: int = 2,
        overhead_seconds: int = 0,
    ) -> str:
        """Simulate running tasks on a single worker session. Returns session_id.

        If task_ids is None, simulates running all tasks in the step.
        overhead_seconds adds idle time at the start of the session before actions begin.
        """
        job_key = next(k for k in self.jobs if k[2] == job_id)
        farm_id, queue_id = job_key[0], job_key[1]

        # Default to all tasks in the step
        if task_ids is None:
            task_ids = [
                self.tasks[k]["taskId"]
                for k in self.tasks
                if k[:4] == (farm_id, queue_id, job_id, step_id)
            ]

        env_ids = self._get_env_ids(job_id, step_id)

        now = self._now()
        current_time = now + timedelta(seconds=overhead_seconds)
        session_id = self._gen_id("session")

        # Env enters (in order)
        for env_id in env_ids:
            action_id = self._gen_id("sessionaction")
            self.session_actions[(farm_id, queue_id, job_id, session_id, action_id)] = {
                "sessionActionId": action_id,
                "status": "SUCCEEDED",
                "startedAt": current_time,
                "endedAt": current_time + timedelta(seconds=env_duration_seconds),
                "definition": {"envEnter": {"environmentId": env_id}},
            }
            current_time += timedelta(seconds=env_duration_seconds)

        # Task runs
        for task_id in task_ids:
            action_id = self._gen_id("sessionaction")
            self.session_actions[(farm_id, queue_id, job_id, session_id, action_id)] = {
                "sessionActionId": action_id,
                "status": "SUCCEEDED",
                "startedAt": current_time,
                "endedAt": current_time + timedelta(seconds=duration_seconds),
                "definition": {"taskRun": {"stepId": step_id, "taskId": task_id}},
            }
            current_time += timedelta(seconds=duration_seconds)

        # Env exits (reverse order)
        for env_id in reversed(env_ids):
            action_id = self._gen_id("sessionaction")
            self.session_actions[(farm_id, queue_id, job_id, session_id, action_id)] = {
                "sessionActionId": action_id,
                "status": "SUCCEEDED",
                "startedAt": current_time,
                "endedAt": current_time + timedelta(seconds=env_duration_seconds),
                "definition": {"envExit": {"environmentId": env_id}},
            }
            current_time += timedelta(seconds=env_duration_seconds)

        self.sessions[(farm_id, queue_id, job_id, session_id)] = {
            "sessionId": session_id,
            "fleetId": "fleet-mock",
            "workerId": worker_id,
            "lifecycleStatus": "ENDED",
            "log": {
                "logDriver": "awslogs",
                "options": {
                    "logGroupName": f"/aws/deadline/{farm_id}/{queue_id}",
                    "logStreamName": session_id,
                },
            },
            "startedAt": now,
            "endedAt": current_time,
        }

        return session_id

    # ========== Batch Get APIs ==========

    def _batch_get(
        self,
        *,
        operation: str,
        identifiers: list[dict],
        id_fields: tuple[str, ...],
        storage: dict,
        storage_key_fields: tuple[str, ...],
        items_field: str,
        resource_type: str,
    ) -> dict:
        """Shared implementation for batch_get_* APIs with partial-success."""
        self._validate(operation, {"identifiers": identifiers})
        if len(identifiers) > 100:
            raise _validation_exception(
                f"identifiers: must have at most 100 items ({len(identifiers)} provided).",
                operation,
            )
        items: list[dict] = []
        errors: list[dict] = []
        for ident in identifiers:
            # Test-only failure injection.
            injected = self._consume_injected_failure(operation, ident)
            if injected is not None:
                errors.append({**{k: ident[k] for k in id_fields}, **injected})
                continue
            key = tuple(ident[k] for k in storage_key_fields)
            if key in storage:
                items.append(storage[key])
            else:
                errors.append(
                    {
                        **{k: ident[k] for k in id_fields},
                        "code": "ResourceNotFoundException",
                        "message": f"Resource of type {resource_type} with id "
                        f"{ident[id_fields[-1]]} does not exist.",
                    }
                )
        return {items_field: items, "errors": errors}

    @route("POST", "/batch-get-step", "BatchGetStep")
    def batch_get_step(self, *, identifiers: list[dict]) -> dict:
        return self._batch_get(
            operation="BatchGetStep",
            identifiers=identifiers,
            id_fields=("farmId", "queueId", "jobId", "stepId"),
            storage=self.steps,
            storage_key_fields=("farmId", "queueId", "jobId", "stepId"),
            items_field="steps",
            resource_type="step",
        )

    @route("POST", "/batch-get-task", "BatchGetTask")
    def batch_get_task(self, *, identifiers: list[dict]) -> dict:
        return self._batch_get(
            operation="BatchGetTask",
            identifiers=identifiers,
            id_fields=("farmId", "queueId", "jobId", "stepId", "taskId"),
            storage=self.tasks,
            storage_key_fields=("farmId", "queueId", "jobId", "stepId", "taskId"),
            items_field="tasks",
            resource_type="task",
        )

    # ========== Test-only failure injection ==========

    def inject_batch_failure(
        self,
        operation: str,
        identifier: dict,
        code: str,
        attempts: int = 1,
        message: str = "injected",
    ) -> None:
        """Cause the next ``attempts`` batch-get calls for ``identifier`` to
        return a per-item error with the given ``code``. Subsequent calls
        behave normally.
        """
        if not hasattr(self, "_injected_failures"):
            self._injected_failures: dict = {}
        key = (operation, tuple(sorted(identifier.items())))
        self._injected_failures[key] = {
            "remaining": attempts,
            "code": code,
            "message": message,
        }

    def _consume_injected_failure(self, operation: str, identifier: dict) -> dict | None:
        """Returns {'code': ..., 'message': ...} if a failure is queued for this
        identifier, else None. Decrements the remaining count."""
        failures = getattr(self, "_injected_failures", None)
        if not failures:
            return None
        key = (operation, tuple(sorted(identifier.items())))
        entry = failures.get(key)
        if entry is None or entry["remaining"] <= 0:
            return None
        entry["remaining"] -= 1
        return {"code": entry["code"], "message": entry["message"]}

    # ========== Mock Integration ==========

    def set_mock_methods(self, deadline_mock: MagicMock) -> None:
        """Wire this backend to a MagicMock for use with deadline_mock fixture."""
        deadline_mock.create_farm.side_effect = self.create_farm
        deadline_mock.get_farm.side_effect = self.get_farm
        deadline_mock.create_queue.side_effect = self.create_queue
        deadline_mock.get_queue.side_effect = self.get_queue
        deadline_mock.create_job.side_effect = self.create_job
        deadline_mock.get_job.side_effect = self.get_job
        deadline_mock.get_step.side_effect = self.get_step
        deadline_mock.get_task.side_effect = self.get_task
        deadline_mock.list_sessions.side_effect = self.list_sessions
        deadline_mock.list_session_actions.side_effect = self.list_session_actions
        deadline_mock.search_jobs.side_effect = self.search_jobs
        deadline_mock.batch_get_step.side_effect = self.batch_get_step
        deadline_mock.batch_get_task.side_effect = self.batch_get_task
        deadline_mock.create_fleet.side_effect = self.create_fleet
        deadline_mock.get_fleet.side_effect = self.get_fleet
        deadline_mock.list_fleets.side_effect = self.list_fleets
        deadline_mock.create_worker.side_effect = self.create_worker
        deadline_mock.get_worker.side_effect = self.get_worker
        deadline_mock.search_workers.side_effect = self.search_workers
        deadline_mock.list_farms.side_effect = self.list_farms
        deadline_mock.list_queues.side_effect = self.list_queues
        deadline_mock.list_storage_profiles_for_queue.side_effect = (
            self.list_storage_profiles_for_queue
        )


# ========== HTTP Server ==========
#
# Runs the backend over HTTP, speaking the Deadline Cloud rest-json protocol
# so the real `deadline` CLI can be pointed at it via AWS_ENDPOINT_URL_DEADLINE.
# Routes are discovered from `@route`-decorated methods on MockDeadlineBackend.
# Responses are filtered and validated against the botocore service model.

_INT_QUERY_PARAMS = {"maxResults", "itemOffset", "pageSize"}


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _discover_routes(backend):
    """Find @route-decorated methods on the backend and compile patterns."""
    routes = []
    for name in dir(backend):
        fn = getattr(backend, name)
        info = getattr(fn, "__http_route__", None)
        if info is None:
            continue
        method, path, operation = info
        pattern = _re.compile("^" + _re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", path) + "$")
        routes.append((method, pattern, fn, operation))
    return routes


class _ResponseValidator:
    def __init__(self):
        session = botocore.session.get_session()
        loader = session.get_component("data_loader")
        self._model = ServiceModel(loader.load_service_model("deadline", "service-2"))
        self._validator = ParamValidator()

    _TYPE_DEFAULTS = {
        "string": "",
        "integer": 0,
        "long": 0,
        "float": 0.0,
        "double": 0.0,
        "boolean": False,
        "timestamp": datetime(1970, 1, 1, tzinfo=timezone.utc),
        "list": [],
        "map": {},
        "structure": {},
    }

    def _filter(self, shape, value):
        """Recursively drop keys that aren't part of the shape, and fill
        required members the caller didn't supply with type-appropriate
        defaults.

        Botocore service models are periodically extended with new required
        response fields (e.g. `costScaleFactor`, `kmsKeyArn` on GetFarm).
        Tests that were written before those additions shouldn't have to
        thread new fields through the mock for every boto3 bump — we just
        synthesize safe defaults so response validation passes on whichever
        botocore happens to be installed.
        """
        if shape is None or value is None:
            return value
        t = shape.type_name
        if t == "structure":
            members = shape.members
            filtered = {k: self._filter(members[k], v) for k, v in value.items() if k in members}
            for req in getattr(shape, "required_members", []):
                if req not in filtered and req in members:
                    filtered[req] = self._TYPE_DEFAULTS.get(members[req].type_name, None)
            return filtered
        if t == "list":
            return [self._filter(shape.member, v) for v in value]
        if t == "map":
            return {k: self._filter(shape.value, v) for k, v in value.items()}
        return value

    def filter_and_validate(self, operation_name, response):
        output_shape = self._model.operation_model(operation_name).output_shape
        if output_shape is None:
            return response
        filtered = self._filter(output_shape, response)
        report = self._validator.validate(filtered, output_shape)
        if report.has_errors():
            raise ValueError(
                f"Mock response for {operation_name} failed validation: {report.generate_report()}"
            )
        return filtered


def _make_handler(routes, validator, backend):
    class _Handler(_BaseHTTPRequestHandler):
        def log_message(self, format, *args):  # silence stderr access logs
            return

        def _send_json(self, status, body, error_code=None):
            data = _json.dumps(body, default=_json_default).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            if error_code:
                self.send_header("x-amzn-errortype", error_code)
            self.end_headers()
            self.wfile.write(data)

        def _dispatch(self, method):
            parsed = _urlparse(self.path)
            for route_method, pattern, handler_fn, op_name in routes:
                if route_method != method:
                    continue
                m = pattern.match(parsed.path)
                if not m:
                    continue
                backend.call_counts[op_name] = backend.call_counts.get(op_name, 0) + 1
                kwargs = dict(m.groupdict())
                for k, v in _parse_qs(parsed.query).items():
                    kwargs[k] = int(v[0]) if k in _INT_QUERY_PARAMS else v[0]
                length = int(self.headers.get("Content-Length", 0))
                if length:
                    kwargs.update(_json.loads(self.rfile.read(length)))
                if "identifiers" in kwargs and isinstance(kwargs["identifiers"], list):
                    backend.batch_call_sizes.setdefault(op_name, []).append(
                        len(kwargs["identifiers"])
                    )
                try:
                    result = handler_fn(**kwargs)
                    result = validator.filter_and_validate(op_name, result)
                    self._send_json(200, result)
                except ClientError as exc:
                    err = exc.response["Error"]
                    code = err.get("Code", "InternalServerException")
                    status = 404 if code == "ResourceNotFoundException" else 400
                    self._send_json(status, {"message": err.get("Message", "")}, error_code=code)
                except Exception as exc:  # noqa: BLE001
                    _traceback.print_exc()
                    self._send_json(
                        500, {"message": str(exc)}, error_code="InternalServerException"
                    )
                return
            # Log 404s at stderr so a debugging CI run surfaces which path
            # the client hit without going through the mock's HTTP response
            # (which BrokenPipes if the client already disconnected).
            _sys.stderr.write(f"[mock-backend] 404 {method} {parsed.path}\n")
            _sys.stderr.flush()
            self._send_json(
                404,
                {"message": f"No route for {method} {parsed.path}"},
                error_code="NotFoundException",
            )

        def do_GET(self):  # noqa: N802
            self._dispatch("GET")

        def do_POST(self):  # noqa: N802
            self._dispatch("POST")

        def do_PATCH(self):  # noqa: N802
            self._dispatch("PATCH")

    return _Handler


def start_server(backend: "MockDeadlineBackend", port: int = 0):
    """Start the HTTP server in a daemon thread. Returns (server, base_url, thread).

    Binds to 127.0.0.1. Callers pointing the ``deadline`` CLI at this server via
    ``AWS_ENDPOINT_URL_DEADLINE`` must also disable botocore's ``management.``
    host-prefix injection for Deadline API calls (e.g. via the sitecustomize
    shim in ``test_cli_fleet_worker_subprocess.py``).
    """
    routes = _discover_routes(backend)
    validator = _ResponseValidator()
    handler_cls = _make_handler(routes, validator, backend)
    server = _HTTPServer(("127.0.0.1", port), handler_cls)
    actual_port = server.server_address[1]
    thread = _threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{actual_port}", thread
