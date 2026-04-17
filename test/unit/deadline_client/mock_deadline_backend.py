# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
MockDeadlineBackend - In-memory Deadline Cloud simulator for testing.

See docs/design/deadline-tests-mock-backend.md for design details.
"""

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import botocore.loaders
import botocore.session
from botocore.exceptions import ClientError
from botocore.model import ServiceModel
from botocore.validate import ParamValidator


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
        self.jobs: dict[tuple, dict] = {}
        self.steps: dict[tuple, dict] = {}
        self.tasks: dict[tuple, dict] = {}
        self.sessions: dict[tuple, dict] = {}
        self.session_actions: dict[tuple, dict] = {}
        self._job_environments: dict[str, list[str]] = {}  # job_id -> [env_name, ...]
        self._step_environments: dict[tuple, list[str]] = {}  # (job_id, step_id) -> [env_name, ...]
        self._id_counter = 0
        self._validate_params = validate_params
        self._validator = None
        self._service_model = None

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
                    "taskId": task_id,
                    "runStatus": "PENDING",
                    "parameters": params,
                    "createdAt": now,
                    "createdBy": "mock-user",
                }

    # ========== Farm APIs ==========

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

    def get_farm(self, *, farmId: str) -> dict:
        self._validate("GetFarm", {"farmId": farmId})
        if farmId not in self.farms:
            raise _resource_not_found("farm", farmId, "GetFarm")
        return self.farms[farmId]

    # ========== Queue APIs ==========

    def create_queue(self, *, farmId: str, displayName: str, **kwargs) -> dict:
        params = {"farmId": farmId, "displayName": displayName, **kwargs}
        self._validate("CreateQueue", params)
        queue_id = self._gen_id("queue")
        self.queues[(farmId, queue_id)] = {
            "queueId": queue_id,
            "farmId": farmId,
            "displayName": displayName,
            "createdAt": self._now(),
            "createdBy": "mock-user",
            **kwargs,
        }
        return {"queueId": queue_id}

    def get_queue(self, *, farmId: str, queueId: str) -> dict:
        self._validate("GetQueue", {"farmId": farmId, "queueId": queueId})
        key = (farmId, queueId)
        if key not in self.queues:
            raise _resource_not_found("queue", queueId, "GetQueue")
        return self.queues[key]

    # ========== Job APIs ==========

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
            "taskRunStatus": "PENDING",
            "priority": priority,
            "createdAt": now,
            "createdBy": "mock-user",
            "startedAt": now,
        }

        # Auto-create steps and tasks from template
        self._create_steps_from_template(farmId, queueId, job_id, job_template, now)

        return {"jobId": job_id}

    def get_job(self, *, farmId: str, queueId: str, jobId: str) -> dict:
        self._validate("GetJob", {"farmId": farmId, "queueId": queueId, "jobId": jobId})
        key = (farmId, queueId, jobId)
        if key not in self.jobs:
            raise _resource_not_found("job", jobId, "GetJob")
        return self.jobs[key]

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

    def get_step(self, *, farmId: str, queueId: str, jobId: str, stepId: str) -> dict:
        self._validate(
            "GetStep", {"farmId": farmId, "queueId": queueId, "jobId": jobId, "stepId": stepId}
        )
        key = (farmId, queueId, jobId, stepId)
        if key not in self.steps:
            raise _resource_not_found("step", stepId, "GetStep")
        return self.steps[key]

    # ========== Task APIs ==========

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

    def list_session_actions(
        self,
        *,
        farmId: str,
        queueId: str,
        jobId: str,
        sessionId: str,
        nextToken: str | None = None,
    ) -> dict:
        params = {"farmId": farmId, "queueId": queueId, "jobId": jobId, "sessionId": sessionId}
        if nextToken:
            params["nextToken"] = nextToken
        self._validate("ListSessionActions", params)
        prefix = (farmId, queueId, jobId, sessionId)
        actions = [a for key, a in self.session_actions.items() if key[:4] == prefix]
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
