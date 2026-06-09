# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for host-requirements prefill logic in the job bundle submitter."""

from deadline.client.ui.job_bundle_submitter import _resolve_template_host_requirements


_HR_A = {
    "amounts": [{"name": "amount.worker.vcpu", "min": 8, "max": 64}],
    "attributes": [{"name": "attr.worker.os.family", "anyOf": ["linux"]}],
}
_HR_B = {
    "amounts": [{"name": "amount.worker.vcpu", "min": 1}],
}


class TestResolveTemplateHostRequirements:
    def test_no_steps_returns_none(self):
        """A template with no steps should resolve to no prefill."""
        assert _resolve_template_host_requirements({}) is None
        assert _resolve_template_host_requirements({"steps": []}) is None

    def test_single_step_with_requirements_prefills(self):
        """A single step's hostRequirements should be returned for prefill."""
        template = {"steps": [{"name": "Step1", "hostRequirements": _HR_A}]}
        assert _resolve_template_host_requirements(template) == _HR_A

    def test_single_step_without_requirements_returns_none(self):
        """A single step lacking hostRequirements should resolve to no prefill."""
        template = {"steps": [{"name": "Step1"}]}
        assert _resolve_template_host_requirements(template) is None

    def test_multiple_steps_identical_requirements_prefills(self):
        """Multiple steps with identical hostRequirements should prefill them."""
        template = {
            "steps": [
                {"name": "Step1", "hostRequirements": _HR_A},
                {"name": "Step2", "hostRequirements": dict(_HR_A)},
            ]
        }
        assert _resolve_template_host_requirements(template) == _HR_A

    def test_multiple_steps_different_requirements_returns_none(self):
        """Multiple steps with differing hostRequirements should not prefill (deactivated)."""
        template = {
            "steps": [
                {"name": "Step1", "hostRequirements": _HR_A},
                {"name": "Step2", "hostRequirements": _HR_B},
            ]
        }
        assert _resolve_template_host_requirements(template) is None

    def test_some_steps_missing_requirements_returns_none(self):
        """If only some steps declare hostRequirements, treat as differing -> no prefill."""
        template = {
            "steps": [
                {"name": "Step1", "hostRequirements": _HR_A},
                {"name": "Step2"},
            ]
        }
        assert _resolve_template_host_requirements(template) is None
