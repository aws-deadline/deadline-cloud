# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.ui.dataclasses functions relating to serializing.
"""
import pytest

from deadline.client.ui.dataclasses import (
    HostRequirements,
    OsRequirements,
    HardwareRequirements,
    CustomRequirements,
    CustomAmountRequirement,
    CustomAttributeRequirement,
)


HOST_REQUIREMENTS_1 = {
    "attributes": [
        {"name": "attr.worker.os.family", "anyOf": ["windows"]},
        {"name": "attr.worker.cpu.arch", "anyOf": ["x86_64"]},
        {"name": "attr.worker.pipelineFeatures", "anyOf": ["feature1", "feature2"]},
    ],
    "amounts": [
        {"name": "amount.worker.vcpu", "min": 8, "max": 64},
        {"name": "amount.worker.memory", "min": 16384, "max": 131072},
        {"name": "amount.worker.Bugs", "min": 1, "max": 10},
    ],
}


def _compare_requirements(first, second):
    assert len(first.get("amounts")) == len(second.get("amounts"))
    assert len(first.get("attributes")) == len(second.get("attributes"))
    for amount in first.get("amounts"):
        name = amount.get("name")
        _min = amount.get("min")
        _max = amount.get("max")
        for ref_amount in second.get("amounts"):
            if ref_amount.get("name") == name:
                assert ref_amount.get("min") == _min
                assert ref_amount.get("max") == _max
                break
        else:
            raise ValueError("Could not find amount with name: {}".format(name))

    for attribute in first.get("attributes"):
        name = attribute.get("name")
        operation = "anyOf" if attribute.get("anyOf") else "allOf"
        values = attribute.get(operation)
        for ref_attribute in second.get("attributes"):
            if ref_attribute.get("name") == name:
                assert ("anyOf" if attribute.get("anyOf") else "allOf") == operation
                assert set(values) == set(ref_attribute.get(operation))
                break
        else:
            raise ValueError("Could not find attribute with name: {}".format(name))


def test_host_requirements_as_objects_serialize():
    requirements = HostRequirements(
        os_requirements=OsRequirements(
            operating_systems=[OsRequirements.WINDOWS],  # type: ignore
            cpu_archs=[OsRequirements.X86_64],  # type: ignore
        ),
        hardware_requirements=HardwareRequirements(
            cpu_min=8,
            cpu_max=64,
            memory_min=16 * 1024,
            memory_max=128 * 1024,
        ),
        custom_requirements=CustomRequirements(
            amounts=[
                CustomAmountRequirement(name="Bugs", min=1, max=10),
            ],
            attributes=[
                CustomAttributeRequirement(
                    name="pipelineFeatures",
                    option=CustomAttributeRequirement.ANY_OF,
                    values=["feature1", "feature2"],
                ),
            ],
        ),
    )
    open_jd_requirements = requirements.serialize()
    _compare_requirements(open_jd_requirements, HOST_REQUIREMENTS_1)


def test_host_requirements_as_dictionary_serialize():
    requirements = HostRequirements(
        os_requirements={  # type: ignore
            "operating_systems": ["windows"],
            "cpu_archs": ["x86_64"],
        },
        hardware_requirements={  # type: ignore
            "cpu_min": 8,
            "cpu_max": 64,
            "memory_min": 16 * 1024,
            "memory_max": 128 * 1024,
        },
        custom_requirements={  # type: ignore
            "amounts": [
                {"name": "Bugs", "min": 1, "max": 10},
            ],
            "attributes": [
                {"name": "pipelineFeatures", "option": "anyOf", "values": ["feature1", "feature2"]},
            ],
        },
    )
    open_jd_requirements = requirements.serialize()
    _compare_requirements(open_jd_requirements, HOST_REQUIREMENTS_1)


def test_host_requirements_invalid_os():
    with pytest.raises(ValueError):
        requirements = HostRequirements(
            os_requirements=OsRequirements(
                operating_systems=["freebsd"],  # type: ignore
                cpu_archs=[OsRequirements.X86_64],  # type: ignore
            ),
            hardware_requirements=HardwareRequirements(
                cpu_min=8,
                cpu_max=64,
                memory_min=16 * 1024,
                memory_max=128 * 1024,
            ),
            custom_requirements=CustomRequirements(
                amounts=[
                    CustomAmountRequirement(name="Bugs", min=1, max=10),
                ],
                attributes=[
                    CustomAttributeRequirement(
                        name="pipelineFeatures",
                        option=CustomAttributeRequirement.ANY_OF,
                        values=["feature1", "feature2"],
                    ),
                ],
            ),
        )
        requirements.serialize()


def test_host_requirements_invalid_arch():
    with pytest.raises(ValueError):
        requirements = HostRequirements(
            os_requirements=OsRequirements(
                operating_systems=[OsRequirements.LINUX],  # type: ignore
                cpu_archs=["risc-v"],  # type: ignore
            ),
            hardware_requirements=HardwareRequirements(
                cpu_min=8,
                cpu_max=64,
                memory_min=16 * 1024,
                memory_max=128 * 1024,
            ),
            custom_requirements=CustomRequirements(
                amounts=[
                    CustomAmountRequirement(name="Bugs", min=1, max=10),
                ],
                attributes=[
                    CustomAttributeRequirement(
                        name="pipelineFeatures",
                        option=CustomAttributeRequirement.ANY_OF,
                        values=["feature1", "feature2"],
                    ),
                ],
            ),
        )
        requirements.serialize()
