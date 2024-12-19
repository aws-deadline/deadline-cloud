# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Contains dataclasses for holding UI parameter values, used by the widgets.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal, Dict, List, Union

from ...job_bundle.parameters import JobParameter

MAX_INT_VALUE = (2**31) - 1


@dataclass
class JobBundleSettings:  # pylint: disable=too-many-instance-attributes
    """
    Settings for the Job Bundle submitter dialog.
    """

    # Used in UI elements and when creating the job bundle directory
    submitter_name: str = field(default="JobBundle")

    # Shared settings
    name: str = field(default="Job bundle")
    description: str = field(default="")

    # Job Bundle settings
    input_job_bundle_dir: str = field(default="")
    parameters: list[JobParameter] = field(default_factory=list)

    # Whether to allow ability to "Load a different job bundle"
    browse_enabled: bool = field(default=False)


@dataclass
class CliJobSettings:  # pylint: disable=too-many-instance-attributes
    """
    Settings for a CLI Job.
    """

    # Used in UI elements and when creating the job bundle directory
    submitter_name: str = field(default="CLI")

    # Shared settings
    name: str = field(default="CLI job")
    description: str = field(default="")

    # CLI job settings
    bash_script_contents: str = field(
        default="""#!/usr/bin/env bash
echo "Data Dir is {{Param.DataDir}}"
cd "{{Param.DataDir}}"

echo "The file contents attached to this job:"
ls

echo "Running index {{Task.Param.Index}}"
sleep 35

# Generate an output file for this task
echo "Content for generated file {{Task.Param.Index}}" > task_output_file_{{Task.Param.Index}}.txt
"""
    )
    use_array_parameter: bool = field(default=True)
    array_parameter_name: str = field(default="Index")
    array_parameter_values: str = field(default="1-5")
    data_dir: str = field(default=os.path.join("~", "CLIJobData"))
    file_format: str = field(default="YAML")


@dataclass
class OsRequirements:
    """
    Settings for a OSRequirementsWidget.
    """

    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"
    OPERATING_SYSTEMS = [LINUX, MACOS, WINDOWS]
    X86_64 = "x86_64"
    ARM_64 = "arm64"
    CPU_ARCHITECTURES = [X86_64, ARM_64]
    operating_systems: List[Literal[LINUX, MACOS, WINDOWS]] = field(default_factory=list)  # type: ignore
    cpu_archs: List[Literal[X86_64, ARM_64]] = field(default_factory=list)  # type: ignore

    def __post_init__(self):
        for operating_system in self.operating_systems:
            if operating_system not in self.OPERATING_SYSTEMS:
                raise ValueError(
                    f"Operating system {operating_system} is not in supported list: {self.OPERATING_SYSTEMS}."
                )
        for cpu in self.cpu_archs:
            if cpu not in self.CPU_ARCHITECTURES:
                raise ValueError(
                    f"CPU architecture {cpu} is in supported list: {self.CPU_ARCHITECTURES}."
                )

    def serialize(self) -> list:
        requirements: List[dict] = []
        if self.operating_systems:
            for operating_system in self.operating_systems:
                if operating_system not in self.OPERATING_SYSTEMS:
                    raise ValueError(
                        f"Operating system {operating_system} is not in supported list: {self.OPERATING_SYSTEMS}."
                    )
            requirements.append(
                {
                    "name": "attr.worker.os.family",
                    "anyOf": self.operating_systems,
                }
            )
        if self.cpu_archs:
            for cpu in self.cpu_archs:
                if cpu not in self.CPU_ARCHITECTURES:
                    raise ValueError(
                        f"CPU architecture {cpu} is not in supported list: {self.CPU_ARCHITECTURES}."
                    )
            requirements.append(
                {
                    "name": "attr.worker.cpu.arch",
                    "anyOf": self.cpu_archs,
                }
            )
        return requirements


@dataclass
class HardwareRequirements:
    """
    Settings for a HardwareRequirementsWidget.
    """

    DEFAULT_VALUE = -1
    cpu_min: int = field(default=DEFAULT_VALUE)
    cpu_max: int = field(default=DEFAULT_VALUE)
    memory_min: int = field(default=DEFAULT_VALUE)
    memory_max: int = field(default=DEFAULT_VALUE)
    acceleration_min: int = field(default=DEFAULT_VALUE)
    acceleration_max: int = field(default=DEFAULT_VALUE)
    acceleration_memory_min: int = field(default=DEFAULT_VALUE)
    acceleration_memory_max: int = field(default=DEFAULT_VALUE)
    scratch_space_min: int = field(default=DEFAULT_VALUE)
    scratch_space_max: int = field(default=DEFAULT_VALUE)

    def __post_init__(self):
        self._validate(self.cpu_min, self.cpu_max, "CPU")
        self._validate(self.memory_min, self.memory_max, "Memory")
        self._validate(self.acceleration_min, self.acceleration_max, "Acceleration")
        self._validate(
            self.acceleration_memory_min, self.acceleration_memory_max, "Acceleration Memory"
        )
        self._validate(self.scratch_space_min, self.scratch_space_max, "Scratch Space")

    def _validate(self, minimum: int, maximum: int, name: str):
        if minimum != self.DEFAULT_VALUE and maximum != self.DEFAULT_VALUE and minimum > maximum:
            raise ValueError(
                f"{name} Minimum cannot be higher than {name} Maximum. {minimum} > {maximum}"
            )

    def _serialize(self, minimum, maximum, name):
        requirements: List[dict] = []
        if minimum != self.DEFAULT_VALUE or maximum != self.DEFAULT_VALUE:
            _requirement = {
                "name": name,
            }
            if minimum != self.DEFAULT_VALUE:
                _requirement["min"] = max(minimum, self.DEFAULT_VALUE)
            if maximum != self.DEFAULT_VALUE:
                _requirement["max"] = min(maximum, MAX_INT_VALUE)
            requirements.append(_requirement)
        return requirements

    def serialize(self) -> list:
        requirements: List[dict] = []
        requirements.extend(self._serialize(self.cpu_min, self.cpu_max, "amount.worker.vcpu"))
        requirements.extend(
            self._serialize(self.memory_min, self.memory_max, "amount.worker.memory")
        )
        requirements.extend(
            self._serialize(self.acceleration_min, self.acceleration_max, "amount.worker.gpu")
        )
        requirements.extend(
            self._serialize(
                self.acceleration_memory_min,
                self.acceleration_memory_max,
                "amount.worker.gpu.memory",
            )
        )
        requirements.extend(
            self._serialize(
                self.scratch_space_min, self.scratch_space_max, "amount.worker.disk.scratch"
            )
        )
        return requirements


@dataclass
class CustomAmountRequirement:
    """
    Settings for a CustomAmountWidget
    """

    DEFAULT_VALUE = -(2**31) + 1
    name: str = field(default="")
    min: int = field(default=DEFAULT_VALUE)
    max: int = field(default=DEFAULT_VALUE)

    def __post_init__(self):
        if not self.name:
            raise ValueError(f"Custom Amount {self} has no name")
        elif self.min and self.max and self.min > self.max:
            raise ValueError(f"Custom Amount {self} has min higher than max")

    def serialize(self) -> dict:
        _requirement = {"name": "amount.worker." + self.name}
        if self.min != self.DEFAULT_VALUE:
            _requirement["min"] = max(self.min, self.DEFAULT_VALUE)
        if self.max != self.DEFAULT_VALUE:
            _requirement["max"] = min(self.max, MAX_INT_VALUE)
        return _requirement


@dataclass
class CustomAttributeRequirement:
    """
    Settings for a CustomAttributeWidget
    """

    ANY_OF = "anyOf"
    ALL_OF = "allOf"
    OPTIONS = [ANY_OF, ALL_OF]
    name: str = field(default="")
    option: Literal[ANY_OF, ALL_OF] = field(default=ALL_OF)  # type: ignore
    values: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.name:
            raise ValueError(f"Custom Amount {self} has no name")
        elif not self.option or self.option not in CustomAttributeRequirement.OPTIONS:
            raise ValueError(
                f"Custom Amount {self} option is not in {CustomAttributeRequirement.OPTIONS}"
            )

    def serialize(self) -> dict:
        if not self.name:
            raise ValueError(f"Custom Attribute {self} has no name")
        _requirement = {
            "name": "attr.worker." + self.name,
        }
        if not self.option:
            raise ValueError(f"Custom Attribute {self} has no option")
        elif self.option not in ["anyOf", "allOf"]:
            raise ValueError(
                f'Custom Attribute {self} has option {self.option} which is not in list: ["anyOf", "allOf"]'
            )
        if not self.values:
            raise ValueError(f"Custom Attribute {self} has no values")
        _requirement[self.option] = self.values
        return _requirement


@dataclass
class CustomRequirements:
    """
    Settings for a CustomRequirementsWidget.
    """

    amounts: List[CustomAmountRequirement] = field(default_factory=list)
    attributes: List[CustomAttributeRequirement] = field(default_factory=list)

    def __post_init__(self):
        amounts = []
        attributes = []
        for amount in self.amounts:
            amounts.append(self._validate_amount(amount))
        for attribute in self.attributes:
            attributes.append(self._validate_attribute(attribute))
        self.amounts = amounts
        self.attributes = attributes

    @staticmethod
    def _validate_amount(amount: Union[dict, CustomAmountRequirement]) -> CustomAmountRequirement:
        if isinstance(amount, dict):
            amount = CustomAmountRequirement(**amount)
        return amount

    @staticmethod
    def _validate_attribute(
        attribute: Union[dict, CustomAttributeRequirement]
    ) -> CustomAttributeRequirement:
        if isinstance(attribute, dict):
            attribute = CustomAttributeRequirement(**attribute)
        return attribute

    def __iter__(self):
        if self.amounts:
            yield from self.amounts
        if self.attributes:
            yield from self.attributes

    def _serialize_amounts(self):
        requirements = []
        for amount in self.amounts:
            requirements.append(amount.serialize())
        return requirements

    def _serialize_attributes(self):
        requirements = []
        for attribute in self.attributes:
            requirements.append(attribute.serialize())
        return requirements

    def serialize(self) -> Dict[str, List]:
        requirements: Dict[str, List] = {}

        if self.amounts:
            requirements.setdefault("amounts", []).extend(self._serialize_amounts())

        if self.attributes:
            requirements.setdefault("attributes", []).extend(self._serialize_attributes())
        return requirements


@dataclass
class HostRequirements:
    """
    Settings for a HostRequirementsWidget.
    """

    os_requirements: OsRequirements = field(default=None)
    hardware_requirements: HardwareRequirements = field(default=None)
    custom_requirements: CustomRequirements = field(default=None)

    def __post_init__(self):
        if isinstance(self.os_requirements, dict):
            self.os_requirements = OsRequirements(**self.os_requirements)
        if isinstance(self.hardware_requirements, dict):
            self.hardware_requirements = HardwareRequirements(**self.hardware_requirements)
        if isinstance(self.custom_requirements, dict):
            self.custom_requirements = CustomRequirements(**self.custom_requirements)

    def serialize(self) -> dict:
        requirements: Dict[str, Union[List[str], List[int]]] = {}
        if self.os_requirements is not None:
            requirements.setdefault("attributes", []).extend(self.os_requirements.serialize())

        if self.hardware_requirements:
            requirements.setdefault("amounts", []).extend(self.hardware_requirements.serialize())

        custom_requirements = self.custom_requirements.serialize()
        if custom_requirements.get("amounts", []):
            requirements.setdefault("amounts", []).extend(custom_requirements["amounts"])
        if custom_requirements.get("attributes", []):
            requirements.setdefault("attributes", []).extend(custom_requirements["attributes"])

        return requirements
