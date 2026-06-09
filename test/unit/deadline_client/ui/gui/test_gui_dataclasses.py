# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Unit tests for deadline.client.ui.dataclasses."""

import pytest

from deadline.client.ui.dataclasses import (
    OsRequirements,
    HardwareRequirements,
    CustomAmountRequirement,
    CustomAttributeRequirement,
    CustomRequirements,
    HostRequirements,
)


class TestOsRequirements:
    def test_construct_with_no_args(self):
        """Default construction should produce empty lists."""
        req = OsRequirements()
        assert req.operating_systems == []
        assert req.cpu_archs == []

    def test_construct_with_valid_os(self):
        """Construction with valid operating systems should succeed."""
        req = OsRequirements(operating_systems=["linux", "windows"])
        assert req.operating_systems == ["linux", "windows"]

    def test_construct_with_valid_cpu_arch(self):
        """Construction with valid CPU architectures should succeed."""
        req = OsRequirements(cpu_archs=["x86_64", "arm64"])
        assert req.cpu_archs == ["x86_64", "arm64"]

    def test_construct_with_all_valid_values(self):
        """Construction with all valid OS and arch values should succeed."""
        req = OsRequirements(
            operating_systems=["linux", "macos", "windows"],
            cpu_archs=["x86_64", "arm64"],
        )
        assert len(req.operating_systems) == 3
        assert len(req.cpu_archs) == 2

    def test_invalid_os_raises_value_error(self):
        """An unsupported operating system should raise ValueError."""
        with pytest.raises(ValueError, match="Operating system"):
            OsRequirements(operating_systems=["bsd"])

    def test_invalid_cpu_arch_raises_value_error(self):
        """An unsupported CPU architecture should raise ValueError."""
        with pytest.raises(ValueError, match="CPU architecture"):
            OsRequirements(cpu_archs=["mips"])

    def test_serialize_empty(self):
        """Serializing with no OS or arch should return empty list."""
        req = OsRequirements()
        assert req.serialize() == []

    def test_serialize_os_only(self):
        """Serializing with only OS should return one attribute entry."""
        req = OsRequirements(operating_systems=["linux"])
        result = req.serialize()
        assert len(result) == 1
        assert result[0]["name"] == "attr.worker.os.family"
        assert result[0]["anyOf"] == ["linux"]

    def test_serialize_cpu_arch_only(self):
        """Serializing with only CPU arch should return one attribute entry."""
        req = OsRequirements(cpu_archs=["arm64"])
        result = req.serialize()
        assert len(result) == 1
        assert result[0]["name"] == "attr.worker.cpu.arch"
        assert result[0]["anyOf"] == ["arm64"]

    def test_serialize_os_and_cpu_arch(self):
        """Serializing with both OS and arch should return two entries."""
        req = OsRequirements(
            operating_systems=["linux", "windows"],
            cpu_archs=["x86_64"],
        )
        result = req.serialize()
        assert len(result) == 2
        os_entry = next(r for r in result if r["name"] == "attr.worker.os.family")
        arch_entry = next(r for r in result if r["name"] == "attr.worker.cpu.arch")
        assert os_entry["anyOf"] == ["linux", "windows"]
        assert arch_entry["anyOf"] == ["x86_64"]

    def test_serialize_multiple_os_values(self):
        """Serializing with all three OS values should list them all in anyOf."""
        req = OsRequirements(operating_systems=["linux", "macos", "windows"])
        result = req.serialize()
        assert result[0]["anyOf"] == ["linux", "macos", "windows"]


class TestHardwareRequirements:
    def test_construct_with_defaults(self):
        """Default construction should set all fields to DEFAULT_VALUE (-1)."""
        req = HardwareRequirements()
        assert req.cpu_min == -1
        assert req.cpu_max == -1
        assert req.memory_min == -1
        assert req.memory_max == -1

    def test_construct_with_valid_cpu_range(self):
        """Setting cpu_min <= cpu_max should succeed."""
        req = HardwareRequirements(cpu_min=2, cpu_max=16)
        assert req.cpu_min == 2
        assert req.cpu_max == 16

    def test_construct_with_only_min(self):
        """Setting only min (max stays default) should succeed."""
        req = HardwareRequirements(cpu_min=4)
        assert req.cpu_min == 4
        assert req.cpu_max == -1

    def test_construct_with_only_max(self):
        """Setting only max (min stays default) should succeed."""
        req = HardwareRequirements(cpu_max=32)
        assert req.cpu_min == -1
        assert req.cpu_max == 32

    def test_cpu_min_greater_than_max_raises_value_error(self):
        """CPU min > max should raise ValueError."""
        with pytest.raises(ValueError, match="CPU Minimum cannot be higher than CPU Maximum"):
            HardwareRequirements(cpu_min=16, cpu_max=4)

    def test_memory_min_greater_than_max_raises_value_error(self):
        """Memory min > max should raise ValueError."""
        with pytest.raises(ValueError, match="Memory Minimum cannot be higher than Memory Maximum"):
            HardwareRequirements(memory_min=8192, memory_max=4096)

    def test_acceleration_min_greater_than_max_raises_value_error(self):
        """Acceleration min > max should raise ValueError."""
        with pytest.raises(ValueError, match="Acceleration Minimum"):
            HardwareRequirements(acceleration_min=8, acceleration_max=2)

    def test_acceleration_memory_min_greater_than_max_raises_value_error(self):
        """Acceleration memory min > max should raise ValueError."""
        with pytest.raises(ValueError, match="Acceleration Memory Minimum"):
            HardwareRequirements(acceleration_memory_min=1024, acceleration_memory_max=512)

    def test_scratch_space_min_greater_than_max_raises_value_error(self):
        """Scratch space min > max should raise ValueError."""
        with pytest.raises(ValueError, match="Scratch Space Minimum"):
            HardwareRequirements(scratch_space_min=100, scratch_space_max=50)

    def test_equal_min_and_max_is_valid(self):
        """min == max should not raise."""
        req = HardwareRequirements(cpu_min=8, cpu_max=8)
        assert req.cpu_min == 8
        assert req.cpu_max == 8

    def test_serialize_defaults_returns_empty(self):
        """Serializing default values should return empty list."""
        req = HardwareRequirements()
        assert req.serialize() == []

    def test_serialize_cpu_min_only(self):
        """Serializing with only cpu_min should produce one entry with only min."""
        req = HardwareRequirements(cpu_min=4)
        result = req.serialize()
        assert len(result) == 1
        assert result[0]["name"] == "amount.worker.vcpu"
        assert result[0]["min"] == 4
        assert "max" not in result[0]

    def test_serialize_cpu_max_only(self):
        """Serializing with only cpu_max should produce one entry with only max."""
        req = HardwareRequirements(cpu_max=32)
        result = req.serialize()
        assert len(result) == 1
        assert result[0]["name"] == "amount.worker.vcpu"
        assert result[0]["max"] == 32
        assert "min" not in result[0]

    def test_serialize_cpu_min_and_max(self):
        """Serializing with both cpu_min and cpu_max should produce entry with both."""
        req = HardwareRequirements(cpu_min=2, cpu_max=16)
        result = req.serialize()
        cpu_entry = next(r for r in result if r["name"] == "amount.worker.vcpu")
        assert cpu_entry["min"] == 2
        assert cpu_entry["max"] == 16

    def test_serialize_multiple_hardware_types(self):
        """Serializing multiple hardware types should produce multiple entries."""
        req = HardwareRequirements(
            cpu_min=4,
            memory_min=8192,
            acceleration_min=1,
        )
        result = req.serialize()
        names = [r["name"] for r in result]
        assert "amount.worker.vcpu" in names
        assert "amount.worker.memory" in names
        assert "amount.worker.gpu" in names

    def test_serialize_all_hardware_types(self):
        """Serializing all five hardware types should produce five entries."""
        req = HardwareRequirements(
            cpu_min=4,
            memory_min=2048,
            acceleration_min=1,
            acceleration_memory_min=512,
            scratch_space_min=100,
        )
        result = req.serialize()
        assert len(result) == 5
        names = [r["name"] for r in result]
        assert "amount.worker.vcpu" in names
        assert "amount.worker.memory" in names
        assert "amount.worker.gpu" in names
        assert "amount.worker.gpu.memory" in names
        assert "amount.worker.disk.scratch" in names

    def test_serialize_max_clamped_to_max_int(self):
        """Max values should be clamped to MAX_INT_VALUE (2^31 - 1)."""
        huge_value = 2**32
        req = HardwareRequirements(cpu_max=huge_value)
        result = req.serialize()
        assert result[0]["max"] == (2**31) - 1


class TestCustomAmountRequirement:
    def test_construct_with_valid_name(self):
        """Construction with a name should succeed."""
        req = CustomAmountRequirement(name="mycustom")
        assert req.name == "mycustom"

    def test_no_name_raises_value_error(self):
        """Construction without a name should raise ValueError."""
        with pytest.raises(ValueError, match="has no name"):
            CustomAmountRequirement(name="")

    def test_min_greater_than_max_raises_value_error(self):
        """Explicit min > max should raise ValueError."""
        with pytest.raises(ValueError, match="has min higher than max"):
            CustomAmountRequirement(name="test", min=10, max=5)

    def test_construct_with_only_min(self):
        """Setting only min (max stays at DEFAULT_VALUE) should succeed."""
        req = CustomAmountRequirement(name="test", min=3)
        assert req.min == 3
        assert req.max == CustomAmountRequirement.DEFAULT_VALUE

    def test_min_equals_max_is_valid(self):
        """min == max should not raise."""
        req = CustomAmountRequirement(name="test", min=5, max=5)
        assert req.min == 5
        assert req.max == 5

    def test_construct_with_zero_min(self):
        """min=0 should be valid."""
        req = CustomAmountRequirement(name="test", min=0)
        assert req.min == 0

    def test_construct_with_only_max(self):
        """Only setting max should succeed."""
        req = CustomAmountRequirement(name="test", max=10)
        assert req.max == 10

    def test_construct_with_min_and_max(self):
        """Setting both min and max with valid range should succeed."""
        req = CustomAmountRequirement(name="test", min=2, max=8)
        assert req.min == 2
        assert req.max == 8

    def test_serialize_name_prefix(self):
        """Serialized name should be prefixed with 'amount.worker.'."""
        req = CustomAmountRequirement(name="mycustom", min=1, max=10)
        result = req.serialize()
        assert result["name"] == "amount.worker.mycustom"

    def test_serialize_with_min_and_max(self):
        """Serializing with both min and max should include both."""
        req = CustomAmountRequirement(name="test", min=2, max=8)
        result = req.serialize()
        assert result["min"] == 2
        assert result["max"] == 8

    def test_serialize_with_max_only(self):
        """Serializing with only max set should include max but not min."""
        req = CustomAmountRequirement(name="test", max=10)
        result = req.serialize()
        assert result["max"] == 10
        assert "min" not in result

    def test_serialize_max_clamped_to_max_int(self):
        """Max should be clamped to MAX_INT_VALUE."""
        req = CustomAmountRequirement(name="test", max=2**32)
        result = req.serialize()
        assert result["max"] == (2**31) - 1

    def test_serialize_with_default_values_excludes_min_and_max(self):
        """Serializing with default min/max should exclude both keys."""
        req = CustomAmountRequirement(name="test")
        result = req.serialize()
        assert result == {"name": "amount.worker.test"}


class TestCustomAttributeRequirement:
    def test_construct_with_valid_name_and_values(self):
        """Construction with name, option, and values should succeed."""
        req = CustomAttributeRequirement(name="myattr", option="allOf", values=["a", "b"])
        assert req.name == "myattr"
        assert req.option == "allOf"
        assert req.values == ["a", "b"]

    def test_no_name_raises_value_error(self):
        """Construction without a name should raise ValueError."""
        with pytest.raises(ValueError, match="has no name"):
            CustomAttributeRequirement(name="", values=["a"])

    def test_invalid_option_raises_value_error(self):
        """Construction with an invalid option should raise ValueError."""
        with pytest.raises(ValueError, match="option is not in"):
            CustomAttributeRequirement(name="test", option="noneOf", values=["a"])

    def test_default_option_is_allof(self):
        """Default option should be 'allOf'."""
        req = CustomAttributeRequirement(name="test", values=["a"])
        assert req.option == "allOf"

    def test_construct_with_anyof(self):
        """Construction with 'anyOf' option should succeed."""
        req = CustomAttributeRequirement(name="test", option="anyOf", values=["a"])
        assert req.option == "anyOf"

    def test_serialize_with_allof(self):
        """Serializing with allOf should use 'allOf' key."""
        req = CustomAttributeRequirement(name="test", option="allOf", values=["val1", "val2"])
        result = req.serialize()
        assert result["name"] == "attr.worker.test"
        assert result["allOf"] == ["val1", "val2"]
        assert "anyOf" not in result

    def test_serialize_with_anyof(self):
        """Serializing with anyOf should use 'anyOf' key."""
        req = CustomAttributeRequirement(name="test", option="anyOf", values=["val1"])
        result = req.serialize()
        assert result["name"] == "attr.worker.test"
        assert result["anyOf"] == ["val1"]
        assert "allOf" not in result

    def test_serialize_no_values_raises_value_error(self):
        """Serializing with empty values should raise ValueError."""
        req = CustomAttributeRequirement(name="test", option="allOf", values=[])
        with pytest.raises(ValueError, match="has no values"):
            req.serialize()

    def test_serialize_name_prefix(self):
        """Serialized name should be prefixed with 'attr.worker.'."""
        req = CustomAttributeRequirement(name="custom.thing", option="allOf", values=["x"])
        result = req.serialize()
        assert result["name"] == "attr.worker.custom.thing"


class TestCustomRequirements:
    def test_construct_with_empty_lists(self):
        """Default construction should have empty amounts and attributes."""
        req = CustomRequirements()
        assert req.amounts == []
        assert req.attributes == []

    def test_construct_with_dataclass_instances(self):
        """Construction with dataclass instances should work."""
        amount = CustomAmountRequirement(name="test_amount", min=1, max=10)
        attribute = CustomAttributeRequirement(name="test_attr", option="allOf", values=["v1"])
        req = CustomRequirements(amounts=[amount], attributes=[attribute])
        assert len(req.amounts) == 1
        assert len(req.attributes) == 1
        assert isinstance(req.amounts[0], CustomAmountRequirement)
        assert isinstance(req.attributes[0], CustomAttributeRequirement)

    def test_construct_with_dicts_converts_to_dataclasses(self):
        """Construction with dicts should convert them to dataclass instances."""
        req = CustomRequirements(
            amounts=[{"name": "test_amount", "min": 1, "max": 10}],  # type: ignore[list-item]
            attributes=[{"name": "test_attr", "option": "allOf", "values": ["v1"]}],  # type: ignore[list-item]
        )
        assert isinstance(req.amounts[0], CustomAmountRequirement)
        assert req.amounts[0].name == "test_amount"
        assert isinstance(req.attributes[0], CustomAttributeRequirement)
        assert req.attributes[0].name == "test_attr"

    def test_construct_with_invalid_dict_amount_raises(self):
        """A dict with invalid amount data should raise during conversion."""
        with pytest.raises(ValueError, match="has no name"):
            CustomRequirements(amounts=[{"name": ""}])  # type: ignore[list-item]

    def test_construct_with_invalid_dict_attribute_raises(self):
        """A dict with invalid attribute data should raise during conversion."""
        with pytest.raises(ValueError, match="has no name"):
            CustomRequirements(attributes=[{"name": ""}])  # type: ignore[list-item]

    def test_iter_yields_amounts_and_attributes(self):
        """__iter__ should yield all amounts followed by all attributes."""
        amount = CustomAmountRequirement(name="a", min=1, max=10)
        attribute = CustomAttributeRequirement(name="b", option="allOf", values=["x"])
        req = CustomRequirements(amounts=[amount], attributes=[attribute])
        items = list(req)
        assert len(items) == 2
        assert items[0] is amount
        assert items[1] is attribute

    def test_iter_empty(self):
        """__iter__ on empty CustomRequirements should yield nothing."""
        req = CustomRequirements()
        assert list(req) == []

    def test_iter_amounts_only(self):
        """__iter__ with only amounts should yield just amounts."""
        amount = CustomAmountRequirement(name="a", min=1, max=10)
        req = CustomRequirements(amounts=[amount])
        items = list(req)
        assert len(items) == 1
        assert isinstance(items[0], CustomAmountRequirement)

    def test_iter_attributes_only(self):
        """__iter__ with only attributes should yield just attributes."""
        attr = CustomAttributeRequirement(name="b", option="anyOf", values=["x"])
        req = CustomRequirements(attributes=[attr])
        items = list(req)
        assert len(items) == 1
        assert isinstance(items[0], CustomAttributeRequirement)

    def test_serialize_empty(self):
        """Serializing empty requirements should return empty dict."""
        req = CustomRequirements()
        assert req.serialize() == {}

    def test_serialize_amounts_only(self):
        """Serializing with only amounts should return dict with 'amounts' key."""
        req = CustomRequirements(
            amounts=[CustomAmountRequirement(name="test", min=3, max=10)],
        )
        result = req.serialize()
        assert "amounts" in result
        assert "attributes" not in result
        assert len(result["amounts"]) == 1
        assert result["amounts"][0]["name"] == "amount.worker.test"

    def test_serialize_attributes_only(self):
        """Serializing with only attributes should return dict with 'attributes' key."""
        req = CustomRequirements(
            attributes=[
                CustomAttributeRequirement(name="test", option="allOf", values=["v1"]),
            ]
        )
        result = req.serialize()
        assert "attributes" in result
        assert "amounts" not in result
        assert len(result["attributes"]) == 1
        assert result["attributes"][0]["name"] == "attr.worker.test"

    def test_serialize_both_amounts_and_attributes(self):
        """Serializing with both should return dict with both keys."""
        req = CustomRequirements(
            amounts=[CustomAmountRequirement(name="amt", min=1, max=10)],
            attributes=[
                CustomAttributeRequirement(name="att", option="anyOf", values=["v"]),
            ],
        )
        result = req.serialize()
        assert "amounts" in result
        assert "attributes" in result
        assert len(result["amounts"]) == 1
        assert len(result["attributes"]) == 1

    def test_serialize_multiple_amounts(self):
        """Serializing multiple amounts should list them all."""
        req = CustomRequirements(
            amounts=[
                CustomAmountRequirement(name="a1", min=1, max=10),
                CustomAmountRequirement(name="a2", max=5),
            ]
        )
        result = req.serialize()
        assert len(result["amounts"]) == 2
        names = [a["name"] for a in result["amounts"]]
        assert "amount.worker.a1" in names
        assert "amount.worker.a2" in names


class TestHostRequirements:
    def test_construct_with_no_args(self):
        """Default construction should have None sub-requirements."""
        req = HostRequirements()
        assert req.os_requirements is None
        assert req.hardware_requirements is None
        assert req.custom_requirements is None

    def test_construct_with_dataclass_instances(self):
        """Construction with dataclass instances should preserve them."""
        os_req = OsRequirements(operating_systems=["linux"])
        hw_req = HardwareRequirements(cpu_min=4)
        custom_req = CustomRequirements()
        req = HostRequirements(
            os_requirements=os_req,
            hardware_requirements=hw_req,
            custom_requirements=custom_req,
        )
        assert req.os_requirements is os_req
        assert req.hardware_requirements is hw_req
        assert req.custom_requirements is custom_req

    def test_construct_with_dicts_converts_os_requirements(self):
        """A dict for os_requirements should be converted to OsRequirements."""
        req = HostRequirements(
            os_requirements={"operating_systems": ["linux", "windows"]},  # type: ignore[arg-type]
        )
        assert isinstance(req.os_requirements, OsRequirements)
        assert req.os_requirements.operating_systems == ["linux", "windows"]

    def test_construct_with_dicts_converts_hardware_requirements(self):
        """A dict for hardware_requirements should be converted to HardwareRequirements."""
        req = HostRequirements(
            hardware_requirements={"cpu_min": 2, "cpu_max": 8},  # type: ignore[arg-type]
        )
        assert isinstance(req.hardware_requirements, HardwareRequirements)
        assert req.hardware_requirements.cpu_min == 2
        assert req.hardware_requirements.cpu_max == 8

    def test_construct_with_dicts_converts_custom_requirements(self):
        """A dict for custom_requirements should be converted to CustomRequirements."""
        req = HostRequirements(
            custom_requirements={  # type: ignore[arg-type]
                "amounts": [{"name": "a", "min": 1, "max": 10}],
                "attributes": [{"name": "b", "option": "allOf", "values": ["x"]}],
            },
        )
        assert isinstance(req.custom_requirements, CustomRequirements)
        assert len(req.custom_requirements.amounts) == 1
        assert len(req.custom_requirements.attributes) == 1

    def test_construct_with_invalid_os_dict_raises(self):
        """A dict with invalid OS should raise ValueError."""
        with pytest.raises(ValueError, match="Operating system"):
            HostRequirements(os_requirements={"operating_systems": ["bsd"]})  # type: ignore[arg-type]

    def test_construct_with_invalid_hardware_dict_raises(self):
        """A dict with invalid hardware ranges should raise ValueError."""
        with pytest.raises(ValueError, match="Minimum cannot be higher"):
            HostRequirements(hardware_requirements={"cpu_min": 16, "cpu_max": 4})  # type: ignore[arg-type]

    def test_serialize_with_os_requirements(self):
        """Serializing with OS requirements should produce attributes."""
        req = HostRequirements(
            os_requirements=OsRequirements(operating_systems=["linux"]),
            custom_requirements=CustomRequirements(),
        )
        result = req.serialize()
        assert "attributes" in result
        os_entry = next(r for r in result["attributes"] if r["name"] == "attr.worker.os.family")
        assert os_entry["anyOf"] == ["linux"]

    def test_serialize_with_hardware_requirements(self):
        """Serializing with hardware requirements should produce amounts."""
        req = HostRequirements(
            hardware_requirements=HardwareRequirements(cpu_min=4, cpu_max=16),
            custom_requirements=CustomRequirements(),
        )
        result = req.serialize()
        assert "amounts" in result
        cpu_entry = next(r for r in result["amounts"] if r["name"] == "amount.worker.vcpu")
        assert cpu_entry["min"] == 4
        assert cpu_entry["max"] == 16

    def test_serialize_with_custom_requirements(self):
        """Serializing with custom requirements should merge amounts and attributes."""
        req = HostRequirements(
            custom_requirements=CustomRequirements(
                amounts=[CustomAmountRequirement(name="custom_amt", min=1, max=10)],
                attributes=[
                    CustomAttributeRequirement(name="custom_attr", option="allOf", values=["v"]),
                ],
            ),
        )
        result = req.serialize()
        assert "amounts" in result
        assert "attributes" in result
        assert any(a["name"] == "amount.worker.custom_amt" for a in result["amounts"])
        assert any(a["name"] == "attr.worker.custom_attr" for a in result["attributes"])

    def test_serialize_combines_hardware_and_custom_amounts(self):
        """Hardware amounts and custom amounts should merge into one 'amounts' list."""
        req = HostRequirements(
            hardware_requirements=HardwareRequirements(cpu_min=4),
            custom_requirements=CustomRequirements(
                amounts=[CustomAmountRequirement(name="extra", min=2, max=20)],
            ),
        )
        result = req.serialize()
        amount_names = [a["name"] for a in result["amounts"]]
        assert "amount.worker.vcpu" in amount_names
        assert "amount.worker.extra" in amount_names

    def test_serialize_combines_os_and_custom_attributes(self):
        """OS attributes and custom attributes should merge into one 'attributes' list."""
        req = HostRequirements(
            os_requirements=OsRequirements(operating_systems=["linux"]),
            custom_requirements=CustomRequirements(
                attributes=[
                    CustomAttributeRequirement(name="myattr", option="anyOf", values=["x"]),
                ],
            ),
        )
        result = req.serialize()
        attr_names = [a["name"] for a in result["attributes"]]
        assert "attr.worker.os.family" in attr_names
        assert "attr.worker.myattr" in attr_names

    def test_serialize_full_host_requirements(self):
        """Serializing with all sub-requirements populated should combine everything."""
        req = HostRequirements(
            os_requirements=OsRequirements(
                operating_systems=["linux"],
                cpu_archs=["x86_64"],
            ),
            hardware_requirements=HardwareRequirements(
                cpu_min=4,
                cpu_max=16,
                memory_min=8192,
            ),
            custom_requirements=CustomRequirements(
                amounts=[CustomAmountRequirement(name="licenses", min=1, max=10)],
                attributes=[
                    CustomAttributeRequirement(
                        name="software", option="allOf", values=["maya", "nuke"]
                    ),
                ],
            ),
        )
        result = req.serialize()

        # Check attributes: os.family, cpu.arch, custom attribute
        assert "attributes" in result
        attr_names = [a["name"] for a in result["attributes"]]
        assert "attr.worker.os.family" in attr_names
        assert "attr.worker.cpu.arch" in attr_names
        assert "attr.worker.software" in attr_names

        # Check amounts: vcpu, memory, custom amount
        assert "amounts" in result
        amount_names = [a["name"] for a in result["amounts"]]
        assert "amount.worker.vcpu" in amount_names
        assert "amount.worker.memory" in amount_names
        assert "amount.worker.licenses" in amount_names


class TestHostRequirementsFromDict:
    """Tests for HostRequirements.from_dict, the inverse of serialize()."""

    def test_empty_dict_returns_empty_requirements(self):
        """An empty dict should produce a HostRequirements with no settings."""
        req = HostRequirements.from_dict({})
        assert req.serialize() == {}

    def test_parses_os_family(self):
        """attr.worker.os.family should map to os_requirements.operating_systems."""
        req = HostRequirements.from_dict(
            {"attributes": [{"name": "attr.worker.os.family", "anyOf": ["linux", "windows"]}]}
        )
        assert isinstance(req.os_requirements, OsRequirements)
        assert req.os_requirements.operating_systems == ["linux", "windows"]

    def test_parses_cpu_arch(self):
        """attr.worker.cpu.arch should map to os_requirements.cpu_archs."""
        req = HostRequirements.from_dict(
            {"attributes": [{"name": "attr.worker.cpu.arch", "anyOf": ["x86_64"]}]}
        )
        assert req.os_requirements is not None
        assert req.os_requirements.cpu_archs == ["x86_64"]

    def test_parses_vcpu_min_and_max(self):
        """amount.worker.vcpu should map to hardware cpu_min/cpu_max."""
        req = HostRequirements.from_dict(
            {"amounts": [{"name": "amount.worker.vcpu", "min": 8, "max": 64}]}
        )
        assert req.hardware_requirements is not None
        assert req.hardware_requirements.cpu_min == 8
        assert req.hardware_requirements.cpu_max == 64

    def test_parses_memory_in_mib(self):
        """amount.worker.memory should be stored in MiB (template units)."""
        req = HostRequirements.from_dict(
            {"amounts": [{"name": "amount.worker.memory", "min": 16384}]}
        )
        # Stored as MiB so it round-trips through serialize()
        assert req.hardware_requirements is not None
        assert req.hardware_requirements.memory_min == 16384

    def test_parses_min_only_leaves_max_default(self):
        """An amount with only a min should leave max at the default."""
        req = HostRequirements.from_dict({"amounts": [{"name": "amount.worker.vcpu", "min": 4}]})
        assert req.hardware_requirements is not None
        assert req.hardware_requirements.cpu_min == 4
        assert req.hardware_requirements.cpu_max == HardwareRequirements.DEFAULT_VALUE

    def test_parses_max_only_leaves_min_default(self):
        """An amount with only a max should leave min at the default."""
        req = HostRequirements.from_dict({"amounts": [{"name": "amount.worker.gpu", "max": 4}]})
        assert req.hardware_requirements is not None
        assert req.hardware_requirements.acceleration_max == 4
        assert req.hardware_requirements.acceleration_min == HardwareRequirements.DEFAULT_VALUE

    def test_parses_gpu_memory_and_scratch(self):
        """GPU memory and scratch space well-known amounts should be parsed."""
        req = HostRequirements.from_dict(
            {
                "amounts": [
                    {"name": "amount.worker.gpu.memory", "min": 2048},
                    {"name": "amount.worker.disk.scratch", "min": 100},
                ]
            }
        )
        assert req.hardware_requirements is not None
        assert req.hardware_requirements.acceleration_memory_min == 2048
        assert req.hardware_requirements.scratch_space_min == 100

    def test_parses_custom_amount(self):
        """A custom amount (bare 'amount.' prefix) should round-trip with the prefix stripped.

        OpenJD reserves the 'amount.worker.' namespace for its own capabilities,
        so user-defined amounts use the bare 'amount.' prefix.
        """
        req = HostRequirements.from_dict(
            {"amounts": [{"name": "amount.Bugs", "min": 1, "max": 10}]}
        )
        assert req.custom_requirements is not None
        assert len(req.custom_requirements.amounts) == 1
        custom = req.custom_requirements.amounts[0]
        assert custom.name == "Bugs"
        assert custom.min == 1
        assert custom.max == 10

    def test_parses_custom_attribute_anyof(self):
        """A custom attribute (bare 'attr.' prefix) should become a custom attribute (anyOf)."""
        req = HostRequirements.from_dict(
            {"attributes": [{"name": "attr.pipelineFeatures", "anyOf": ["feature1", "feature2"]}]}
        )
        assert req.custom_requirements is not None
        assert len(req.custom_requirements.attributes) == 1
        custom = req.custom_requirements.attributes[0]
        assert custom.name == "pipelineFeatures"
        assert custom.option == "anyOf"
        assert custom.values == ["feature1", "feature2"]

    def test_parses_custom_attribute_allof(self):
        """A custom attribute using allOf should preserve the allOf option."""
        req = HostRequirements.from_dict(
            {"attributes": [{"name": "attr.software", "allOf": ["maya", "nuke"]}]}
        )
        assert req.custom_requirements is not None
        custom = req.custom_requirements.attributes[0]
        assert custom.option == "allOf"
        assert custom.values == ["maya", "nuke"]

    @staticmethod
    def _compare_requirements(first, second):
        assert len(first.get("amounts", [])) == len(second.get("amounts", []))
        assert len(first.get("attributes", [])) == len(second.get("attributes", []))
        for amount in first.get("amounts", []):
            ref = next(a for a in second["amounts"] if a["name"] == amount["name"])
            assert ref.get("min") == amount.get("min")
            assert ref.get("max") == amount.get("max")
        for attribute in first.get("attributes", []):
            ref = next(a for a in second["attributes"] if a["name"] == attribute["name"])
            operation = "anyOf" if "anyOf" in attribute else "allOf"
            assert operation in ref
            assert set(attribute[operation]) == set(ref[operation])

    def test_roundtrip_well_known_capabilities_through_serialize(self):
        """For well-known capabilities, from_dict then serialize reproduces the input.

        Custom capabilities are intentionally excluded here: the widget (the real
        submission path) and OpenJD use the bare ``attr.``/``amount.`` prefix for
        custom names, while the dataclass ``serialize()`` re-adds ``attr.worker.``/
        ``amount.worker.``, so the two are not symmetric for custom entries. The
        custom-prefix handling is covered by the dedicated ``test_parses_custom_*``
        tests above.
        """
        source = {
            "attributes": [
                {"name": "attr.worker.os.family", "anyOf": ["windows"]},
                {"name": "attr.worker.cpu.arch", "anyOf": ["x86_64"]},
            ],
            "amounts": [
                {"name": "amount.worker.vcpu", "min": 8, "max": 64},
                {"name": "amount.worker.memory", "min": 16384, "max": 131072},
            ],
        }
        result = HostRequirements.from_dict(source).serialize()
        self._compare_requirements(result, source)
