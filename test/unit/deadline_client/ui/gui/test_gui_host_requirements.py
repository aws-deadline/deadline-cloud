# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for HostRequirementsWidget covering mode toggling, get/set requirements."""

import pytest

try:
    from deadline.client.ui.widgets.host_requirements_tab import (
        HostRequirementsWidget,
        HardwareRequirementsWidget,
        OSRequirementsWidget,
    )
    from deadline.client.ui.dataclasses import (
        HostRequirements,
        HardwareRequirements,
        OsRequirements,
    )
except ImportError:
    pytest.skip("GUI dependencies not available", allow_module_level=True)


class TestHostRequirementsWidget:
    def test_defaults_to_all_workers(self, qtbot):
        """Verify widget defaults to 'Run on all available worker hosts'."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)

        assert widget.mode_selection_box.use_default_button.isChecked()
        assert not widget.mode_selection_box.use_custom_button.isChecked()

    def test_sub_widgets_disabled_by_default(self, qtbot):
        """Verify OS, hardware, and custom sections are disabled when default mode."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)

        assert not widget.os_requirements_box.isEnabled()
        assert not widget.hardware_requirements_box.isEnabled()
        assert not widget.custom_requirements_box.isEnabled()

    def test_custom_mode_enables_sub_widgets(self, qtbot):
        """Verify toggling custom mode enables all sub-widgets."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)

        widget.mode_selection_box.use_custom_button.setChecked(True)

        assert widget.os_requirements_box.isEnabled()
        assert widget.hardware_requirements_box.isEnabled()
        assert widget.custom_requirements_box.isEnabled()

    def test_get_requirements_returns_none_when_default(self, qtbot):
        """Verify get_requirements() returns None in default mode."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)

        assert widget.get_requirements() is None

    def test_get_requirements_returns_empty_when_custom_no_values(self, qtbot):
        """Verify get_requirements() returns empty dict when custom mode but no values set."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)
        widget.mode_selection_box.use_custom_button.setChecked(True)

        result = widget.get_requirements()
        assert result == {}

    def test_set_requirements_enables_custom_mode(self, qtbot):
        """Verify set_requirements() switches to custom mode."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)

        requirements = HostRequirements(
            hardware_requirements=HardwareRequirements(cpu_min=4, cpu_max=8)
        )
        widget.set_requirements(requirements)

        assert widget.mode_selection_box.use_custom_button.isChecked()


class TestHardwareRequirementsWidget:
    def test_get_requirements_empty_by_default(self, qtbot):
        """Verify hardware requirements returns empty list by default."""
        widget = HardwareRequirementsWidget()
        qtbot.addWidget(widget)

        assert widget.get_requirements() == []

    def test_set_requirements_cpu_values(self, qtbot):
        """Verify CPU min/max can be set and retrieved."""
        widget = HardwareRequirementsWidget()
        qtbot.addWidget(widget)

        requirements = HardwareRequirements(cpu_min=2, cpu_max=16)
        widget.set_requirements(requirements)

        result = widget.get_requirements()
        cpu_req = next((r for r in result if r["name"] == "amount.worker.vcpu"), None)
        assert cpu_req is not None
        assert cpu_req["min"] == 2
        assert cpu_req["max"] == 16

    def test_memory_gib_to_mib_scaling(self, qtbot):
        """Verify memory values are scaled from GiB (UI) to MiB (API)."""
        widget = HardwareRequirementsWidget()
        qtbot.addWidget(widget)

        # Set memory to 4 GiB min via set_requirements (which expects MiB)
        requirements = HardwareRequirements(memory_min=4096)  # 4 GiB in MiB
        widget.set_requirements(requirements)

        result = widget.get_requirements()
        mem_req = next((r for r in result if r["name"] == "amount.worker.memory"), None)
        assert mem_req is not None
        # UI shows 4 GiB, output should be 4 * 1024 = 4096 MiB
        assert mem_req["min"] == 4096

    def test_gpu_values(self, qtbot):
        """Verify GPU count requirements work."""
        widget = HardwareRequirementsWidget()
        qtbot.addWidget(widget)

        requirements = HardwareRequirements(acceleration_min=1, acceleration_max=4)
        widget.set_requirements(requirements)

        result = widget.get_requirements()
        gpu_req = next((r for r in result if r["name"] == "amount.worker.gpu"), None)
        assert gpu_req is not None
        assert gpu_req["min"] == 1
        assert gpu_req["max"] == 4


class TestOSRequirementsWidget:
    def test_get_requirements_empty_by_default(self, qtbot):
        """Verify no OS requirements are set by default."""
        widget = OSRequirementsWidget()
        qtbot.addWidget(widget)

        assert widget.get_requirements() == []

    def test_set_requirements_os_family(self, qtbot):
        """Verify OS family selection roundtrips correctly."""
        widget = OSRequirementsWidget()
        qtbot.addWidget(widget)

        requirements = OsRequirements(operating_systems=["linux", "windows"])
        widget.set_requirements(requirements)

        result = widget.get_requirements()
        os_req = next((r for r in result if r["name"] == "attr.worker.os.family"), None)
        assert os_req is not None
        assert set(os_req["anyOf"]) == {"linux", "windows"}

    def test_set_requirements_cpu_arch(self, qtbot):
        """Verify CPU architecture selection roundtrips correctly."""
        widget = OSRequirementsWidget()
        qtbot.addWidget(widget)

        requirements = OsRequirements(cpu_archs=["x86_64"])
        widget.set_requirements(requirements)

        result = widget.get_requirements()
        cpu_req = next((r for r in result if r["name"] == "attr.worker.cpu.arch"), None)
        assert cpu_req is not None
        assert cpu_req["anyOf"] == ["x86_64"]


class TestHostRequirementsIntegration:
    def test_full_requirements_roundtrip(self, qtbot):
        """Verify complete host requirements can be set and retrieved."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)

        requirements = HostRequirements(
            os_requirements=OsRequirements(operating_systems=["linux"], cpu_archs=["x86_64"]),
            hardware_requirements=HardwareRequirements(cpu_min=4, memory_min=8192),
        )
        widget.set_requirements(requirements)

        result = widget.get_requirements()
        assert result is not None
        assert "amounts" in result
        assert "attributes" in result

        cpu_req = next((r for r in result["amounts"] if r["name"] == "amount.worker.vcpu"), None)
        assert cpu_req is not None
        assert cpu_req["min"] == 4

        os_req = next(
            (r for r in result["attributes"] if r["name"] == "attr.worker.os.family"), None
        )
        assert os_req is not None
        assert os_req["anyOf"] == ["linux"]

    def test_toggling_back_to_default_returns_none(self, qtbot):
        """Verify switching back to default mode returns None."""
        widget = HostRequirementsWidget()
        qtbot.addWidget(widget)

        # Set custom requirements
        requirements = HostRequirements(hardware_requirements=HardwareRequirements(cpu_min=4))
        widget.set_requirements(requirements)
        assert widget.get_requirements() is not None

        # Switch back to default
        widget.mode_selection_box.use_default_button.setChecked(True)
        assert widget.get_requirements() is None
