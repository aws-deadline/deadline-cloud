import QtQuick
import QtQuick.Controls
import QtQuick.Controls.Basic as Basic
import QtQuick.Layouts
import com.amazon.deadline.gui

ApplicationWindow {
    id: root
    minimumWidth: 400
    minimumHeight: 400
    width: 580
    height: 750
    visible: true
    title: {
        var name = submitModel.submitter_name
        if (name && name !== "JobBundle") {
            return "Deadline Cloud " + name + " Submitter"
        }
        return "Submit to AWS Deadline Cloud"
    }

    SubmitModel { id: submitModel }
    AuthModel { id: authModel; Component.onCompleted: authModel.start_watching() }
    ResourceModel { id: resourceModel }

    Connections {
        target: authModel
        function onApi_availableChanged() {
            submitModel.set_api_available(authModel.api_available)
            if (authModel.api_available) resourceModel.refresh_farms()
        }
    }

    Connections {
        target: resourceModel
        function onFarm_namesChanged() {
            var idx = resourceModel.selected_farm_index
            var ids = resourceModel.farm_ids.toString().split(";")
            var names = resourceModel.farm_names.toString().split(";")
            if (idx >= 0 && idx < names.length && names[idx]) {
                farmLabel.text = names[idx]
                submitModel.set_farm_queue(ids[idx] || "", submitModel.queue_display)
            }
        }
        function onQueue_namesChanged() {
            var idx = resourceModel.selected_queue_index
            var ids = resourceModel.queue_ids.toString().split(";")
            var names = resourceModel.queue_names.toString().split(";")
            if (idx >= 0 && idx < names.length && names[idx]) {
                queueLabel.text = names[idx]
                var farmIds = resourceModel.farm_ids.toString().split(";")
                var farmIdx = resourceModel.selected_farm_index
                var farmId = (farmIdx >= 0 && farmIdx < farmIds.length) ? farmIds[farmIdx] : ""
                submitModel.set_farm_queue(farmId, ids[idx] || "")
            }
        }
    }

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 10
        spacing: 10

        // ── Tab navigation ──
        Row {
            Layout.alignment: Qt.AlignHCenter
            spacing: 1
            Accessible.name: "tab_group"

            Basic.Button {
                text: "Shared job settings"; Accessible.name: "Shared job settings"
                checkable: true; checked: tabStack.currentIndex === 0; onClicked: tabStack.currentIndex = 0
                flat: tabStack.currentIndex !== 0
                leftPadding: 8; rightPadding: 8; topPadding: 6; bottomPadding: 6
                background: Rectangle {
                    radius: 4
                    border.width: 1
                    border.color: palette.mid
                    color: parent.checked ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                }
                contentItem: Text { text: parent.text; color: parent.checked ? palette.highlightedText : palette.text; horizontalAlignment: Text.AlignHCenter }
            }
            Basic.Button {
                text: "Job-specific settings"; Accessible.name: "Job-specific settings"
                checkable: true; checked: tabStack.currentIndex === 1; onClicked: tabStack.currentIndex = 1
                flat: tabStack.currentIndex !== 1
                leftPadding: 8; rightPadding: 8; topPadding: 6; bottomPadding: 6
                background: Rectangle {
                    radius: 4
                    border.width: 1
                    border.color: palette.mid
                    color: parent.checked ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                }
                contentItem: Text { text: parent.text; color: parent.checked ? palette.highlightedText : palette.text; horizontalAlignment: Text.AlignHCenter }
            }
            Basic.Button {
                text: "Job attachments"; Accessible.name: "Job attachments"
                checkable: true; checked: tabStack.currentIndex === 2; onClicked: tabStack.currentIndex = 2
                flat: tabStack.currentIndex !== 2
                leftPadding: 8; rightPadding: 8; topPadding: 6; bottomPadding: 6
                background: Rectangle {
                    radius: 4
                    border.width: 1
                    border.color: palette.mid
                    color: parent.checked ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                }
                contentItem: Text { text: parent.text; color: parent.checked ? palette.highlightedText : palette.text; horizontalAlignment: Text.AlignHCenter }
            }
            Basic.Button {
                text: "Host requirements"; Accessible.name: "Host requirements"
                checkable: true; checked: tabStack.currentIndex === 3; onClicked: tabStack.currentIndex = 3
                flat: tabStack.currentIndex !== 3
                leftPadding: 8; rightPadding: 8; topPadding: 6; bottomPadding: 6
                background: Rectangle {
                    radius: 4
                    border.width: 1
                    border.color: palette.mid
                    color: parent.checked ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                }
                contentItem: Text { text: parent.text; color: parent.checked ? palette.highlightedText : palette.text; horizontalAlignment: Text.AlignHCenter }
            }
        }

        StackLayout {
            id: tabStack
            Layout.fillWidth: true
            Layout.fillHeight: true

            // ── Tab 0: Shared job settings ──
            ScrollView {
                clip: true
                contentWidth: availableWidth
                ColumnLayout {
                    width: parent.width
                    spacing: 12

                    GroupBox {
                        title: "Job Properties"
                        Layout.fillWidth: true
                        GridLayout {
                            columns: 2
                            columnSpacing: 12
                            rowSpacing: 8
                            anchors.left: parent.left
                            anchors.right: parent.right

                            Label { text: "Name" }
                            TextField { id: nameField; text: submitModel.name; Layout.fillWidth: true; Accessible.name: "Name"; onTextChanged: submitModel.name = text }
                            Label { text: "Description" }
                            TextField { text: submitModel.description; Layout.fillWidth: true; onTextChanged: submitModel.description = text }
                            Label { text: "Priority"; Accessible.name: "Priority" }
                            SpinBox { from: 0; to: 100; value: submitModel.priority; Accessible.name: "Priority"; onValueChanged: submitModel.priority = value }
                            Label { text: "Initial state"; Accessible.name: "Initial state" }
                            ComboBox { model: ["READY", "SUSPENDED"]; currentIndex: submitModel.initial_status === "SUSPENDED" ? 1 : 0; Accessible.name: currentText; onCurrentTextChanged: submitModel.initial_status = currentText }
                            Label { text: "Maximum failed tasks count" }
                            SpinBox { from: 0; to: 2147483647; value: submitModel.max_failed_tasks_count; onValueChanged: submitModel.max_failed_tasks_count = value }
                            Label { text: "Maximum retries per task" }
                            SpinBox { from: 0; to: 2147483647; value: submitModel.max_retries_per_task; onValueChanged: submitModel.max_retries_per_task = value }
                            Label { text: "Maximum worker count" }
                            ColumnLayout {
                                Layout.fillWidth: true
                                RadioButton { text: "No max worker count"; checked: !submitModel.use_max_worker_count; Accessible.name: "Run on all available worker hosts"; onCheckedChanged: if (checked) submitModel.use_max_worker_count = false }
                                RadioButton { text: "Set max worker count"; checked: submitModel.use_max_worker_count; Accessible.name: "Run on worker hosts that meet the following requirements"; onCheckedChanged: if (checked) submitModel.use_max_worker_count = true }
                                SpinBox { visible: submitModel.use_max_worker_count; from: 1; to: 2147483647; value: submitModel.max_worker_count > 0 ? submitModel.max_worker_count : 1; onValueChanged: if (visible) submitModel.max_worker_count = value }
                            }
                        }
                    }

                    GroupBox {
                        title: "Deadline Cloud settings"
                        Layout.fillWidth: true
                        GridLayout {
                            columns: 2
                            columnSpacing: 12
                            rowSpacing: 6
                            anchors.left: parent.left
                            anchors.right: parent.right
                            Label { text: "Farm" }
                            Label { id: farmLabel; text: submitModel.farm_display || "<not configured>"; Layout.fillWidth: true; elide: Text.ElideRight }
                            Label { text: "Queue" }
                            Label { id: queueLabel; text: submitModel.queue_display || "<not configured>"; Layout.fillWidth: true; elide: Text.ElideRight }
                        }
                    }
                }
            }

            // ── Tab 1: Job-specific settings ──
            ScrollView {
                clip: true
                contentWidth: availableWidth
                ColumnLayout {
                    width: parent.width
                    spacing: 12
                    GroupBox {
                        title: "Job Bundle"
                        Layout.fillWidth: true
                        ColumnLayout {
                            anchors.left: parent.left
                            anchors.right: parent.right
                            Label { text: "Bundle directory:"; font.bold: true }
                            Label { text: submitModel.job_bundle_dir || "<none>"; Layout.fillWidth: true; wrapMode: Text.WrapAnywhere }
                        }
                    }
                }
            }

            // ── Tab 2: Job attachments ──
            ScrollView {
                clip: true
                contentWidth: availableWidth
                ColumnLayout {
                    width: parent.width
                    spacing: 12
                    GroupBox { title: "Input files"; Layout.fillWidth: true; Label { text: "No input files configured."; wrapMode: Text.Wrap } }
                    GroupBox { title: "Input directories"; Layout.fillWidth: true; Label { text: "No input directories configured."; wrapMode: Text.Wrap } }
                    GroupBox { title: "Output directories"; Layout.fillWidth: true; Label { text: "No output directories configured."; wrapMode: Text.Wrap } }
                }
            }

            // ── Tab 3: Host requirements ──
            ScrollView {
                clip: true
                contentWidth: availableWidth
                ColumnLayout {
                    width: parent.width
                    spacing: 12
                    GroupBox {
                        title: "Host Requirements"
                        Layout.fillWidth: true
                        ColumnLayout {
                            anchors.left: parent.left
                            anchors.right: parent.right
                            RadioButton { text: "Run on all available worker hosts"; checked: true; Accessible.name: "Run on all available worker hosts" }
                            RadioButton { text: "Run on worker hosts that meet the following requirements"; Accessible.name: "Run on worker hosts that meet the following requirements" }
                        }
                    }
                }
            }
        }

        // ── Auth status bar ──
        GroupBox {
            Layout.fillWidth: true
            padding: 6
            RowLayout {
                anchors.fill: parent
                spacing: 8
                Label { text: authModel.status_text; Layout.fillWidth: true; wrapMode: Text.Wrap }
                Button { text: "Login"; visible: authModel.show_login; onClicked: authModel.login() }
                Button { text: "Logout"; visible: authModel.show_logout; onClicked: authModel.logout() }
            }
        }

        // ── Button bar ──
        RowLayout {
            Layout.fillWidth: true
            spacing: 8
            Button { text: "Settings..."; onClicked: {} }
            Button { text: "Help"; onClicked: {} }
            Item { Layout.fillWidth: true }
            Button { text: "Export bundle"; Accessible.name: "Export bundle"; onClicked: {} }
            Button { text: "Submit"; enabled: submitModel.can_submit; Accessible.name: "Submit"; onClicked: {} }
        }
    }

    Component.onCompleted: {
        submitModel.initialize("")
        resourceModel.set_configured_ids(submitModel.farm_display, submitModel.queue_display, "")
        var profile = submitModel.aws_profile
        resourceModel.set_profile(profile)
        authModel.set_profile(profile)
    }
}
