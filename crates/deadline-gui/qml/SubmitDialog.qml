import QtQuick
import QtQuick.Controls
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
        return "Deadline Cloud " + name + " Submitter"
    }

    SubmitModel { id: submitModel }
    AuthModel { id: authModel; Component.onCompleted: authModel.start_watching() }
    ResourceModel { id: resourceModel }
    ParameterListModel { id: parameterModel }
    AttachmentModel { id: attachmentModel }

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
                var fid = ids[idx] || ""
                if (fid) submitModel.set_farm_queue(fid, submitModel.queue_display)
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
                var qid = ids[idx] || ""
                if (farmId && qid) {
                    submitModel.set_farm_queue(farmId, qid)
                    parameterModel.refresh(farmId, qid, submitModel.aws_profile)
                }
            }
        }
    }

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 10
        spacing: 10

        // ── Tab navigation ──
        TabBar {
            id: tabBar
            Layout.alignment: Qt.AlignHCenter
            Layout.fillWidth: true
            background: Rectangle { color: "transparent" }

            TabButton {
                text: "Shared job settings"
                background: Rectangle {
                    implicitHeight: 32
                    color: tabBar.currentIndex === 0 ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                    radius: 4
                }
                contentItem: Text {
                    text: parent.text
                    color: tabBar.currentIndex === 0 ? palette.highlightedText : palette.text
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                }
            }
            TabButton {
                text: "Job-specific settings"
                background: Rectangle {
                    implicitHeight: 32
                    color: tabBar.currentIndex === 1 ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                    radius: 4
                }
                contentItem: Text {
                    text: parent.text
                    color: tabBar.currentIndex === 1 ? palette.highlightedText : palette.text
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                }
            }
            TabButton {
                text: "Job attachments"
                background: Rectangle {
                    implicitHeight: 32
                    color: tabBar.currentIndex === 2 ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                    radius: 4
                }
                contentItem: Text {
                    text: parent.text
                    color: tabBar.currentIndex === 2 ? palette.highlightedText : palette.text
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                }
            }
            TabButton {
                text: "Host requirements"
                background: Rectangle {
                    implicitHeight: 32
                    color: tabBar.currentIndex === 3 ? palette.highlight : (parent.hovered ? palette.midlight : "transparent")
                    radius: 4
                }
                contentItem: Text {
                    text: parent.text
                    color: tabBar.currentIndex === 3 ? palette.highlightedText : palette.text
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                }
            }
        }

        StackLayout {
            id: tabStack
            currentIndex: tabBar.currentIndex
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

                    // ── Queue Parameters (dynamic) ──
                    Loader {
                        Layout.fillWidth: true
                        active: true
                        sourceComponent: parameterModel.is_loading
                            ? loadingComponent
                            : parameterModel.loading_state.toString() !== ""
                              ? errorComponent
                              : parameterModel.parameters_json.toString() !== "[]"
                                ? parametersComponent
                                : null
                    }

                    Component {
                        id: loadingComponent
                        Label {
                            text: "Loading Queue Environments..."
                            horizontalAlignment: Text.AlignHCenter
                            Accessible.name: "Loading Queue Environments"
                        }
                    }

                    Component {
                        id: errorComponent
                        Label {
                            text: parameterModel.loading_state
                            color: "red"
                            wrapMode: Text.Wrap
                        }
                    }

                    Component {
                        id: parametersComponent
                        ColumnLayout {
                            spacing: 8
                            width: parent ? parent.width : 0
                            Repeater {
                                model: {
                                    try {
                                        var params = JSON.parse(parameterModel.parameters_json)
                                        return params.filter(function(p) {
                                            return p.name.indexOf(":") === -1
                                        })
                                    } catch(e) { return [] }
                                }
                                delegate: ColumnLayout {
                                    Layout.fillWidth: true
                                    visible: {
                                        var ctrl = modelData._resolvedControl || ""
                                        return ctrl !== "HIDDEN"
                                    }
                                    // Group label header
                                    Label {
                                        visible: {
                                            var gl = (modelData.userInterface || {}).groupLabel || ""
                                            if (!gl) return false
                                            var params = JSON.parse(parameterModel.parameters_json).filter(function(p) { return p.name.indexOf(":") === -1 })
                                            for (var i = 0; i < params.length; i++) {
                                                if ((params[i].userInterface || {}).groupLabel === gl) {
                                                    return params[i].name === modelData.name
                                                }
                                            }
                                            return false
                                        }
                                        text: (modelData.userInterface || {}).groupLabel || ""
                                        font.bold: true
                                    }
                                    RowLayout {
                                        Layout.fillWidth: true
                                        Label {
                                            text: (modelData.userInterface || {}).label || modelData.name
                                            Layout.minimumWidth: 120
                                            Layout.maximumWidth: 250
                                            wrapMode: Text.Wrap
                                            Accessible.name: (modelData.userInterface || {}).label || modelData.name
                                        }
                                        // LINE_EDIT / MULTILINE_EDIT
                                        TextField {
                                            visible: modelData._resolvedControl === "LINE_EDIT" || modelData._resolvedControl === "MULTILINE_EDIT"
                                            Layout.fillWidth: true
                                            text: modelData.value !== undefined ? String(modelData.value) : (modelData["default"] !== undefined ? String(modelData["default"]) : "")
                                            Accessible.name: modelData.name
                                            onTextChanged: parameterModel.set_parameter_value(modelData.name, text)
                                        }
                                        // DROPDOWN_LIST
                                        ComboBox {
                                            visible: modelData._resolvedControl === "DROPDOWN_LIST"
                                            Layout.fillWidth: true
                                            model: modelData.allowedValues || []
                                            currentIndex: {
                                                var vals = modelData.allowedValues || []
                                                var cur = modelData.value !== undefined ? String(modelData.value) : (modelData["default"] !== undefined ? String(modelData["default"]) : "")
                                                var idx = vals.indexOf(cur)
                                                return idx >= 0 ? idx : 0
                                            }
                                            Accessible.name: currentText
                                            onActivated: parameterModel.set_parameter_value(modelData.name, currentText)
                                        }
                                        // SPIN_BOX (INT/FLOAT)
                                        SpinBox {
                                            visible: modelData._resolvedControl === "SPIN_BOX"
                                            Layout.fillWidth: true
                                            from: modelData.minValue !== undefined ? Number(modelData.minValue) : -2147483647
                                            to: modelData.maxValue !== undefined ? Number(modelData.maxValue) : 2147483647
                                            value: {
                                                var v = modelData.value !== undefined ? modelData.value : modelData["default"]
                                                return v !== undefined ? Number(v) : 0
                                            }
                                            Accessible.name: modelData.name
                                            onValueChanged: parameterModel.set_parameter_value(modelData.name, String(value))
                                        }
                                        // CHECK_BOX
                                        CheckBox {
                                            visible: modelData._resolvedControl === "CHECK_BOX"
                                            Layout.fillWidth: true
                                            checked: {
                                                var v = modelData.value !== undefined ? String(modelData.value) : (modelData["default"] !== undefined ? String(modelData["default"]) : "")
                                                return v.toUpperCase() === "TRUE" || v.toUpperCase() === "YES" || v.toUpperCase() === "ON" || v === "1"
                                            }
                                            Accessible.name: (modelData.userInterface || {}).label || modelData.name
                                            onCheckedChanged: {
                                                var vals = modelData.allowedValues || ["True", "False"]
                                                parameterModel.set_parameter_value(modelData.name, checked ? vals[0] : vals[1])
                                            }
                                        }
                                    }
                                }
                            }
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

                    CheckBox {
                        text: "Require all input paths exist"
                        checked: attachmentModel.require_paths_exist
                        Accessible.name: "Require all input paths exist"
                        onCheckedChanged: attachmentModel.require_paths_exist = checked
                    }

                    GroupBox {
                        title: "Input files"
                        Layout.fillWidth: true
                        ColumnLayout {
                            anchors.left: parent.left
                            anchors.right: parent.right
                            RowLayout {
                                Button { text: "Add..."; Accessible.name: "Add input file"; onClicked: inputFileDialog.open() }
                                Button { text: "Remove selected"; Accessible.name: "Remove input file"; onClicked: { if (inputFilesList.currentIndex >= 0) attachmentModel.remove_input_file(inputFilesList.currentIndex) } }
                                Item { Layout.fillWidth: true }
                                Label { text: (attachmentModel.auto_input_files.toString() ? attachmentModel.auto_input_files.toString().split(";").length : 0) + " auto, " + (attachmentModel.input_files.toString() ? attachmentModel.input_files.toString().split(";").length : 0) + " added" }
                            }
                            ListView {
                                id: inputFilesList
                                Layout.fillWidth: true
                                Layout.preferredHeight: 100
                                clip: true
                                Rectangle { anchors.fill: parent; color: palette.base; z: -1 }
                                model: {
                                    var items = []
                                    var auto = attachmentModel.auto_input_files.toString()
                                    if (auto) items = items.concat(auto.split(";").map(function(p) { return {path: p, isAuto: true} }))
                                    var user = attachmentModel.input_files.toString()
                                    if (user) items = items.concat(user.split(";").map(function(p) { return {path: p, isAuto: false} }))
                                    return items
                                }
                                delegate: Text {
                                    text: modelData.path
                                    color: palette.text
                                    font.italic: modelData.isAuto
                                    width: inputFilesList.width
                                    elide: Text.ElideMiddle
                                }
                            }
                        }
                    }

                    GroupBox {
                        title: "Input directories"
                        Layout.fillWidth: true
                        ColumnLayout {
                            anchors.left: parent.left
                            anchors.right: parent.right
                            RowLayout {
                                Button { text: "Add..."; Accessible.name: "Add input directory"; onClicked: inputDirDialog.open() }
                                Button { text: "Remove selected"; Accessible.name: "Remove input directory"; onClicked: { if (inputDirsList.currentIndex >= 0) attachmentModel.remove_input_dir(inputDirsList.currentIndex) } }
                                Item { Layout.fillWidth: true }
                                Label { text: (attachmentModel.auto_input_dirs.toString() ? attachmentModel.auto_input_dirs.toString().split(";").length : 0) + " auto, " + (attachmentModel.input_dirs.toString() ? attachmentModel.input_dirs.toString().split(";").length : 0) + " added" }
                            }
                            ListView {
                                id: inputDirsList
                                Layout.fillWidth: true
                                Layout.preferredHeight: 80
                                clip: true
                                Rectangle { anchors.fill: parent; color: palette.base; z: -1 }
                                model: {
                                    var items = []
                                    var auto = attachmentModel.auto_input_dirs.toString()
                                    if (auto) items = items.concat(auto.split(";").map(function(p) { return {path: p, isAuto: true} }))
                                    var user = attachmentModel.input_dirs.toString()
                                    if (user) items = items.concat(user.split(";").map(function(p) { return {path: p, isAuto: false} }))
                                    return items
                                }
                                delegate: Text {
                                    text: modelData.path
                                    color: palette.text
                                    font.italic: modelData.isAuto
                                    width: inputDirsList.width
                                    elide: Text.ElideMiddle
                                }
                            }
                        }
                    }

                    GroupBox {
                        title: "Output directories"
                        Layout.fillWidth: true
                        ColumnLayout {
                            anchors.left: parent.left
                            anchors.right: parent.right
                            RowLayout {
                                Button { text: "Add..."; Accessible.name: "Add output directory"; onClicked: outputDirDialog.open() }
                                Button { text: "Remove selected"; Accessible.name: "Remove output directory"; onClicked: { if (outputDirsList.currentIndex >= 0) attachmentModel.remove_output_dir(outputDirsList.currentIndex) } }
                                Item { Layout.fillWidth: true }
                                Label { text: (attachmentModel.auto_output_dirs.toString() ? attachmentModel.auto_output_dirs.toString().split(";").length : 0) + " auto, " + (attachmentModel.output_dirs.toString() ? attachmentModel.output_dirs.toString().split(";").length : 0) + " added" }
                            }
                            ListView {
                                id: outputDirsList
                                Layout.fillWidth: true
                                Layout.preferredHeight: 80
                                clip: true
                                Rectangle { anchors.fill: parent; color: palette.base; z: -1 }
                                model: {
                                    var items = []
                                    var auto = attachmentModel.auto_output_dirs.toString()
                                    if (auto) items = items.concat(auto.split(";").map(function(p) { return {path: p, isAuto: true} }))
                                    var user = attachmentModel.output_dirs.toString()
                                    if (user) items = items.concat(user.split(";").map(function(p) { return {path: p, isAuto: false} }))
                                    return items
                                }
                                delegate: Text {
                                    text: modelData.path
                                    color: palette.text
                                    font.italic: modelData.isAuto
                                    width: outputDirsList.width
                                    elide: Text.ElideMiddle
                                }
                            }
                        }
                    }
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
            Button { text: "Export bundle"; Accessible.name: "Export bundle"; onClicked: { submitModel.export_bundle(); exportOkDialog.visible = true } }
            Button {
                text: "Submit"; enabled: submitModel.can_submit && !submitModel.is_submitting
                Accessible.name: "Submit"
                onClicked: {
                    submitModel.submit()
                    progressDialog.visible = true
                }
            }
            Button { text: "Cancel"; Accessible.name: "Cancel"; onClicked: root.close() }
        }
    }

    ApplicationWindow {
        id: exportOkDialog
        width: 350
        height: 120
        title: "Export complete"
        modality: Qt.ApplicationModal
        flags: Qt.Dialog | Qt.WindowTitleHint
        visible: false
        ColumnLayout {
            anchors.fill: parent
            anchors.margins: 16
            Label { text: "Saved the submission as a job bundle."; Layout.fillWidth: true }
            Item { Layout.fillHeight: true }
            RowLayout {
                Layout.fillWidth: true
                Item { Layout.fillWidth: true }
                Button { text: "OK"; Accessible.name: "OK"; onClicked: { exportOkDialog.close(); root.close() } }
            }
        }
    }

    ApplicationWindow {
        id: progressDialog
        width: 500
        height: 400
        title: "AWS Deadline Cloud submission"
        modality: Qt.ApplicationModal
        flags: Qt.Dialog | Qt.WindowTitleHint | Qt.WindowCloseButtonHint
        visible: false

        ColumnLayout {
            anchors.fill: parent
            anchors.margins: 16
            spacing: 12

            Label {
                text: submitModel.submission_error !== ""
                      ? "Submission error"
                      : submitModel.job_id_result !== ""
                        ? "Submission complete"
                        : submitModel.submission_complete
                          ? "Submission canceled"
                          : submitModel.is_submitting
                            ? "Preparing files..."
                            : ""
                font.bold: true
                font.pointSize: 13
                Accessible.name: submitModel.submission_error !== ""
                      ? "Submission error"
                      : submitModel.job_id_result !== ""
                        ? "Submission complete"
                        : submitModel.submission_complete
                          ? "Submission canceled"
                          : submitModel.is_submitting
                            ? "Preparing files..."
                            : ""
            }

            GroupBox {
                title: "Hashing Attachments"
                Layout.fillWidth: true
                ColumnLayout {
                    anchors.left: parent.left
                    anchors.right: parent.right
                    ProgressBar {
                        Layout.fillWidth: true
                        from: 0; to: 100
                        value: submitModel.hashing_progress
                    }
                    Label { text: submitModel.hashing_message; elide: Text.ElideRight; Layout.fillWidth: true }
                }
            }

            GroupBox {
                title: "Uploading Attachments"
                Layout.fillWidth: true
                ColumnLayout {
                    anchors.left: parent.left
                    anchors.right: parent.right
                    ProgressBar {
                        Layout.fillWidth: true
                        from: 0; to: 100
                        value: submitModel.upload_progress
                    }
                    Label { text: submitModel.upload_message; elide: Text.ElideRight; Layout.fillWidth: true }
                }
            }

            GroupBox {
                title: "Log"
                Layout.fillWidth: true
                Layout.fillHeight: true
                ScrollView {
                    anchors.fill: parent
                    TextArea {
                        readOnly: true
                        text: submitModel.log_text
                        wrapMode: TextEdit.Wrap
                        font.family: Qt.platform.os === "osx" ? "Menlo" : "Monospace"
                        font.pointSize: 10
                    }
                }
            }

            Label {
                visible: submitModel.submission_error !== ""
                text: submitModel.submission_error
                color: "red"
                wrapMode: Text.Wrap
                Layout.fillWidth: true
            }

            Label {
                visible: submitModel.job_id_result !== ""
                text: "Job ID: " + submitModel.job_id_result
                font.bold: true
                Layout.fillWidth: true
            }

            RowLayout {
                Layout.fillWidth: true
                Item { Layout.fillWidth: true }
                Button {
                    text: submitModel.job_id_result !== "" ? "Ok"
                          : submitModel.submission_complete ? "Close"
                          : "Cancel"
                    Accessible.name: submitModel.job_id_result !== "" ? "Ok"
                          : submitModel.submission_complete ? "Close"
                          : "Cancel"
                    onClicked: {
                        if (!submitModel.submission_complete) {
                            submitModel.cancel_submission()
                        }
                        progressDialog.close()
                        if (submitModel.job_id_result !== "") {
                            root.close()
                        }
                    }
                }
            }
        }
    }

    Component.onCompleted: {
        submitModel.initialize("")
        attachmentModel.initialize(submitModel.job_bundle_dir)
        resourceModel.set_configured_ids(submitModel.farm_display, submitModel.queue_display, "")
        var profile = submitModel.aws_profile
        resourceModel.set_profile(profile)
        authModel.set_profile(profile)
    }
}
