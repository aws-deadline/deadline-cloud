import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import com.amazon.deadline.gui

ApplicationWindow {
    id: root
    minimumWidth: 550
    minimumHeight: 600
    width: 650
    height: 800
    visible: true
    title: "AWS Deadline Cloud workstation configuration"

    ConfigModel {
        id: configModel
        Component.onCompleted: configModel.load_settings()
    }

    ResourceModel {
        id: resourceModel
    }

    AuthModel {
        id: authModel
        Component.onCompleted: authModel.start_watching()
    }

    Connections {
        target: authModel
        function onApi_availableChanged() {
            if (authModel.api_available) {
                resourceModel.refresh_farms()
            }
        }
    }

    Connections {
        target: configModel
        function onAws_profileChanged() {
            var p = configModel.aws_profile
            resourceModel.set_configured_ids(configModel.farm_id, configModel.queue_id, configModel.storage_profile_id)
            authModel.set_profile(p)
            resourceModel.set_profile(p)
        }
    }

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 16
        spacing: 8

        ScrollView {
            Layout.fillWidth: true
            Layout.fillHeight: true
            clip: true

            ColumnLayout {
                width: parent.parent.width
                spacing: 16

                // ─── Global settings ───────────────────────────────
                GroupBox {
                    title: "Global settings"
                    Layout.fillWidth: true
                    Accessible.name: "Global settings"
                    Accessible.role: Accessible.Grouping

                    GridLayout {
                        columns: 2
                        anchors.left: parent.left
                        anchors.right: parent.right
                        columnSpacing: 12
                        rowSpacing: 8

                        Label { text: "AWS Profile" }
                        ComboBox {
                            id: profileBox
                            Layout.fillWidth: true
                            model: configModel.aws_profile_names !== "" ? configModel.aws_profile_names.split(";") : []
                            currentIndex: model ? model.indexOf(configModel.aws_profile) : -1
                            onActivated: function(index) {
                                if (currentText !== "") {
                                    configModel.aws_profile = currentText
                                    configModel.notify_changed()
                                }
                            }
                            Accessible.name: "AWS Profile"
                        }
                    }
                }

                // ─── Profile settings ──────────────────────────────
                GroupBox {
                    title: "Profile settings"
                    Layout.fillWidth: true
                    Accessible.name: "Profile settings"
                    Accessible.role: Accessible.Grouping

                    GridLayout {
                        columns: 2
                        anchors.left: parent.left
                        anchors.right: parent.right
                        columnSpacing: 12
                        rowSpacing: 8

                        Label { text: "Job history directory" }
                        TextField {
                            Layout.fillWidth: true
                            text: configModel.job_history_dir
                            onEditingFinished: {
                                configModel.job_history_dir = text
                                configModel.notify_changed()
                            }
                            Accessible.name: "Job history directory"
                        }

                        Label { text: "Default farm" }
                        RowLayout {
                            Layout.fillWidth: true
                            ComboBox {
                                id: farmBox
                                Layout.fillWidth: true
                                model: resourceModel.farm_names !== "" ? resourceModel.farm_names.split(";") : []
                                enabled: !resourceModel.farms_loading && model.length > 0
                                currentIndex: resourceModel.selected_farm_index
                                onActivated: function(index) {
                                    resourceModel.select_farm(index)
                                    var ids = resourceModel.farm_ids.split(";")
                                    if (index >= 0 && index < ids.length) {
                                        configModel.farm_id = ids[index]
                                        configModel.notify_changed()
                                    }
                                }
                                Accessible.name: "Default farm"
                                displayText: resourceModel.farms_loading ? "<refreshing>" : (model.length > 0 ? currentText : configModel.farm_id)
                            }
                            Button {
                                text: "↻"
                                implicitWidth: 30
                                onClicked: resourceModel.refresh_farms()
                                Accessible.name: "Refresh farms"
                            }
                        }
                    }
                }

                // ─── Farm settings ─────────────────────────────────
                GroupBox {
                    title: "Farm settings"
                    Layout.fillWidth: true
                    Accessible.name: "Farm settings"
                    Accessible.role: Accessible.Grouping

                    GridLayout {
                        columns: 2
                        anchors.left: parent.left
                        anchors.right: parent.right
                        columnSpacing: 12
                        rowSpacing: 8

                        Label { text: "Default queue" }
                        RowLayout {
                            Layout.fillWidth: true
                            ComboBox {
                                id: queueBox
                                Layout.fillWidth: true
                                model: resourceModel.queue_names !== "" ? resourceModel.queue_names.split(";") : []
                                enabled: !resourceModel.queues_loading && model.length > 0
                                currentIndex: resourceModel.selected_queue_index
                                onActivated: function(index) {
                                    resourceModel.select_queue(index)
                                    var ids = resourceModel.queue_ids.split(";")
                                    if (index >= 0 && index < ids.length) {
                                        configModel.queue_id = ids[index]
                                        configModel.notify_changed()
                                    }
                                }
                                Accessible.name: "Default queue"
                                displayText: resourceModel.queues_loading ? "<refreshing>" : (model.length > 0 ? currentText : configModel.queue_id)
                            }
                            Button {
                                text: "↻"
                                implicitWidth: 30
                                onClicked: resourceModel.refresh_queues()
                                Accessible.name: "Refresh queues"
                            }
                        }

                        Label { text: "Default storage profile" }
                        RowLayout {
                            Layout.fillWidth: true
                            ComboBox {
                                id: storageProfileBox
                                Layout.fillWidth: true
                                model: resourceModel.storage_profile_names !== "" ? resourceModel.storage_profile_names.split(";") : []
                                enabled: !resourceModel.storage_profiles_loading && model.length > 0
                                currentIndex: resourceModel.selected_storage_profile_index
                                onActivated: function(index) {
                                    var ids = resourceModel.storage_profile_ids.split(";")
                                    if (index >= 0 && index < ids.length) {
                                        configModel.storage_profile_id = ids[index]
                                        configModel.notify_changed()
                                    }
                                }
                                Accessible.name: "Default storage profile"
                                displayText: resourceModel.storage_profiles_loading ? "<refreshing>" : (model.length > 0 ? currentText : configModel.storage_profile_id)
                            }
                            Button {
                                text: "↻"
                                implicitWidth: 30
                                onClicked: resourceModel.refresh_storage_profiles()
                                Accessible.name: "Refresh storage profiles"
                            }
                        }

                        Label { text: "Job attachments filesystem options" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["COPIED", "VIRTUAL"]
                            currentIndex: model.indexOf(configModel.job_attachments_filesystem)
                            onActivated: {
                                configModel.job_attachments_filesystem = currentText
                                configModel.notify_changed()
                            }
                            Accessible.name: "Job attachments filesystem options"
                        }
                    }
                }

                // ─── General settings ──────────────────────────────
                GroupBox {
                    title: "General settings"
                    Layout.fillWidth: true
                    Accessible.name: "General settings"
                    Accessible.role: Accessible.Grouping

                    GridLayout {
                        columns: 2
                        anchors.left: parent.left
                        anchors.right: parent.right
                        columnSpacing: 12
                        rowSpacing: 8

                        Label { text: "Auto accept prompt defaults" }
                        CheckBox {
                            checked: configModel.auto_accept
                            onToggled: {
                                configModel.auto_accept = checked
                                configModel.notify_changed()
                            }
                            Accessible.name: "Auto accept prompt defaults"
                        }

                        Label { text: "Telemetry opt out" }
                        CheckBox {
                            checked: configModel.telemetry_opt_out
                            onToggled: {
                                configModel.telemetry_opt_out = checked
                                configModel.notify_changed()
                            }
                            Accessible.name: "Telemetry opt out"
                        }

                        Label { text: "Always check S3 job attachments" }
                        CheckBox {
                            checked: configModel.force_s3_check
                            onToggled: {
                                configModel.force_s3_check = checked
                                configModel.notify_changed()
                            }
                            Accessible.name: "Always check S3 job attachments"
                        }

                        Label { text: "Show submitter update notifications" }
                        CheckBox {
                            checked: configModel.submitter_update_notification
                            onToggled: {
                                configModel.submitter_update_notification = checked
                                configModel.notify_changed()
                            }
                            Accessible.name: "Show submitter update notifications"
                        }

                        Label { text: "Conflict resolution option" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["NOT_SELECTED", "CREATE_COPY", "SKIP", "OVERWRITE"]
                            currentIndex: {
                                var idx = model.indexOf(configModel.conflict_resolution)
                                return idx >= 0 ? idx : 0
                            }
                            onActivated: {
                                configModel.conflict_resolution = currentText
                                configModel.notify_changed()
                            }
                            Accessible.name: "Conflict resolution option"
                        }

                        Label { text: "Current logging level" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["ERROR", "WARNING", "INFO", "DEBUG"]
                            currentIndex: {
                                var idx = model.indexOf(configModel.log_level)
                                return idx >= 0 ? idx : 2
                            }
                            onActivated: {
                                configModel.log_level = currentText
                                configModel.notify_changed()
                            }
                            Accessible.name: "Current logging level"
                        }

                        Label { text: "Language" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["", "de_DE", "en_US", "es_ES", "fr_FR", "id_ID", "it_IT", "ja_JP", "ko_KR", "pt_BR", "tr_TR", "zh_CN", "zh_TW"]
                            currentIndex: {
                                var idx = model.indexOf(configModel.locale)
                                return idx >= 0 ? idx : 0
                            }
                            onActivated: {
                                configModel.locale = currentText
                                configModel.notify_changed()
                            }
                            Accessible.name: "Language"
                        }
                    }
                }
            }
        }

        // ─── Auth status bar ───────────────────────────────────────
        GroupBox {
            Layout.fillWidth: true
            Accessible.name: "Authentication status"
            Accessible.role: Accessible.Grouping

            RowLayout {
                anchors.fill: parent
                spacing: 8

                Label {
                    text: authModel.is_refreshing ? "⟳" : (authModel.api_available ? "✓" : "⚠")
                    font.pixelSize: 16
                    Accessible.name: authModel.api_available ? "Authenticated" : "Authentication warning"
                }

                Label {
                    text: authModel.status_text
                    Layout.fillWidth: true
                    elide: Text.ElideRight
                    Accessible.name: authModel.status_text
                }

                Button {
                    text: "Log in"
                    visible: authModel.show_login
                    onClicked: authModel.login()
                    Accessible.name: "Log in"
                }

                Button {
                    text: "Log out"
                    visible: authModel.show_logout
                    onClicked: authModel.logout()
                    Accessible.name: "Log out"
                }

                Button {
                    text: "More info"
                    visible: authModel.show_more_info
                    Accessible.name: "More info"
                }
            }
        }

        // ─── Buttons ───────────────────────────────────────────────
        RowLayout {
            Layout.fillWidth: true
            Item { Layout.fillWidth: true }
            spacing: 8

            Button {
                text: "Ok"
                Accessible.name: "Ok"
                onClicked: {
                    configModel.apply_settings()
                    root.close()
                }
            }
            Button {
                text: "Cancel"
                Accessible.name: "Cancel"
                onClicked: {
                    configModel.revert_settings()
                    root.close()
                }
            }
            Button {
                text: "Apply"
                Accessible.name: "Apply"
                enabled: configModel.has_changes
                onClicked: configModel.apply_settings()
            }
        }
    }
}
