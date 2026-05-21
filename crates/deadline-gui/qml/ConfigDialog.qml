import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import com.amazon.deadline.gui

ApplicationWindow {
    id: root
    minimumWidth: 550
    minimumHeight: 600
    width: 600
    height: 750
    visible: true
    title: "AWS Deadline Cloud workstation configuration"

    ConfigModel {
        id: configModel
        Component.onCompleted: configModel.loadSettings()
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

                    ColumnLayout {
                        anchors.fill: parent
                        spacing: 8

                        Label { text: "AWS Profile" }
                        ComboBox {
                            id: profileBox
                            Layout.fillWidth: true
                            model: configModel.awsProfileNames !== "" ? configModel.awsProfileNames.split(";") : []
                            currentIndex: model ? model.indexOf(configModel.awsProfile) : -1
                            onCurrentTextChanged: {
                                if (currentText !== "") {
                                    configModel.awsProfile = currentText
                                    configModel.notifyChanged()
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

                    ColumnLayout {
                        anchors.fill: parent
                        spacing: 8

                        Label { text: "Job history directory" }
                        TextField {
                            Layout.fillWidth: true
                            text: configModel.jobHistoryDir
                            onEditingFinished: {
                                configModel.jobHistoryDir = text
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Job history directory"
                        }

                        Label { text: "Default farm" }
                        TextField {
                            Layout.fillWidth: true
                            text: configModel.farmId
                            readOnly: true
                            placeholderText: "No farm selected"
                            Accessible.name: "Default farm"
                        }
                    }
                }

                // ─── Farm settings ─────────────────────────────────
                GroupBox {
                    title: "Farm settings"
                    Layout.fillWidth: true
                    Accessible.name: "Farm settings"
                    Accessible.role: Accessible.Grouping

                    ColumnLayout {
                        anchors.fill: parent
                        spacing: 8

                        Label { text: "Default queue" }
                        TextField {
                            Layout.fillWidth: true
                            text: configModel.queueId
                            readOnly: true
                            placeholderText: "No queue selected"
                            Accessible.name: "Default queue"
                        }

                        Label { text: "Default storage profile" }
                        TextField {
                            Layout.fillWidth: true
                            text: configModel.storageProfileId
                            readOnly: true
                            placeholderText: "No storage profile selected"
                            Accessible.name: "Default storage profile"
                        }

                        Label { text: "Job attachments filesystem options" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["COPIED", "VIRTUAL"]
                            currentIndex: model.indexOf(configModel.jobAttachmentsFilesystem)
                            onCurrentTextChanged: {
                                configModel.jobAttachmentsFilesystem = currentText
                                configModel.notifyChanged()
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
                            checked: configModel.autoAccept
                            onCheckedChanged: {
                                configModel.autoAccept = checked
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Auto accept prompt defaults"
                        }

                        Label { text: "Telemetry opt out" }
                        CheckBox {
                            checked: configModel.telemetryOptOut
                            onCheckedChanged: {
                                configModel.telemetryOptOut = checked
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Telemetry opt out"
                        }

                        Label { text: "Always check S3 job attachments" }
                        CheckBox {
                            checked: configModel.forceS3Check
                            onCheckedChanged: {
                                configModel.forceS3Check = checked
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Always check S3 job attachments"
                        }

                        Label { text: "Show submitter update notifications" }
                        CheckBox {
                            checked: configModel.submitterUpdateNotification
                            onCheckedChanged: {
                                configModel.submitterUpdateNotification = checked
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Show submitter update notifications"
                        }

                        Label { text: "Conflict resolution option" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["NOT_SELECTED", "CREATE_COPY", "SKIP", "OVERWRITE"]
                            currentIndex: model.indexOf(configModel.conflictResolution)
                            onCurrentTextChanged: {
                                configModel.conflictResolution = currentText
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Conflict resolution option"
                        }

                        Label { text: "Current logging level" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["ERROR", "WARNING", "INFO", "DEBUG"]
                            currentIndex: model.indexOf(configModel.logLevel)
                            onCurrentTextChanged: {
                                configModel.logLevel = currentText
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Current logging level"
                        }

                        Label { text: "Language" }
                        ComboBox {
                            Layout.fillWidth: true
                            model: ["", "de_DE", "en_US", "es_ES", "fr_FR", "id_ID", "it_IT", "ja_JP", "ko_KR", "pt_BR", "tr_TR", "zh_CN", "zh_TW"]
                            currentIndex: model.indexOf(configModel.locale)
                            onCurrentTextChanged: {
                                configModel.locale = currentText
                                configModel.notifyChanged()
                            }
                            Accessible.name: "Language"
                        }
                    }
                }
            }
        }

        // ─── Auth status bar ───────────────────────────────────────
        Rectangle {
            Layout.fillWidth: true
            height: 32
            color: "#f0f0f0"
            radius: 4

            Label {
                anchors.centerIn: parent
                text: configModel.awsProfile !== "" ? "Profile: " + configModel.awsProfile : ""
                Accessible.name: configModel.awsProfile !== "" ? "Profile: " + configModel.awsProfile : ""
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
                    configModel.applySettings()
                    root.close()
                }
            }
            Button {
                text: "Cancel"
                Accessible.name: "Cancel"
                onClicked: {
                    configModel.revertSettings()
                    root.close()
                }
            }
            Button {
                text: "Apply"
                Accessible.name: "Apply"
                enabled: configModel.hasChanges
                onClicked: configModel.applySettings()
            }
        }
    }
}
