import QtQuick
import QtQuick.Controls
import QtQuick.Layouts

Window {
    id: progressRoot
    width: 500
    height: 400
    title: "Submitting to AWS Deadline Cloud"
    modality: Qt.ApplicationModal
    flags: Qt.Dialog | Qt.WindowTitleHint | Qt.WindowCloseButtonHint

    property var submitModel

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 16
        spacing: 12

        Label {
            text: submitModel.submission_error !== ""
                  ? "Submission failed"
                  : submitModel.job_id_result !== ""
                    ? "Submission complete"
                    : "Submitting job..."
            font.bold: true
            font.pointSize: 13
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
                    font.family: "monospace"
                    font.pointSize: 10
                }
            }
        }

        // Error display
        Label {
            visible: submitModel.submission_error !== ""
            text: submitModel.submission_error
            color: "red"
            wrapMode: Text.Wrap
            Layout.fillWidth: true
        }

        // Success display
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
                text: submitModel.submission_complete ? "Close" : "Cancel"
                onClicked: {
                    if (!submitModel.submission_complete) {
                        submitModel.cancel_submission()
                    }
                    progressRoot.close()
                }
            }
        }
    }
}
