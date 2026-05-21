import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import com.amazon.deadline.gui

ApplicationWindow {
    id: root
    width: 500
    height: 400
    visible: true
    title: qsTr("AWS Deadline Cloud - Workstation Configuration")

    ConfigModel {
        id: configModel
        Component.onCompleted: configModel.loadSettings()
    }

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 20
        spacing: 15

        GroupBox {
            title: qsTr("Global Settings")
            Layout.fillWidth: true

            GridLayout {
                columns: 2
                anchors.fill: parent

                Label { text: qsTr("AWS Profile:") }
                TextField {
                    id: profileField
                    text: configModel.awsProfile
                    Layout.fillWidth: true
                    onEditingFinished: configModel.awsProfile = text
                }

                Label { text: qsTr("Farm ID:") }
                TextField {
                    id: farmField
                    text: configModel.farmId
                    Layout.fillWidth: true
                    onEditingFinished: configModel.farmId = text
                }

                Label { text: qsTr("Queue ID:") }
                TextField {
                    id: queueField
                    text: configModel.queueId
                    Layout.fillWidth: true
                    onEditingFinished: configModel.queueId = text
                }
            }
        }

        Label {
            text: configModel.statusMessage
            font.italic: true
        }

        Item { Layout.fillHeight: true }

        RowLayout {
            Layout.alignment: Qt.AlignRight
            spacing: 10

            Button {
                text: qsTr("Apply")
                onClicked: configModel.applySettings()
            }
            Button {
                text: qsTr("Close")
                onClicked: root.close()
            }
        }
    }
}
