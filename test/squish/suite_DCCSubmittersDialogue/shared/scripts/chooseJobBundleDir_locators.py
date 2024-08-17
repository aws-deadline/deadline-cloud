# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# -*- coding: utf-8 -*-

# choose job history directory dialogue (deadline bundle gui-submit --browse)
qFileDialog_QFileDialog = {"name": "QFileDialog", "type": "QFileDialog", "visible": 1}
qFileDialog_splitter_QSplitter = {"name": "splitter", "type": "QSplitter", "visible": 1, "window": qFileDialog_QFileDialog}
splitter_sidebar_QSidebar = {"container": qFileDialog_splitter_QSplitter, "name": "sidebar", "type": "QSidebar", "visible": 1}
splitter_frame_QFrame = {"container": qFileDialog_splitter_QSplitter, "name": "frame", "type": "QFrame", "visible": 1}
frame_stackedWidget_QStackedWidget = {"container": splitter_frame_QFrame, "name": "stackedWidget", "type": "QStackedWidget", "visible": 1}
stackedWidget_treeView_QTreeView = {"container": frame_stackedWidget_QStackedWidget, "name": "treeView", "type": "QTreeView", "visible": 1}
treeView_QScrollBar = {"container": stackedWidget_treeView_QTreeView, "occurrence": 2, "type": "QScrollBar", "unnamed": 1, "visible": 1}
qFileDialog_Choose_QPushButton = {"text": "Choose", "type": "QPushButton", "unnamed": 1, "visible": 1, "window": qFileDialog_QFileDialog}
submit_to_AWS_Deadline_Cloud_SubmitJobToDeadlineDialog = {"type": "SubmitJobToDeadlineDialog", "unnamed": 1, "visible": 1, "windowTitle": "Submit to AWS Deadline Cloud"}
qFileDialog_fileNameLabel_QLabel = {"name": "fileNameLabel", "type": "QLabel", "visible": 1, "window": qFileDialog_QFileDialog}
fileNameEdit_QLineEdit = {"buddy": qFileDialog_fileNameLabel_QLabel, "name": "fileNameEdit", "type": "QLineEdit", "visible": 1}
qFileDialog_qt_edit_menu_QMenu = {"name": "qt_edit_menu", "type": "QMenu", "visible": 1, "window": qFileDialog_QFileDialog}
o_QListView = {"type": "QListView", "unnamed": 1, "visible": 1}
