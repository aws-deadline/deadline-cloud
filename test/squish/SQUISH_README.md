# Squish GUI Submitter Tests

Squish requires a license. Currently, you may either have your own Squish license or you may file a [pull request](https://help.github.com/articles/creating-a-pull-request/) to the Deadline Cloud team to run or add any Squish automated tests against any changes to be committed. Please perform any necessary manual tests prior to submitting any changes, in addition to making sure at least a minimal render job test passes. If you have a Squish license, please follow our basic guide below to get set up. 

## Set Up/Install Deadline Cloud Client

If you haven't done so already, install Deadline Cloud with the GUI dependencies:
```sh
$ pip install "deadline[gui]"
```
Verify that it was successfully installed by running `deadline config gui` and allow the Deadline Settings dialog to load. 

## Set Up/Install Squish Framework

Install Squish on Linux, Windows, or macOS. You will need the Squish for QT editions. Do select the correct version of QT based on the version of PySide6 that is being used with Deadline Cloud Client on your machine. Currently, set up instructions have been validated on Linux, with QT 6.6. Once installed, launch Squish IDE on your machine and follow the remaining instructions below.

## Configure Squish Environment 

Register the Deadline Client executable in the AUT Settings in the Squish IDE. This is typically done in Squish by going to 'Edit' -> 'Server Settings' and registering under 'Mapped AUTs'. On Linux, the Deadline Client executable may live in this file path: `~/.local/bin/deadline`

In order to launch Deadline Cloud Client in Squish, you will need to set an environment variable pointing to the PySide6 QT libraries which Deadline Client uses. For example, on Linux, the PySide6 QT libraries may live in `/home/<user>/.local/lib/python3.9/site-packages/PySide6/Qt/lib`, which needs to be set to the `LD_LIBRARY_PATH` environment variable. Generally, this environment variable setting can be set prior to launching your environment, the `envvars` file, or under AUT Environment in the Settings page of the Squish IDE.

Example:
```sh
LD_LIBRARY_PATH=/home/<user>/.local/lib/python3.9/site-packages/PySide6/Qt/lib
```

Once Deadline is registered and the environment variable is configured, you may test out that the Deadline AUT launches successfully by going to 'Run' -> 'Launch AUT' in the Squish IDE. 

## Deadline Cloud Resources Needed for Running Tests - config.py file

Once the Deadline AUT is set up, the following Deadline Cloud resources are needed in order to run `tst_verify_settings_dialogue` test suite:

- AWS default profile
- A farm named "Deadline Cloud Squish Farm"
- A queue named "Squish Automation Queue"
- A storage profile named "Squish Storage Profile"

Note: The names of the above resources are in `./suite_deadline_gui/shared/config.py` file. 

Additionally, in config.py file, `simple_ui_with_ja` and `simple_ui_no_ja` file paths need to point to where those files live on your system when running `tst_verify_gui_submitter_bundles` tests locally.

## Running Squish Tests Locally

Squish tests can be run from the IDE or the command line. 
- In the IDE, hit the green 'Play' button located in the Test Suites left-hand column. 
- To run tests using the command line:
    - First, `cd ~/<squish_install_folder>/bin`
    - Example command to trigger all existing tests: 
    `./squishrunner --testsuite /home/<user>/deadline-cloud/test/squish/suite_deadline_gui --local`
    - Example command to trigger only `tst_verify_gui_submitter_bundles` test suite: 
    `./squishrunner --testsuite /home/<user>/deadline-cloud/test/squish/suite_deadline_gui --testcase tst_verify_gui_submitter_bundles --local`

## Adding Tests

Squish element locators:

* Squish automatically identifies and creates all locators for you in a file called `names.py`.
* As the names of these locators do not necessarily have easily identifiable labels/names, all locators have been renamed and organized in their own page. Please allow `names.py` file to be updated as new locators are identified, and then copy/add new locators with easily identifiable labels/names into the appropriate files (ie: `workstation_config_locators.py`, `loginout_locators.py`, etc).

## A Final Word on Testing

To ensure quality and reliability of any changes made to Deadline Client, please run any necessary automated and manual tests prior to submitting changes.

Happy Squish testing! Any further questions about Squish API can be found in [Squish official documentation](https://doc.qt.io/squish/).