# Squish GUI Submitter Tests

Squish requires a license. Currently, you may either have your own Squish license or you may file a [pull request](https://help.github.com/articles/creating-a-pull-request/) to the Deadline Cloud team to run or add any Squish automated tests against any changes to be committed. Please perform any necessary manual tests prior to submitting any changes, in addition to making sure at least a minimal render job test passes. If you have a Squish license, please follow our basic guide below to get set up. 

## Install Deadline Cloud Client

If you haven't done so already, install Deadline Cloud with the GUI dependencies:
```sh
$ pip install "deadline[gui]"
```
Verify that it was successfully installed by running `deadline config gui` and allow the Deadline Settings dialog to load. 

## Install Squish Framework

Install the latest version of Squish for Qt on Linux, Windows, or macOS. (As of this writing, these instructions have been validated on Squish for Qt 8.1.0, Qt 6.8.) If you are using any other version, be sure to select the correct version of Qt based on the version of PySide6 that is being used with Deadline Cloud Client on your machine. Currently, these set up instructions have been validated on Linux, Windows, and macOS using Qt 6.8.2.1. Once installed, launch Squish IDE on your machine and follow the remaining instructions below.

## Configure Squish Environment 

To get started with writing tests, register the Deadline Client executable in AUT Settings in the Squish IDE. This is typically done by going to 'Edit' -> 'Server Settings' and registering under 'Mapped AUTs' (in Squish IDE). 

Specific steps for setting up on Linux, Windows, and macOS are below. The steps also include Squish OS-specific environment variables which should point to where the PySide6 Qt libraries live on your system. The environment variables can be added in the AUT Environment table in the Test Suite Settings page 
(in Squish IDE) or in the `suite_deadline_gui/envvars` file. 

### For Linux: 

On Linux, the Deadline Client executable (`deadline`) needs to be set as the main AUT. It may live in this file path: `~/.local/bin/deadline`

#### Squish Environment Variables: 

```sh
LD_LIBRARY_PATH=/home/<user>/.local/lib/python3.9/site-packages/PySide6/Qt/lib
```
- To test that Deadline Client is registered and environment variable is configured successfully, navigate to the Test Suite Settings page (in Squish IDE) and add `config gui` to `Arguments`. Then, launch Deadline AUT by going to 'Run' -> 'Launch AUT' in the Squish IDE. If Deadline AUT launches successfully with no issues, you may begin writing tests using Squish IDE. 

### For Windows:

On Windows, `python.exe` needs to be set as the main AUT. It may live in this file path: `C:/Users/<user>/AppData/Local/Programs/Python/Python310`

#### Squish Environment Variables: 

```sh
PATH=C:\Users\<user>\AppData\Local\Programs\Python\Python310\Lib\site-packages\PySide6\

SQUISH_NO_CAPTURE_OUTPUT=1
```

- To test that Deadline Client is registered and environment variables are configured successfully, navigate to the Test Suite Settings page (in Squish IDE) and add `C:\Users\<user>\AppData\Local\Programs\Python\Python310\Scripts\deadline.exe config gui` to `Arguments`. Then, launch Deadline AUT by going to 'Run' -> 'Launch AUT' in the Squish IDE. If Deadline AUT launches successfully with no issues, you may begin writing tests using Squish IDE.

#### Additional Windows Environment Variable: 

The following environment variable needs to be set in the environment where Squish tests are invoked from. It is used in launching the AUT on Windows during the automated tests, and needs to point to where `deadline.exe` lives on your system. If not set, it will use a default. The default can be found on lines 11 and 13 in the `/shared/config.py` file.

```sh
WINDOWS_DEADLINE_PATH=C:\Users\<user>\AppData\Local\Programs\Python\Python310\Scripts\deadline.exe
```

### For macOS:

On macOS, the Deadline Client executable (`deadline`) needs to be set as the main AUT. It may live in this file path: `/Users/<user>/Library/Python/3.9/bin/deadline.exe`

#### Squish Environment Variables: 
```sh
DYLD_LIBRARY_PATH=/Users/<user>/Library/Python/3.9/lib/python/site-packages/PySide6/Qt/lib
```

- To test that Deadline Client is registered and environment variable is configured successfully, navigate to the Test Suite Settings page (in Squish IDE) and add `config gui` to Arguments. Then, launch Deadline AUT by going to 'Run' -> 'Launch AUT' in the Squish IDE. If Deadline AUT launches successfully with no issues, you may begin writing tests using Squish IDE.

## Deadline Cloud Resources Needed for Running Tests

The following Deadline Cloud resources are needed in order to run `tst_verify_settings_dialogue` test suite:

- An AWS default profile (used for authentication)
- A farm named "Deadline Cloud Squish Farm"
- A queue named "Squish Automation Queue"
- Three storage profiles named "Linux Storage Profile", "Windows Storage Profile", and "macOS Storage Profile"

## Running Squish Tests Locally

Squish tests can be run from the IDE or the command line. Prior to running tests, you will want to authenticate using the AWS default profile so the Deadline Cloud resources can be accessed. Additionally, remember that on Windows, the `WINDOWS_DEADLINE_PATH` environment variable should be set to where `deadline.exe` lives on your system; otherwise, it will use a default.
- In the IDE, hit the green 'Play' button located in the Test Suites left-hand column. 
- To run tests using the command line:
    - First, `cd ~/<squish_install_folder>/bin`
    - Example command to trigger all existing tests on Linux: 
    `./squishrunner --testsuite /home/<user>/deadline-cloud/test/squish/suite_deadline_gui --local`
    - Example command to trigger only `tst_verify_gui_submitter_bundles` test suite (on Linux): 
    `./squishrunner --testsuite /home/<user>/deadline-cloud/test/squish/suite_deadline_gui --testcase tst_verify_gui_submitter_bundles --local`

## Adding Tests

Squish element locators:

* Squish automatically identifies and creates all locators for you in a file called `names.py`.
* As the names of these locators do not necessarily have easily identifiable labels/names, all locators have been renamed and organized in their own page. Please allow `names.py` file to be updated as new locators are identified, and then copy/add new locators with easily identifiable labels/names into the appropriate files (ie: `workstation_config_locators.py`, `loginout_locators.py`, etc).

## A Final Word on Testing

To ensure quality and reliability of any changes made to Deadline Client, please run any necessary automated and manual tests prior to submitting changes.

Happy Squish testing! Any further questions about Squish API can be found in [Squish official documentation](https://doc.qt.io/squish/).