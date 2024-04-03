# Development documentation

This documentation provides guidance on developer workflows for working with the code in this repository.

## Code organization

This repository is split up into two main modules:
1. `src/client`
2. `src/job_attachments`

The `src/client` organization is laid out below.

For more information on job attachments, see [here](src/deadline/job_attachments/README.md).

### `src/client/api`

This submodule contains utilities to call boto3 in a standardized way
using an aws profile configured for AWS Deadline Cloud, helpers for working with the
AWS Deadline Cloud monitor login/logout, and objects representing AWS Deadline Cloud
resources.

### `src/client/cli`

This submodule contains entry points for the CLI applications provided
by the library.

### `src/client/config`

This submodule contains an interface to the machine-specific AWS Deadline Cloud
configuration, specifically settings stored in `~/.deadline/*`

### `src/client/ui`

This submodule contains Qt GUIs, based on PySide(2/6), for common controls
and widgets used in interactive submitters, and to display the status
of various AWS Deadline Cloud resources.

### `src/client/job_bundle`

This submodule contains code related to the history of job submissions
performed on the workstation. Its initial functionality is to create
job bundle directories in a standardized manner.

# Build / Test / Release

## Build the package.
```
hatch build
```

## Run tests
```
hatch run test
```

## Run integration tests
```
hatch run integ:test
```

## Run linting
```
hatch run lint
```

## Run formating
```
hatch run fmt
```

## Run tests for all supported Python versions.
```
hatch run all:test
```

## Qt and Calling AWS (including AWS Deadline Cloud) APIs

> TL;DR Never call an AWS API from the main Qt event loop. Always run it in a separate thread,
> and use a Signal/Slot to send the result back to GUI widget that needs an update. The code
> in the separate thread should watch a boolean flag indicating whether to abandon its work.

AWS APIs, while often quick, can be very slow sometimes. When calling to a distant region,
they can consistently have very high latency.

In Qt, event handling happens in the process's main thread that is running an event
loop. If code performs a slow operation, such as calling an AWS API, that blocks all
interactivity with the GUI.

We can maintain GUI interactivity by running these slow operations in a separate thread.
If the separate thread, however, directly modifies the GUI, this can produce crashes or
undefined behavior. Therefore, the only way the results of these operations should be consumed
is by emitting a Qt Signal from the thread, and consuming it in the Widget.

Another detail is that threads need to finish running before the process can exit. If an
operation in a thread continues indefinitely, this will block program exit, so it should watch
for a signal from the application.

If interacting with the GUI can start multiple background threads, you should also track which
is the latest, so the code only applies the result of the newest operation.

See `deadline_config_dialog.py` for some examples that do all of the above. Here's some
code that was edited to show how it fits together:

```python
class MyCustomWidget(QWidget):
   # Signals for the widget to receive from the thread
   background_exception = Signal(str, BaseException)
   update = Signal(int, BackgroundResult)

   def __init__(self, ...):
      # Save information about the thread
      self.__refresh_thread = None
      self.__refresh_id = 0

      # Set this to True when exiting
      self.canceled = False

      # Connect the Signals to handler functions that run on the main thread
      self.update.connect(self.handle_update)
      self.background_exception.connect(self.handle_background_exception)

    def closeEvent(self, event):
      # Tell background threads when the widget closes
      self.canceled = True
      event.accept()

   def handle_background_exception(self, e: BaseException):
      # Handle the error
      QMessageBox.warning(...)

   def handle_update(self, refresh_id: int, result: BackgroundResult):
      # Apply the refresh if it's still for the latest call
      if refresh_id == self.__refresh_id:
         # Do something with result
         self.result_widget.set_message(result)

    def start_the_refresh(self):
        # This function starts the thread to run in the background

        # Update the GUI state to reflect the update
        self.result_widget.set_refreshing_status(True)

        self.__refresh_id += 1
        self.__refresh_thread = threading.Thread(
            target=self._refresh_thread_function,
            name=f"AWS Deadline Cloud Refresh Thread",
            args=(self.__refresh_id,),
        )
        self.__refresh_thread.start()

   def _refresh_thread_function(self, refresh_id: int):
      # This function is for the background thread
      try:
         # Call the slow operations
         result = boto3_client.potentially_expensive_api(...)
         # Only emit the result if it isn't canceled
         if not self.canceled:
            self.update.emit(refresh_id, result)
      except BaseException as e:
         # Use multiple signals for different meanings, such as handling errors.
         if not self.canceled:
            self.background_exception.emit(f"Background thread error", e)

```

**We recommend you set up your runtimes via `mise`.**

## Running Docker-based Unit Tests

- Some of the unit tests in this package require a docker environment to run. These tests are marked with `@pytest.mark.docker`. In order to execute these tests, please run the `run_sudo_tests.sh` script located in the `scripts` directory. For detailed instructions, please refer to [scripts/README.md](./scripts/README.md).
- If you make changes to the `download` or `asset_sync` modules, it's highly recommended to run and ensure these tests pass.
