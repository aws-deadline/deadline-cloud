import logging
import os
import shutil
import sys

from dataclasses import dataclass
from hatchling.builders.hooks.plugin.interface import BuildHookInterface


_logger = logging.Logger(__name__, logging.INFO)
_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.addFilter(lambda record: record.levelno > logging.INFO)
_logger.addHandler(_stdout_handler)
_logger.addHandler(_stderr_handler)


@dataclass
class CopyConfig:
    sources: list
    destinations: list


class CustomBuildHookException(Exception):
    pass


class CustomBuildHook(BuildHookInterface):
    """
    A Hatch build hook that is pulled in automatically by Hatch's "custom" hook support
    See: https://hatch.pypa.io/1.6/plugins/build-hook/custom/
    This build hook copies files from one location (sources) to another (destinations).
    Config options:
    - `log_level (str)`: The logging level. Any value accepted by logging.Logger.setLevel is allowed. Default is INFO.
    - `copy_map (list[dict])`: A list of mappings of files to copy and the destinations to copy them into. In TOML files,
      this is expressed as an array of tables. See https://toml.io/en/v1.0.0#array-of-tables
    Example TOML config:
    ```
    [tool.hatch.build.hooks.custom]
    path = "hatch_hook.py"
    log_level = "DEBUG"
    [[tool.hatch.build.hooks.custom.copy_map]]
    sources = [
      "_version.py",
    ]
    destinations = [
      "src/openjobio",
      "src/openjobio_adaptor_runtime",
      "src/openjobio_adaptor_runtime_client",
    ]
    [[tool.hatch.build.hooks.custom.copy_map]]
    sources = [
      "something_the_tests_need.py",
      "something_else_the_tests_need.ini",
    ]
    destinations = [
      "test/openjobio",
      "test/openjobio_adaptor_runtime",
      "test/openjobio_adaptor_runtime_client",
    ]
    ```
    """

    REQUIRED_OPTS = [
        "copy_map",
    ]

    def initialize(self, version, build_data) -> None:
        if not self._prepare():
            return

        for copy_cfg in self.copy_map:
            _logger.info(f"Copying {copy_cfg.sources} to {copy_cfg.destinations}")
            for destination in copy_cfg.destinations:
                for source in copy_cfg.sources:
                    copy_func = shutil.copy if os.path.isfile(source) else shutil.copytree
                    copy_func(
                        os.path.join(self.root, source),
                        os.path.join(self.root, destination),
                    )
            _logger.info("Copy complete")

    def clean(self, versions) -> None:
        if not self._prepare():
            return

        for copy_cfg in self.copy_map:
            _logger.info(f"Cleaning {copy_cfg.sources} from {copy_cfg.destinations}")
            cleaned_count = 0
            for destination in copy_cfg.destinations:
                for source in copy_cfg.sources:
                    source_path = os.path.join(self.root, destination, source)
                    remove_func = os.remove if os.path.isfile(source_path) else os.rmdir
                    try:
                        remove_func(source_path)
                    except FileNotFoundError:
                        _logger.debug(f"Skipping {source_path} because it does not exist...")
                    else:
                        cleaned_count += 1
            _logger.info(f"Cleaned {cleaned_count} items")

    def _prepare(self) -> bool:
        missing_required_opts = [
            opt for opt in self.REQUIRED_OPTS if opt not in self.config or not self.config[opt]
        ]
        if missing_required_opts:
            _logger.warn(
                f"Required options {missing_required_opts} are missing or empty. "
                "Contining without copying sources to destinations...",
                file=sys.stderr,
            )
            return False

        log_level = self.config.get("log_level")
        if log_level:
            _logger.setLevel(log_level)

        return True

    # Function that copies a list of files from one location to another python 3.7
    # def copy_files(self, sources: list[str], destinations: list[str]) -> None:

    @property
    def copy_map(self):
        raw_copy_map: list[dict] = self.config.get("copy_map")
        if not raw_copy_map:
            return None

        if not (
            isinstance(raw_copy_map, list)
            and all(isinstance(copy_cfg, dict) for copy_cfg in raw_copy_map)
        ):
            raise CustomBuildHookException(
                f'"copy_map" config option is a nonvalid type. Expected list[dict], but got {raw_copy_map}'
            )

        def verify_list_of_file_paths(file_paths, config_name):
            if not (isinstance(file_paths, list) and all(isinstance(fp, str) for fp in file_paths)):
                raise CustomBuildHookException(
                    f'"{config_name}" config option is a nonvalid type. Expected list[str], but got {file_paths}'
                )

            missing_paths = [
                fp for fp in file_paths if not os.path.exists(os.path.join(self.root, fp))
            ]
            if len(missing_paths) > 0:
                raise CustomBuildHookException(
                    f'"{config_name}" config option contains some file paths that do not exist: {missing_paths}'
                )

        copy_map: list[CopyConfig] = []

        for copy_cfg in raw_copy_map:
            destinations: list[str] = copy_cfg.get("destinations")
            verify_list_of_file_paths(destinations, "destinations")

            sources: list[str] = copy_cfg.get("sources")
            verify_list_of_file_paths(sources, "source")

            copy_map.append(CopyConfig(sources, destinations))

        return copy_map
