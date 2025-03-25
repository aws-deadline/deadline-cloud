# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#!/usr/bin/env python
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os
import platform

import click

from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from build_installer import main


def _snake_to_kebab(name: str) -> str:
    return name.replace("_", "-")


def _combine_callbacks(
    *callbacks: Callable[[click.Context, click.Option, Any], Any],
) -> Callable[[click.Context, click.Option, Any], Any]:
    """
    Combine multiple click callbacks which will then subsequently be called in order
    """

    def _combined_callback(ctx: click.Context, param: click.Option, value: Any):
        for callback in callbacks:
            value = callback(ctx, param, value)
        return value

    return _combined_callback


def _mutually_exclude(others: Iterable[str]) -> Callable[[click.Context, click.Option, Any], Any]:
    """
    Creates a callback wich raises an error if this argument is specified and any of other specified arguments are also specified
    """

    def _callback(ctx: click.Context, param: click.Option, value: Any) -> Any:
        if value:
            for other in others:
                if ctx.params.get(other):
                    raise click.BadParameter(
                        f"Cannot specify both --{param.name} and --{_snake_to_kebab(other)}"
                    )
        return value

    return _callback


def _dependency(dependency: str) -> Callable[[click.Context, click.Option, Any], Any]:
    """
    Creates a callback that raises an error if this argument is specified but an given argument it depends on is not
    """

    def _callback(ctx: click.Context, param: click.Option, value: Any) -> Any:
        if value:
            if not ctx.params.get(dependency):
                raise click.BadParameter(
                    f"Must specify --{_snake_to_kebab(dependency)} when specifying --{param.name}"
                )
        return value

    return _callback


def _require_if_false_or_unspecified(
    other: str,
) -> Callable[[click.Context, click.Option, Any], Any]:
    """
    Creates a callback that raises an error if this argument is not specified and a given argument is either False or unspecified
    """

    def _callback(ctx: click.Context, param: click.Option, value: Any) -> Any:
        if not ctx.params.get(other):
            if not value:
                raise click.BadParameter(
                    f"Must specify --{param.name} when --{_snake_to_kebab(other)} is not specified"
                )
        return value

    return _callback


def _require_if_all_false_or_unspecified(
    others: Iterable[str],
) -> Callable[[click.Context, click.Option, Any], Any]:
    """
    Creates a callback that raises an error if this argument is not specified and all of the given arguments are either False or unspecified
    """

    def _callback(ctx: click.Context, param: click.Option, value: Any) -> Any:
        if value is not None:
            return value
        for other in others:
            if ctx.params.get(other):
                return value
        all_params = [f"--{_snake_to_kebab(other)}" for other in others]
        name = _snake_to_kebab(param.name if param.name else "")
        raise click.BadParameter(
            f"Must specify --{name} when none of {', '.join(all_params)} are specified"
        )

    return _callback


def _current_platform_as_default(
    _ctx: click.Context, _param: click.Option, value: Optional[str]
) -> str:
    """
    A callback that dynamically sets the default to the current platform
    """
    if value is None:
        value = platform.system()
        if value == "Darwin":
            return "MacOS"
    return value


def _not_allowed_if_env_var_set(
    env_var_name: str,
) -> Callable[[click.Context, click.Option, Any], Any]:
    """
    Creates a callback that raises an error if the argument was specified and a given environment variable is set
    """

    def _callback(_ctx: click.Context, param: click.Option, value: Any) -> Any:
        if value and os.environ.get(env_var_name) is not None:
            raise click.BadParameter(f"--{param.name} cannot be used when {env_var_name} is set.")
        return value

    return _callback


@click.command()
@click.option(
    "--install-builder-path",
    type=Path,
    callback=_mutually_exclude(["install_builder_s3_bucket"]),
    help="The path to the InstallBuilder builder executable",
)
@click.option(
    "--install-builder-s3-bucket",
    type=str,
    callback=_combine_callbacks(
        _require_if_false_or_unspecified("local_dev"), _mutually_exclude(["install_builder_path"])
    ),
    help="The name of S3 Bucket that contains an archive of an install of InstallBuilder",
)
@click.option(
    "--install-builder-s3-key",
    type=str,
    callback=_dependency("install_builder_s3_bucket"),
    help="The key of the archive of an install of InstallBuilder",
)
@click.option(
    "--install-builder-license-path",
    type=Path,
    callback=_require_if_all_false_or_unspecified(["dev", "local_dev"]),
    help="The path to the InstallBuilder license file",
)
@click.option(
    "--dev",
    is_flag=True,
    callback=_mutually_exclude(["local_dev"]),
    help="If specified, build in dev mode",
)
@click.option(
    "--local-dev",
    is_flag=True,
    callback=_combine_callbacks(
        _not_allowed_if_env_var_set("CODEBUILD_BUILD_ID"), _mutually_exclude(["dev"])
    ),
    help="If specified, build in local dev mode",
)
@click.option(
    "--platform",
    type=click.Choice(["Windows", "MacOS", "Linux"], case_sensitive=False),
    callback=_combine_callbacks(
        _require_if_false_or_unspecified("local_dev"), _current_platform_as_default
    ),
    help="The platform to build the installer for. Default is the current platform",
)
@click.option(
    "--output-dir",
    type=Path,
    callback=_require_if_false_or_unspecified("local_dev"),
    help="The directory to output the installer to",
)
@click.option(
    "--no-cleanup",
    is_flag=True,
    help="If specified, do not clean up the temporary directory after building the installer",
)
@click.option(
    "--installer-source-path",
    type=Path,
    help="The path to the installer source xml file",
)
def cli(
    install_builder_path: Optional[Path],
    install_builder_s3_bucket: Optional[str],
    install_builder_s3_key: Optional[str],
    install_builder_license_path: Optional[Path],
    dev: bool,
    local_dev: bool,
    platform: str,
    output_dir: Optional[Path],
    no_cleanup: bool,
    installer_source_path: Path,
) -> None:
    cli_body(
        install_builder_path,
        install_builder_s3_bucket,
        install_builder_s3_key,
        install_builder_license_path,
        dev,
        local_dev,
        platform,
        output_dir,
        no_cleanup,
        installer_source_path,
    )


def cli_body(
    install_builder_path: Optional[Path],
    install_builder_s3_bucket: Optional[str],
    install_builder_s3_key: Optional[str],
    install_builder_license_path: Optional[Path],
    dev: bool,
    local_dev: bool,
    platform: str,
    output_dir: Optional[Path],
    no_cleanup: bool,
    installer_source_path: Path,
) -> None:
    """
    Separate from the command function so we can mock the body out
    when testing the cli arguments
    """
    main(
        dev or local_dev,
        install_builder_path,
        install_builder_license_path,
        install_builder_s3_bucket,
        install_builder_s3_key,
        output_dir,
        not no_cleanup,
        platform,
        installer_source_path,
    )


if __name__ == "__main__":
    cli()
