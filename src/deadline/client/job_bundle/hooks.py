# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""Public access to the submission-hook types.

The hook implementation lives in the private ``_hooks`` package. This module is the public
face of it, so callers (e.g. a DCC submitter writing a ``confirm_callback`` for
``deadline.client.ui.pre_gui_hooks.run_pre_gui_hooks``) have a supported type to import and
annotate against, rather than reaching into ``_hooks``.
"""

from ._hooks import HookManager

__all__ = ["HookManager"]
