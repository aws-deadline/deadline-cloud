# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import signal


class SigIntHandler:
    """
    A singleton class to handle SIGINT signals (triggered by Ctrl + C). Sets
    a flag `continue_operation` to False to indicate the interruption of an
    ongoing process. (e.g. job submission, output download)
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.continue_operation = True
            signal.signal(signal.SIGINT, cls._instance._handle_sigint)
        return cls._instance

    def _handle_sigint(self, signum, frame):
        self.continue_operation = False
