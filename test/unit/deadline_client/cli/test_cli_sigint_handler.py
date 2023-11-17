# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline.client.cli._groups._sigint_handler import SigIntHandler


class TestSigIntHAndler:
    def test_singleton_instance(self):
        """Ensures that only one instance of SigIntHandler can exist"""
        handler1 = SigIntHandler()
        handler2 = SigIntHandler()
        assert handler1 is handler2
