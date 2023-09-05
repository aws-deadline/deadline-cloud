# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module that defines the asset manifest versions. """

from enum import Enum


class ManifestVersion(str, Enum):
    """
    Enumerant of all Asset Manifest versions supported by this library.

    Special values:
      UNDEFINED -- Purely for internal testing.

    Versions:
      v2023_03_03 - First version.
    """

    UNDEFINED = "UNDEFINED"
    v2023_03_03 = "2023-03-03"
