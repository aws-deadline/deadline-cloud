# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

""" Module that defines the asset manifest versions. """

from enum import Enum


class ManifestVersion(str, Enum):
    """
    Enumerant of all Asset Manifest versions supported by this library.

    Special values:
      UNDEFINED -- Purely for internal testing.

    Versions:
      v2022_03_01 - The first version
      v2022_06_06 - Second version; massive simplification on the first manifest.
      v2023_03_03 - Third version; added support for 'size' and 'mtime' in the manifest.
    """

    UNDEFINED = "UNDEFINED"
    v2022_03_01 = "2022-03-01"
    v2022_06_06 = "2022-06-06"
    v2023_03_03 = "2023-03-03"
