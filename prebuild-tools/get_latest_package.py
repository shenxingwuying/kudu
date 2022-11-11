#!/bin/env python3
# -*- coding: UTF-8 -*-

import json
import sys
import os
from collections import OrderedDict


def main():
    if len(sys.argv) > 1:
        workspace = sys.argv[1]
    else:
        raise Exception("Param dirctionary is not set for get latest package.")

    files = os.listdir(workspace)
    files = [f for f in files if f.endswith("package_index.json")]
    if len(files) > 1:
        raise Exception("There are too many package_index.json files.")

    with open(files[0], "r") as infile:
        data = json.load(infile, object_pairs_hook=OrderedDict)
    latestPackage = data["latestPackage"]
    print(latestPackage)


if __name__ == "__main__":
    main()
