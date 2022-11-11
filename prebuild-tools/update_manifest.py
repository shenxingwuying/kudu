#!/bin/env python3
# -*- coding: UTF-8 -*-

import json
import sys
import os
from collections import OrderedDict


if __name__ == "__main__":
    if len(sys.argv) > 1:
        workspace = sys.argv[1]
    else:
        raise Exception("Param dirctionary is not set for update manifest.")
    target_file = "package_index.json"

    files = os.listdir(workspace)
    files = [f for f in files if f.endswith(".manifest.json")]

    data = []
    for f in files:
        with open(f, "r") as infile:
            data.append(json.load(infile, object_pairs_hook=OrderedDict))

    # 只记录最近20次记录
    if len(data) > 20:
        data.sort(key=lambda x: x["lastUpdated"])
        data = data[-20:]

    package_index = {"package_index": []}
    package_index["lastUpdated"] = data[-1]["lastUpdated"]
    package_index["latestPackage"] = data[-1]["packageName"]
    for d in data:
        package_index["package_index"] += d["packages"]

    with open(target_file, "w") as outfile:
        json.dump(package_index, outfile, indent=4)
