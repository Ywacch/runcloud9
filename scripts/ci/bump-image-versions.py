#!/usr/bin/env python3
"""Set image-versions.json tags for the names passed on the command line."""

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) < 4:
        sys.exit("usage: bump-image-versions.py <file> <tag> <image> [image...]")
    path, tag, *names = sys.argv[1:]
    file = Path(path)
    data = json.loads(file.read_text(encoding="utf-8"))
    missing = [name for name in names if name not in data]
    if missing:
        sys.exit("unknown image(s): " + ", ".join(missing))
    for name in names:
        data[name] = tag
    file.write_text(json.dumps(data, indent=4) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
