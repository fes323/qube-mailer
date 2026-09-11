from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path


def encoded_key() -> str:
    return base64.b64encode(os.urandom(32)).decode("ascii")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an AES-GCM queue keyring")
    parser.add_argument("path", nargs="?", default="secrets/queue-keyring.json")
    args = parser.parse_args()
    path = Path(args.path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "active": 1,
        "keys": {"1": encoded_key()},
        "dedupe_key": encoded_key(),
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2)
            file.write("\n")
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    print(f"Created {path}")


if __name__ == "__main__":
    main()
