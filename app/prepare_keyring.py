"""Prepare an app-readable copy of the host secret without changing the source."""
from __future__ import annotations

import os
import pwd
from pathlib import Path

from app.config import _load_keyring


def prepare(source: Path, directory: Path, uid: int, gid: int) -> None:
    _load_keyring(source)  # Fail before replacing a usable keyring if the source is invalid.
    directory.mkdir(parents=True, exist_ok=True)
    os.chown(directory, uid, gid)
    os.chmod(directory, 0o700)
    temporary = directory / ".queue_keyring.tmp"
    try:
        # The directory is private and the service has exclusive access during startup.
        with temporary.open("wb") as output:
            os.chmod(temporary, 0o600)
            output.write(source.read_bytes())
            output.flush()
            os.fsync(output.fileno())
        os.chown(temporary, uid, gid)
        os.chmod(temporary, 0o400)
        os.replace(temporary, directory / "queue_keyring")
    finally:
        temporary.unlink(missing_ok=True)
    os.chmod(directory, 0o500)


if __name__ == "__main__":
    account = pwd.getpwnam("app")
    prepare(Path("/run/secrets/queue_keyring"), Path("/runtime-keyring"), account.pw_uid, account.pw_gid)
