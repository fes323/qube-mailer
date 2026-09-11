from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(os.name == "posix", "Container permissions require POSIX")
class PrepareKeyringTests(unittest.TestCase):
    def test_private_copy_refresh_and_invalid_source(self):
        from app.prepare_keyring import prepare

        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = directory / "source.json"
            fixture = Path(__file__).parent / "fixtures" / "queue-keyring.json"
            source.write_bytes(fixture.read_bytes())
            source.chmod(0o600)
            runtime = directory / "runtime"
            try:
                prepare(source, runtime, os.getuid(), os.getgid())
                target = runtime / "queue_keyring"
                assert target.read_bytes() == source.read_bytes()
                assert stat.S_IMODE(target.stat().st_mode) == 0o400
                assert stat.S_IMODE(runtime.stat().st_mode) == 0o500
                assert stat.S_IMODE(source.stat().st_mode) == 0o600
                assert target.stat().st_uid == os.getuid()
                # Running again must work with a previously read-only private directory.
                prepare(source, runtime, os.getuid(), os.getgid())
                previous = target.read_bytes()
                source.write_text("invalid")
                with self.assertRaises(RuntimeError):
                    prepare(source, runtime, os.getuid(), os.getgid())
                assert target.read_bytes() == previous
            finally:
                runtime.chmod(0o700)


if __name__ == "__main__":
    unittest.main()
