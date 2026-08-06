#!/usr/bin/env python3

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from codex_package import v8


class V8DownloadTest(unittest.TestCase):
    @mock.patch.object(v8, "urlopen", side_effect=OSError("certificate failed"))
    @mock.patch.object(v8.subprocess, "run")
    def test_download_file_falls_back_to_curl(
        self,
        run: mock.Mock,
        _urlopen: mock.Mock,
    ) -> None:
        def fake_run(cmd: list[str], *, check: bool) -> subprocess.CompletedProcess:
            self.assertTrue(check)
            output = Path(cmd[cmd.index("-o") + 1])
            output.write_bytes(b"downloaded with curl")
            return subprocess.CompletedProcess(cmd, 0)

        run.side_effect = fake_run
        with tempfile.TemporaryDirectory() as temp_dir:
            dest = Path(temp_dir) / "artifact.gz"

            v8.download_file("https://example.test/artifact.gz", dest)

            self.assertEqual(dest.read_bytes(), b"downloaded with curl")
            run.assert_called_once()
            self.assertEqual(run.call_args.args[0][0], "curl")


if __name__ == "__main__":
    unittest.main()
