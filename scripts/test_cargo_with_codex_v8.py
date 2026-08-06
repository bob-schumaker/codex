#!/usr/bin/env python3

import sys
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import cargo_with_codex_v8


class CargoWithCodexV8Test(unittest.TestCase):
    def test_target_from_args_prefers_explicit_target_flag(self) -> None:
        self.assertEqual(
            cargo_with_codex_v8.cargo_target_from_args(
                ["build", "--target", "aarch64-apple-darwin"],
                {"CARGO_BUILD_TARGET": "x86_64-unknown-linux-gnu"},
            ),
            "aarch64-apple-darwin",
        )

    def test_target_from_args_supports_equals_form(self) -> None:
        self.assertEqual(
            cargo_with_codex_v8.cargo_target_from_args(
                ["build", "--target=x86_64-apple-darwin"],
                {},
            ),
            "x86_64-apple-darwin",
        )

    @mock.patch.object(
        cargo_with_codex_v8,
        "host_cargo_target",
        return_value="aarch64-apple-darwin",
    )
    @mock.patch.object(
        cargo_with_codex_v8,
        "resolve_codex_v8_cargo_env",
        return_value={
            "RUSTY_V8_ARCHIVE": "/tmp/librusty_v8.a.gz",
            "RUSTY_V8_SRC_BINDING_PATH": "/tmp/src_binding.rs",
        },
    )
    @mock.patch.object(cargo_with_codex_v8.subprocess, "run")
    def test_main_runs_cargo_with_codex_v8_env(
        self,
        run: mock.Mock,
        _resolve_codex_v8_cargo_env: mock.Mock,
        _host_cargo_target: mock.Mock,
    ) -> None:
        self.assertEqual(
            cargo_with_codex_v8.main(
                ["build", "-p", "codex-code-mode-host"],
                environ={"CARGO": "cargo-custom", "PATH": "/bin"},
            ),
            0,
        )

        run.assert_called_once_with(
            ["cargo-custom", "build", "-p", "codex-code-mode-host"],
            check=True,
            env={
                "CARGO": "cargo-custom",
                "PATH": "/bin",
                "RUSTY_V8_ARCHIVE": "/tmp/librusty_v8.a.gz",
                "RUSTY_V8_SRC_BINDING_PATH": "/tmp/src_binding.rs",
            },
        )


if __name__ == "__main__":
    unittest.main()
