#!/usr/bin/env python3
"""Run Cargo with Codex-built rusty_v8 artifact overrides."""

import os
import platform
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path


# Some developer environments set PYTHONSAFEPATH=1, which prevents Python from
# adding the script directory to sys.path. Add it explicitly so the local helper
# package remains importable when this executable is launched from any cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from codex_package.targets import TARGET_SPECS
from codex_package.targets import TargetSpec
from codex_package.targets import normalize_machine
from codex_package.v8 import resolve_codex_v8_cargo_env


def main(
    argv: list[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    argv = sys.argv[1:] if argv is None else argv
    environ = os.environ if environ is None else environ
    target = cargo_target_from_args(argv, environ)
    spec = target_spec(target)
    cargo_env = {**environ, **resolve_codex_v8_cargo_env(spec, environ=environ)}
    cargo = environ.get("CARGO", "cargo")
    cmd = [cargo, *argv]

    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True, env=cargo_env)
    return 0


def cargo_target_from_args(args: list[str], environ: Mapping[str, str]) -> str:
    explicit_target = explicit_target_from_args(args)
    if explicit_target is not None:
        return explicit_target

    cargo_build_target = environ.get("CARGO_BUILD_TARGET")
    if cargo_build_target:
        return cargo_build_target

    return host_cargo_target()


def explicit_target_from_args(args: list[str]) -> str | None:
    for index, arg in enumerate(args):
        if arg == "--target":
            try:
                return args[index + 1]
            except IndexError as exc:
                raise RuntimeError("cargo --target requires a target triple.") from exc
        if arg.startswith("--target="):
            return arg.removeprefix("--target=")
    return None


def host_cargo_target() -> str:
    system = platform.system().lower()
    machine = normalize_machine(platform.machine())
    if system == "darwin" and machine in {"aarch64", "x86_64"}:
        return f"{machine}-apple-darwin"
    if system == "linux" and machine in {"aarch64", "x86_64"}:
        return f"{machine}-unknown-linux-gnu"
    if system == "windows" and machine in {"aarch64", "x86_64"}:
        return f"{machine}-pc-windows-msvc"

    supported = ", ".join(sorted(TARGET_SPECS))
    raise RuntimeError(
        f"Unsupported host platform {platform.system()}/{platform.machine()}. "
        f"Pass --target explicitly. Supported targets: {supported}"
    )


def target_spec(target: str) -> TargetSpec:
    spec = TARGET_SPECS.get(target)
    if spec is not None:
        return spec

    supported = ", ".join(sorted(TARGET_SPECS))
    raise RuntimeError(
        f"Unsupported target for Codex-built V8 artifacts: {target}. "
        f"Supported targets: {supported}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
