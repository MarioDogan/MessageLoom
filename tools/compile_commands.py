#!/usr/bin/env python3
"""Writes compile_commands.json for clangd from Bazel's C++ compile actions.

Usage: tools/compile_commands.py [bazel flags, e.g. --config=clang]
Run from anywhere inside the workspace after a build (external headers must exist).
"""

import json
import os
import subprocess
import sys


GCC_ONLY_FLAGS = {"-fno-canonical-system-headers"}


def bazel(*args):
    return subprocess.run(["bazel", *args], check=True, capture_output=True, text=True).stdout


def main():
    flags = sys.argv[1:]
    workspace = bazel("info", "workspace", *flags).strip()
    execroot = bazel("info", "execution_root", *flags).strip()
    query = 'mnemonic("CppCompile", deps(//mloom/... + //examples/...))'
    actions = json.loads(bazel("aquery", query, "--output=jsonproto", *flags)).get("actions", [])

    entries = []
    for action in actions:
        # clangd rejects GCC-only flags.
        arguments = [a for a in action["arguments"] if a not in GCC_ONLY_FLAGS]
        if "-c" not in arguments:
            continue
        source = arguments[arguments.index("-c") + 1]
        # Point our own sources at the workspace so clangd matches the files open in the editor.
        local = os.path.join(workspace, source)
        file = local if not source.startswith(("external/", "bazel-out/")) and os.path.exists(local) else os.path.join(execroot, source)
        entries.append({"directory": execroot, "arguments": arguments, "file": file})

    with open(os.path.join(workspace, "compile_commands.json"), "w") as out:
        json.dump(entries, out, indent=1)
    print(f"wrote {len(entries)} entries to {workspace}/compile_commands.json")


if __name__ == "__main__":
    main()
