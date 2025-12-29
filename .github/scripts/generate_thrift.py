#!/usr/bin/env python3
import importlib
import os
import shutil
import subprocess
import sys
from pathlib import Path


def find_baseplate_thrift_dir():
    try:
        bp = importlib.import_module("baseplate")
    except Exception as e:
        print("Failed to import baseplate:", e)
        return None
    bp_root = os.path.dirname(bp.__file__)
    for r, d, files in os.walk(bp_root):
        for f in files:
            if f.endswith(".thrift"):
                return os.path.dirname(os.path.join(r, f))
    return None


def main():
    thrift_include = find_baseplate_thrift_dir()
    if not thrift_include:
        print("Could not find baseplate .thrift files installed; aborting thrift gen")
        sys.exit(1)

    print("Using thrift include dir:", thrift_include)

    # Run thrift compiler
    cmd = ["thrift", "-I", thrift_include, "--gen", "py", "reddit_service_activity/activity.thrift"]
    try:
        subprocess.check_call(cmd)
    except FileNotFoundError:
        print("thrift compiler not found in PATH. Ensure 'thrift' is installed.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print("thrift compiler failed:", e)
        sys.exit(1)

    gen_dir = Path("gen-py")
    dest = Path("reddit_service_activity/activity_thrift")

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    if gen_dir.exists():
        for item in gen_dir.iterdir():
            try:
                shutil.move(str(item), str(dest))
            except Exception:
                # best-effort
                pass

    # Ensure package init
    init_file = dest / "__init__.py"
    if not init_file.exists():
        init_file.touch()

    # Cleanup
    if gen_dir.exists():
        try:
            shutil.rmtree(gen_dir)
        except Exception:
            pass

    print("Thrift gen completed")


if __name__ == "__main__":
    main()
