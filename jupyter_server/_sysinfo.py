"""项目内部接口说明。"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os
import platform
import subprocess
import sys

import jupyter_server


def pkg_commit_hash(pkg_path):
    """项目内部接口说明。"""

    # maybe we are in a repository, check for a .git folder
    p = os.path
    cur_path = None
    par_path = pkg_path
    while cur_path != par_path:
        cur_path = par_path
        if p.exists(p.join(cur_path, ".git")):
            try:
                proc = subprocess.Popen(
                    ["git", "rev-parse", "--short", "HEAD"],  # noqa: S607
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=pkg_path,
                )
                repo_commit, _ = proc.communicate()
            except OSError:
                repo_commit = None

            if repo_commit:
                return "repository", repo_commit.strip().decode("ascii")
            else:
                return "", ""
        par_path = p.dirname(par_path)

    return "", ""


def pkg_info(pkg_path):
    """项目内部接口说明。"""
    src, hsh = pkg_commit_hash(pkg_path)
    return {
        "jupyter_server_version": jupyter_server.__version__,
        "jupyter_server_path": pkg_path,
        "commit_source": src,
        "commit_hash": hsh,
        "sys_version": sys.version,
        "sys_executable": sys.executable,
        "sys_platform": sys.platform,
        "platform": platform.platform(),
        "os_name": os.name,
    }


def get_sys_info():
    """项目内部接口说明。"""
    p = os.path
    path = p.realpath(p.dirname(p.abspath(p.join(jupyter_server.__file__))))
    return pkg_info(path)
