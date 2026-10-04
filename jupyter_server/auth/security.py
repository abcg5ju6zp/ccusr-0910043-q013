"""项目内部接口说明。"""

import getpass
import hashlib
import json
import os
import random
import traceback
import warnings
from contextlib import contextmanager

from jupyter_core.paths import jupyter_config_dir
from traitlets.config import Config
from traitlets.config.loader import ConfigFileNotFound, JSONFileConfigLoader

# Length of the salt in nr of hex chars, which implies salt_len * 4
# bits of randomness.
salt_len = 12


def passwd(passphrase=None, algorithm="argon2"):
    """项目内部接口说明。"""
    if passphrase is None:
        for _ in range(3):
            p0 = getpass.getpass("Enter password: ")
            p1 = getpass.getpass("Verify password: ")
            if p0 == p1:
                passphrase = p0
                break
            warnings.warn("Passwords do not match.", stacklevel=2)
        else:
            msg = "No matching passwords found. Giving up."
            raise ValueError(msg)

    if algorithm == "argon2":
        import argon2

        ph = argon2.PasswordHasher(
            memory_cost=10240,
            time_cost=10,
            parallelism=8,
        )
        h_ph = ph.hash(passphrase)

        return f"{algorithm}:{h_ph}"

    h = hashlib.new(algorithm)
    salt = ("%0" + str(salt_len) + "x") % random.getrandbits(4 * salt_len)
    h.update(passphrase.encode("utf-8") + salt.encode("ascii"))

    return f"{algorithm}:{salt}:{h.hexdigest()}"


def passwd_check(hashed_passphrase, passphrase):
    """项目内部接口说明。"""
    if hashed_passphrase.startswith("argon2:"):
        import argon2
        import argon2.exceptions

        ph = argon2.PasswordHasher()

        try:
            return ph.verify(hashed_passphrase[7:], passphrase)
        except argon2.exceptions.VerificationError:
            return False

    try:
        algorithm, salt, pw_digest = hashed_passphrase.split(":", 2)
    except (ValueError, TypeError):
        return False

    try:
        h = hashlib.new(algorithm)
    except ValueError:
        return False

    if len(pw_digest) == 0:
        return False

    h.update(passphrase.encode("utf-8") + salt.encode("ascii"))

    return h.hexdigest() == pw_digest


@contextmanager
def persist_config(config_file=None, mode=0o600):
    """项目内部接口说明。"""

    if config_file is None:
        config_file = os.path.join(jupyter_config_dir(), "jupyter_server_config.json")

    os.makedirs(os.path.dirname(config_file), exist_ok=True)

    loader = JSONFileConfigLoader(os.path.basename(config_file), os.path.dirname(config_file))
    try:
        config = loader.load_config()
    except ConfigFileNotFound:
        config = Config()

    yield config

    with open(config_file, "w", encoding="utf8") as f:
        f.write(json.dumps(config, indent=2))

    try:
        os.chmod(config_file, mode)
    except Exception:
        tb = traceback.format_exc()
        warnings.warn(
            f"Failed to set permissions on {config_file}:\n{tb}", RuntimeWarning, stacklevel=2
        )


def set_password(password=None, config_file=None):
    """项目内部接口说明。"""

    hashed_password = passwd(password)

    with persist_config(config_file) as config:
        config.IdentityProvider.hashed_password = hashed_password
    return hashed_password
