import datetime
import os
import subprocess
from collections import namedtuple


class stdnet_version(
    namedtuple("stdnet_version", "major minor micro releaselevel serial")
):
    __impl = None

    def __new__(cls, *args, **kwargs):
        if cls.__impl is None:
            cls.__impl = super(stdnet_version, cls).__new__(cls, *args, **kwargs)
            return cls.__impl
        else:
            raise TypeError("cannot create stdnet_version instances")


def get_version(version):
    "Returns a PEP 386-compliant version number from *version*."
    assert len(version) == 5
    assert version[3] in ("alpha", "beta", "rc", "final")
    parts = 2 if version[2] == 0 else 3
    main = ".".join(map(str, version[:parts]))
    sub = ""
    if version[3] == "alpha" and version[4] == 0:
        git_changeset = get_git_changeset()
        if git_changeset:
            sub = ".dev%s" % git_changeset
    elif version[3] != "final":
        mapping = {"alpha": "a", "beta": "b", "rc": "c"}
        sub = mapping[version[3]] + str(version[4])
    return main + sub


def get_git_changeset():
    """Returns a numeric identifier of the latest git changeset.

    The result is the UTC timestamp of the changeset in YYYYMMDDHHMMSS format.
    This value isn't guaranteed to be unique, but collisions are very unlikely,
    so it's sufficient for generating the development version numbers.
    """
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    git_show = subprocess.Popen(
        "git show --pretty=format:%ct --quiet HEAD",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        cwd=repo_dir,
        universal_newlines=True,
    )
    timestamp = git_show.communicate()[0].partition("\n")[0]
    try:
        timestamp = datetime.datetime.utcfromtimestamp(int(timestamp))
    except ValueError:
        return None
    return timestamp.strftime("%Y%m%d%H%M%S")
