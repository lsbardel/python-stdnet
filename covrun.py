import os
import sys

from runtests import run

if __name__ == "__main__":
    if sys.version_info > (3, 3):
        run(coverage=True, coveralls=True)
    else:
        run()
