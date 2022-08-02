"""streaming_jupyter_integrations module."""
import os
import sys
from typing import List

from setuptools import find_packages, setup
from setuptools.command.install import install

__version__ = "0.2.1"

with open("README.md") as f:
    README = f.read()


def get_requirements(filename: str) -> List[str]:
    with open(filename, "r", encoding="utf-8") as fp:
        reqs = [
            x.strip()
            for x in fp.read().splitlines()
            if not x.strip().startswith("#") and not x.strip().startswith("-i")
        ]
    return reqs


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self) -> None:
        tag = os.getenv('CI_COMMIT_TAG')

        if tag != __version__:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, __version__
            )
            sys.exit(info)


EXTRA_REQUIRE = {
    "tests": [
        "pytest>=6.2.2, <7.0.0",
        "pytest-cov>=2.8.0, <3.0.0",
        "pre-commit==2.15.0",
        "tox==3.21.1",
    ]
}

setup(
    name="streaming_jupyter_integrations",
    version=__version__,
    description="JupyterNotebook Flink magics",
    long_description=README,
    long_description_content_type="text/markdown",
    license="Apache Software License (Apache 2.0)",
    license_files=("LICENSE",),
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    keywords="jupyter flink sql ipython",
    author=u"GetInData",
    author_email="office@getindata.com",
    url="https://github.com/getindata/streaming-jupyter-integrations",
    packages=find_packages(exclude=["docs", "tests", "examples"]),
    include_package_data=True,
    install_requires=get_requirements("requirements.txt"),
    extras_require=EXTRA_REQUIRE,
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
