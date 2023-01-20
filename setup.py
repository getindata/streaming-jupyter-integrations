"""streaming_jupyter_integrations module."""
import json
import os
import sys
from pathlib import Path
from typing import List

from setuptools import find_packages, setup
from setuptools.command.install import install

__version__ = "0.13.1"

HERE = Path(__file__).parent.resolve()

# Get the package info from package.json
pkg_json = json.loads((HERE / "package.json").read_bytes())
lab_path = (HERE / pkg_json["jupyterlab"]["outputDir"])

# Representative files that should exist after a successful build
ensured_targets = [str(lab_path / "package.json")]

labext_name = pkg_json["name"]

data_files_spec = [
    (f"share/jupyter/labextensions/{labext_name}", str(lab_path.relative_to(HERE)), "**"),
    (f"share/jupyter/labextensions/{labext_name}", str("."), "install.json"),
]

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
        "jupyter-packaging>=0.12.2",
        "responses>=0.22.0",
    ]
}

setup_args = {
    'name': "streaming_jupyter_integrations",
    'version': __version__,
    'description': "JupyterNotebook Flink magics",
    'long_description': README,
    'long_description_content_type': "text/markdown",
    'license': "Apache Software License (Apache 2.0)",
    'license_files': ("LICENSE",),
    'python_requires': ">=3.7",
    'classifiers': [
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    'keywords': "jupyter flink sql ipython",
    'author': u"GetInData",
    'author_email': "office@getindata.com",
    'url': "https://github.com/getindata/streaming-jupyter-integrations",
    'packages': find_packages(exclude=["docs", "tests", "examples"]),
    'include_package_data': True,
    'install_requires': get_requirements("requirements.txt"),
    'extras_require': EXTRA_REQUIRE,
    'cmdclass': {
        'verify': VerifyVersionCommand,
    }
}

try:
    from jupyter_packaging import get_data_files, npm_builder, wrap_installers
    post_develop = npm_builder(
        build_cmd="install:extension", source_dir="src", build_dir=lab_path
    )
    setup_args["cmdclass"] = wrap_installers(post_develop=post_develop, ensured_targets=ensured_targets)
    setup_args["data_files"] = get_data_files(data_files_spec)
except ImportError as e:
    import logging
    logging.basicConfig(format="%(levelname)s: %(message)s")
    logging.warning("Build tool `jupyter-packaging` is missing. Install it with pip or conda.")
    if not ("--name" in sys.argv or "--version" in sys.argv):
        raise e

if __name__ == "__main__":
    setup(**setup_args)
