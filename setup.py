"""streaming_jupyter_integrations module."""
from typing import List

from setuptools import find_packages, setup

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
    version="0.0.1",
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
    url="https://gitlab.com/getindata/streaming-labs/streaming-jupyter-integrations",
    packages=find_packages(exclude=["docs", "tests", "examples"]),
    include_package_data=True,
    install_requires=get_requirements("requirements.txt"),
    extras_require=EXTRA_REQUIRE,
)
