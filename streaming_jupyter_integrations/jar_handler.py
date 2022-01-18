import os.path
from shutil import copy2

import requests
import wget


class JarHandler:
    def __init__(self, project_root_dir: str):
        self.project_root_dir = project_root_dir
        self.project_jars = os.path.join(project_root_dir, "jars")
        self.create_project_jars()

    def local_copy(self, jar_path: str) -> str:
        if not os.path.exists(jar_path):
            raise ValueError(f"Path {jar_path} does not exist.")
        full_dst = copy2(jar_path, self.project_jars)
        return JarHandler.__prepend_file_protocol(full_dst)

    def create_project_jars(self) -> None:
        if not os.path.exists(self.project_jars):
            os.makedirs(self.project_jars, exist_ok=True)
        elif not os.path.isdir(self.project_jars):
            raise ValueError(
                f"{self.project_jars} is already in the project directory with incompatible type."
            )

    @staticmethod
    def __ensure_jar_accessible(remote_path: str) -> None:
        r = requests.head(remote_path, allow_redirects=True)
        if r.status_code != 200:
            raise ValueError(f"Remote path {remote_path} is not accessible")

    @staticmethod
    def __prepend_file_protocol(path: str) -> str:
        return f"file://{path}"

    def remote_copy(self, remote_path: str) -> str:
        JarHandler.__ensure_jar_accessible(remote_path)
        # Ensure wget overwrites the file. Without removing the current file `wget`
        # by default adds `__{number}` to the name.
        filename = wget.filename_from_url(remote_path)
        local_filepath = os.path.join(self.project_jars, filename)
        if os.path.exists(local_filepath):
            os.remove(local_filepath)

        # Well, wget does not seem to know that it should output an end line after it's done.
        full_dst = wget.download(url=remote_path, out=self.project_jars)
        print()

        return JarHandler.__prepend_file_protocol(full_dst)
