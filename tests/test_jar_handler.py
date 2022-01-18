import os
import tempfile

import pytest

from streaming_jupyter_integrations.jar_handler import JarHandler


class TestJarHandler:
    """Test whether a jar folder is created on initialization"""

    def test_creates_jar_folder_on_init(self):
        tmp_dir = tempfile.mkdtemp()
        JarHandler(tmp_dir)
        assert os.path.exists(os.path.join(tmp_dir, "jars"))

    """Test whether content is not removed when initialization happens for jars folder"""

    def test_does_not_remove_content(self):
        tmp_dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(tmp_dir, "jars"))
        with open(os.path.join(tmp_dir, "jars", "file.jar"), "w+") as f:
            f.write("Husky.")
        JarHandler(tmp_dir)
        assert os.path.exists(os.path.join(tmp_dir, "jars"))
        assert os.path.exists(os.path.join(tmp_dir, "jars", "file.jar"))

    """Test copy local file"""

    def test_copy_local_file(self):
        some_other_dir = tempfile.mkdtemp()
        path_to_other_jar = os.path.join(some_other_dir, "golden_retriever.jar")
        tmp_dir = tempfile.mkdtemp()
        with open(path_to_other_jar, "w+") as f:
            f.write("Husky?")
        jar_handler = JarHandler(tmp_dir)
        copied_file = jar_handler.local_copy(path_to_other_jar)
        assert os.path.exists(os.path.join(tmp_dir, "jars"))
        assert os.path.exists(os.path.join(tmp_dir, "jars", "golden_retriever.jar"))
        assert "file://" in copied_file

    """Test copy local file. Throws if file does not exist"""

    def test_copy_local_file_throws_if_file_does_not_exist(self):
        some_other_dir = tempfile.mkdtemp()
        path_to_other_jar = os.path.join(some_other_dir, "golden_retriever.jar")
        tmp_dir = tempfile.mkdtemp()
        jar_handler = JarHandler(tmp_dir)
        with pytest.raises(ValueError):
            jar_handler.local_copy(path_to_other_jar)
