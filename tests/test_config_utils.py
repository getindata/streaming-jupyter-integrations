import os
import tempfile
import unittest
from unittest import mock

from streaming_jupyter_integrations.config_utils import read_flink_config_file


class TestConfigUtils(unittest.TestCase):

    def test_should_read_from_flink_conf_dir(self):
        with tempfile.TemporaryDirectory() as conf_dir:
            with tempfile.TemporaryDirectory() as home_dir:
                # Create flink-conf.yaml in FLINK_CONF_DIR
                self.__write_to_file(os.path.join(conf_dir, "flink-conf.yaml"), "property: some-value")
                # Create flink-conf.yaml in FLINK_HOME
                os.mkdir(os.path.join(home_dir, "conf"))
                self.__write_to_file(os.path.join(home_dir, "conf", "flink-conf.yaml"), "property: another-value")
                with mock.patch.dict(os.environ, {"FLINK_CONF_DIR": conf_dir, "FLINK_HOME": home_dir}, clear=True):
                    config = read_flink_config_file()

        self.assertEqual(config, {'property': 'some-value'})

    def test_should_read_from_flink_home_if_flink_conf_dir_not_defined(self):
        with tempfile.TemporaryDirectory() as home_dir:
            # Create flink-conf.yaml in FLINK_HOME
            os.mkdir(os.path.join(home_dir, "conf"))
            self.__write_to_file(os.path.join(home_dir, "conf", "flink-conf.yaml"), "property: another-value")
            with mock.patch.dict(os.environ, {"FLINK_HOME": home_dir}, clear=True):
                config = read_flink_config_file()

        self.assertEqual(config, {'property': 'another-value'})

    def test_return_empty_config_if_both_envs_not_defined(self):
        config = read_flink_config_file()
        self.assertEqual(config, {})

    @staticmethod
    def __write_to_file(file_path: str, content: str) -> None:
        f = open(file_path, "w")
        f.write(content)
        f.flush()
