import unittest

import responses
from yarn_api_client.errors import APIError

from streaming_jupyter_integrations.yarn import find_session_jm_address


class TestYarn(unittest.TestCase):
    APP1_DATA = {
        "id": "application_1666172784500_0001",
        "name": "queries-session-cluster",
        "amRPCAddress": "node-1.example.com:35331",
        'state': 'RUNNING'
    }
    APP2_DATA = {
        "id": "application_1666172784500_0002",
        "name": "another-yarn-application",
        "amRPCAddress": "node-1.example.com:46921",
        'state': 'RUNNING'
    }

    @responses.activate
    def test_should_fetch_job_manager_address(self):
        self._mock_cluster_call()
        self._mock_cluster_apps_success([self.APP1_DATA])
        self._mock_cluster_app_success(self.APP1_DATA)

        hostname, port = find_session_jm_address(rm_hostname="example.com", rm_port=8123, yarn_application_id=None)

        self.assertEqual(hostname, "node-1.example.com")
        self.assertEqual(port, 35331)

    @responses.activate
    def test_should_fetch_job_manager_address_when_yarn_app_is_provided(self):
        self._mock_cluster_call()
        self._mock_cluster_apps_success([self.APP1_DATA, self.APP2_DATA])
        self._mock_cluster_app_success(self.APP1_DATA)
        self._mock_cluster_app_success(self.APP2_DATA)

        hostname, port = find_session_jm_address(rm_hostname="example.com", rm_port=8123,
                                                 yarn_application_id=self.APP2_DATA["id"])

        self.assertEqual(hostname, "node-1.example.com")
        self.assertEqual(port, 46921)

    @responses.activate
    def test_should_fail_if_app_does_not_exist(self):
        self._mock_cluster_call()
        self._mock_cluster_apps_success([self.APP1_DATA])
        self._mock_cluster_app_not_found(self.APP1_DATA["id"])

        with self.assertRaises(APIError) as e:
            find_session_jm_address(rm_hostname="example.com", rm_port=8123, yarn_application_id=self.APP1_DATA["id"])
        self.assertRegex(str(e.exception), "Response finished with status: 404.*")

    @responses.activate
    def test_should_fail_if_no_yarn_application_running(self):
        self._mock_cluster_call()
        self._mock_cluster_apps_success([])

        with self.assertRaises(ValueError) as e:
            find_session_jm_address(rm_hostname="example.com", rm_port=8123, yarn_application_id=None)
        self.assertEqual(str(e.exception), "No running YARN applications found.")

    @responses.activate
    def test_should_fail_if_there_are_many_yarn_application_running(self):
        self._mock_cluster_call()
        self._mock_cluster_apps_success([self.APP1_DATA, self.APP2_DATA])

        with self.assertRaises(ValueError) as e:
            find_session_jm_address(rm_hostname="example.com", rm_port=8123, yarn_application_id=None)
        self.assertEqual(str(e.exception), "Expected one running YARN application, but 2 found.")

    @staticmethod
    def _mock_cluster_call():
        responses.add(
            responses.GET,
            "http://example.com:8123/cluster",
            json={},
            status=200,
        )

    @staticmethod
    def _mock_cluster_apps_success(apps_list):
        responses.add(
            responses.GET,
            "http://example.com:8123/ws/v1/cluster/apps?states=RUNNING",
            json={"apps": {"app": apps_list}},
            status=200,
        )

    @staticmethod
    def _mock_cluster_app_success(app):
        responses.add(
            responses.GET,
            f"http://example.com:8123/ws/v1/cluster/apps/{app['id']}",
            json={"app": app},
            status=200,
        )

    @staticmethod
    def _mock_cluster_app_not_found(app_id):
        responses.add(
            responses.GET,
            f"http://example.com:8123/ws/v1/cluster/apps/{app_id}",
            json={},
            status=404,
        )
