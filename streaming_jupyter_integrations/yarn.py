import socket
from typing import Any, Optional, Tuple

from yarn_api_client.resource_manager import ResourceManager


class ResourceManagerClient:
    def __init__(self, rm_hostname: Optional[str] = None, rm_port: Optional[int] = None):
        if rm_hostname is None:
            rm_hostname = socket.getfqdn()
        if rm_port is None:
            rm_port = 8088
        self.rm_client = ResourceManager(service_endpoints=[f"http://{rm_hostname}:{rm_port}"])

    def list_yarn_applications(self) -> Any:
        applications = self.rm_client.cluster_applications(states=["RUNNING"])
        if not applications or not applications.data or not applications.data["apps"]:
            return []
        return applications.data["apps"]["app"]

    def describe_application(self, application_id: str) -> Any:
        return self.rm_client.cluster_application(application_id).data["app"]


def find_session_jm_address(rm_hostname: Optional[str], rm_port: Optional[int],
                            yarn_application_id: Optional[str]) -> Tuple[str, int]:
    if rm_hostname and rm_port:
        rm_client = ResourceManagerClient(rm_hostname, rm_port)
    else:
        rm_client = ResourceManagerClient()
    if yarn_application_id is None:
        yarn_application_id = __find_yarn_application_id(rm_client)

    app_metadata = rm_client.describe_application(yarn_application_id)
    address = app_metadata["amRPCAddress"]
    address_parts = address.rsplit(":", 1)
    hostname = address_parts[0]
    port = int(address_parts[1])
    return hostname, port


def __find_yarn_application_id(rm_client: ResourceManagerClient) -> str:
    yarn_apps = rm_client.list_yarn_applications()
    yarn_apps_count = len(yarn_apps)
    if yarn_apps_count == 0:
        raise ValueError("No running YARN applications found.")
    elif yarn_apps_count > 1:
        raise ValueError(f"Expected one running YARN application, but {yarn_apps_count} found.")
    else:
        return yarn_apps[0]["id"]
