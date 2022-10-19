import socket
import typing

from yarn_api_client.resource_manager import ResourceManager


class ResourceManagerClient:
    def __init__(self, rm_hostname: typing.Optional[str] = None, rm_port: typing.Optional[int] = None):
        rm_hostname = rm_hostname if rm_hostname else socket.getfqdn()
        rm_port = rm_port if rm_port else 8088
        self.rm_client = ResourceManager(service_endpoints=[f"http://{rm_hostname}:{rm_port}"])

    def list_yarn_applications(self) -> typing.Any:
        applications = self.rm_client.cluster_applications()
        if not applications or not applications.data or not applications.data["apps"]:
            return []
        return applications.data["apps"]["app"]

    def describe_application(self, application_id: str) -> typing.Any:
        return self.rm_client.cluster_application(application_id).data["app"]
