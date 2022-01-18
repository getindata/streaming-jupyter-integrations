from typing import Callable

from IPython.core.display import display
from ipywidgets import widgets


class DeploymentBar:
    def __init__(self, interrupt_callback: Callable[[], None]):
        self.interrupt_callback = interrupt_callback

    def __cancel_deployment(self, *args: str) -> None:
        print("Deployment cancellation in progress...")
        self.interrupt_callback()

    def __cancel_deployment_button(self) -> widgets.Button:
        button = widgets.Button(
            description="Interrupt",
            disabled=False,
            button_style="warning",
            tooltip="Cancel the deployment",
            icon="times",
        )
        button.on_click(self.__cancel_deployment)
        return button

    def show_deployment_bar(self) -> None:
        display(widgets.HBox(children=(self.__cancel_deployment_button(),)))
