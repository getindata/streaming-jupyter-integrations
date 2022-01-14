from IPython.core.display import display
from ipywidgets import widgets


class DeploymentBar:
    def __init__(self, interrupt_callback):
        self.interrupt_callback = interrupt_callback

    def __cancel_deployment(self, *args):
        print("Deployment cancellation in progress...")
        self.interrupt_callback()

    def __cancel_deployment_button(self):
        button = widgets.Button(
            description="Interrupt",
            disabled=False,
            button_style="warning",
            tooltip="Cancel the deployment",
            icon="times",
        )
        button.on_click(self.__cancel_deployment)
        return button

    def show_deployment_bar(self):
        display(widgets.HBox(children=(self.__cancel_deployment_button(),)))
