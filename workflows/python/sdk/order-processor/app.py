import json
import time
import workflow
from dapr.ext.workflow import WorkflowRuntime, DaprWorkflowClient
from dapr.clients import DaprClient
from model import InventoryItem, OrderPayload

STORE_NAME = "statestore-actors"
DEFAULT_ITEM_NAME = "cars"


class WorkflowConsoleApp:
    def main(self):
        workflow_runtime = WorkflowRuntime()
        workflow_runtime.register_workflow(workflow.order_processing_workflow)
        workflow_runtime.register_activity(workflow.notify_activity)
        workflow_runtime.register_activity(workflow.requst_approval_activity)
        workflow_runtime.register_activity(workflow.verify_inventory_activity)
        workflow_runtime.register_activity(workflow.process_payment_activity)
        workflow_runtime.register_activity(workflow.update_inventory_activity)
        workflow_runtime.start()
        time.sleep(3)

        base_inventory = {}
        base_inventory["paperclip"] = InventoryItem("Paperclip", 5, 100)
        base_inventory["cars"] = InventoryItem("Cars", 15000, 100)
        base_inventory["computers"] = InventoryItem("Computers", 500, 100)

        with DaprClient() as dapr_client:
            self.restock_inventory(dapr_client, base_inventory)

        print("==========Begin the purchase of item:==========")
        item_name = DEFAULT_ITEM_NAME
        order_quantity = 11

        total_cost = int(order_quantity) * base_inventory[item_name].per_item_cost
        order = OrderPayload(item_name=item_name, quantity=int(order_quantity), total_cost=total_cost)
        print(f'Starting order workflow, purchasing {order_quantity} of {item_name}')
        dapr_wf_client = DaprWorkflowClient()
        _id = dapr_wf_client.schedule_new_workflow(workflow.order_processing_workflow, input=order)
        state = dapr_wf_client.wait_for_workflow_completion(_id, timeout_in_seconds=60)
        print(f'Workflow completed with state: {state.runtime_status}')

    def restock_inventory(self, daprClient: DaprClient, baseInventory):
        for key, item in baseInventory.items():
            print(f'item: {item}')
            item = {"name": item.item_name, "quantity": item.quantity, "per_item_cost": item.per_item_cost}
            daprClient.save_state("statestore-actors", key, json.dumps(item))


if __name__ == '__main__':
    app = WorkflowConsoleApp()
    app.main()
