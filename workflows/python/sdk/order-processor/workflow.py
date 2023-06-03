
import logging
import json
from typing import Dict
from dapr.ext.workflow import DaprWorkflowContext, WorkflowActivityContext
from model import (OrderPayload, InventoryItem, InventoryRequest, InventoryResult,
                   PaymentRequest, OrderResult, Notification)
from dapr.clients import DaprClient

logging.basicConfig(level=logging.INFO)

STORE_NAME = "statestore-actors"


def order_processing_workflow(
        ctx: DaprWorkflowContext,
        order: OrderPayload):

    order_id = ctx.instance_id
    yield ctx.call_activity(
        activity=notify_activity,
        input=Notification(
            message=f'Received order {order_id} for {order.quantity} {order.item_name} at ${order.total_cost}!'))

    result = yield ctx.call_activity(
        activity=verify_inventory_activity,
        input=InventoryRequest(request_id=order_id, item_name=order.item_name, quantity=order.quantity))

    if not result.success:
        yield ctx.call_activity(
            activity=notify_activity,
            input=Notification(message=f'Insufficient inventory for {order.item_name}!'))
        return OrderResult(processed=False)

    yield ctx.call_activity(
        activity=process_payment_activity,
        input=PaymentRequest(
            request_id=order_id,
            item_being_purchased=order.item_name,
            amount=order.total_cost,
            quantity=order.quantity))

    try:
        yield ctx.call_activity(
            activity=update_inventory_activity,
            input=PaymentRequest(
                request_id=order_id,
                item_being_purchased=order.item_name,
                amount=order.total_cost,
                quantity=order.quantity))
    except Exception:
        yield ctx.call_activity(
            activity=notify_activity,
            input=Notification(message=f'Order {order_id} Failed!'))
        return OrderResult(processed=False)

    yield ctx.call_activity(
        activity=notify_activity,
        input=Notification(message=f'Order {order_id} has completed!'))
    return OrderResult(processed=True)


def notify_activity(ctx: WorkflowActivityContext, input: Notification):
    logger = logging.getLogger('NotifyActivity')
    logger.info(input.message)


def process_payment_activity(ctx: WorkflowActivityContext, input: PaymentRequest):
    logger = logging.getLogger('ProcessPaymentActivity')
    logger.info(f'Processing payment: {input.request_id} for {input.quantity} '
                f'{input.item_being_purchased} at {input.amount} USD')
    logger.info(f'Payment for request ID {input.request_id} processed successfully')


def verify_inventory_activity(
        ctx: WorkflowActivityContext,
        input: InventoryRequest) -> InventoryResult:
    logger = logging.getLogger('VerifyInventoryActivity')

    logger.info(f'Verifying inventory for order {input.request_id} of {input.quantity} {input.item_name}')
    inventoryItem: InventoryItem
    with DaprClient() as client:
        state = client.get_state(STORE_NAME, input.item_name)
    if state.data is None:
        return InventoryResult(False, None)

    state_json: Dict[str, str] = json.loads(state.data.decode('utf-8'))
    logger.info(f'There are {state_json["quantity"]} {state_json["name"]} available for purchase')
    inventoryItem = InventoryItem(
        item_name=input.item_name,
        per_item_cost=state_json['per_item_cost'],
        quantity=state_json['quantity'])

    if state_json['quantity'] >= input.quantity:
        return InventoryResult(True, inventoryItem)
    return InventoryResult(False, None)


def update_inventory_activity(
        ctx: WorkflowActivityContext,
        input: PaymentRequest) -> InventoryResult:
    logger = logging.getLogger('UpdateInventoryActivity')

    logger.info(f'Checking inventory for order {input.request_id} for {input.quantity} {input.item_being_purchased}')
    with DaprClient() as client:
        existing_state = client.get_state(STORE_NAME, input.item_being_purchased)
        existing_state_json = json.loads(existing_state.data.decode('utf-8'))
        new_quantity = existing_state_json['quantity'] - input.quantity
        per_item_cost = existing_state_json['per_item_cost']
        if new_quantity < 0:
            raise ValueError(
                f'Payment for request ID {input.item_being_purchased} could not be processed. Insufficient inventory.')
        new_state = json.dumps({
            "name": input.item_being_purchased,
            "quantity": new_quantity,
            "per_item_cost": per_item_cost,
        })
        client.save_state(STORE_NAME, input.item_being_purchased, new_state)
        logger.info(f'There are now {new_quantity} {input.item_being_purchased} left in stock')


def requst_approval_activity(
        ctx: WorkflowActivityContext,
        input: OrderPayload):
    logger = logging.getLogger('RequestApprovalActivity')
    logger.info(f'Requesting approval for payment of {input.total_cost} USD for {input.quantity} {input.item_name}')
