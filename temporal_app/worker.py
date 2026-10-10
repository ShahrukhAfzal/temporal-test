import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_app.activities import compose_greeting
from temporal_app.order_activities import (
    charge_payment,
    create_shipment,
    reserve_inventory,
    send_confirmation,
    validate_order,
    verify_customer,
)
from temporal_app.order_workflows import (
    CustomerVerificationWorkflow,
    InventoryWorkflow,
    OrderWorkflow,
    PaymentWorkflow,
    ShippingWorkflow,
)
from temporal_app.workflows import GreetingWorkflow

TASK_QUEUE = "examples-task-queue"


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            GreetingWorkflow,
            OrderWorkflow,
            PaymentWorkflow,
            InventoryWorkflow,
            CustomerVerificationWorkflow,
            ShippingWorkflow,
        ],
        activities=[
            compose_greeting,
            validate_order,
            charge_payment,
            reserve_inventory,
            verify_customer,
            create_shipment,
            send_confirmation,
        ],
    )
    print(f"Worker listening on {TASK_QUEUE!r}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
