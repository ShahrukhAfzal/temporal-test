import asyncio
import argparse
from dataclasses import asdict
from pprint import pprint
from uuid import uuid4

from temporalio.client import Client

from temporal_app.order_models import OrderItem, OrderRequest
from temporal_app.order_workflows import OrderWorkflow
from temporal_app.worker import TASK_QUEUE


async def main(auto_signal_after: float | None) -> None:
    client = await Client.connect("localhost:7233")
    order = OrderRequest(
        order_id=f"order-{uuid4().hex[:8]}",
        customer_email="customer@example.com",
        items=[
            OrderItem(sku="LAPTOP-STAND", quantity=1, unit_price_cents=4999),
            OrderItem(sku="USB-C-CABLE", quantity=2, unit_price_cents=1299),
        ],
    )

    handle = await client.start_workflow(
        OrderWorkflow.run,
        order,
        id=order.order_id,
        task_queue=TASK_QUEUE,
    )

    print(f"Started workflow {order.order_id}")
    print(f"Expected payment ID: pay-{order.order_id}")
    print(
        "From another terminal, signal it with:\n"
        f"  .\\.venv\\Scripts\\python.exe -m temporal_app.signal_payment "
        f"{order.order_id} pay-{order.order_id}"
    )

    if auto_signal_after is not None:
        await asyncio.sleep(auto_signal_after)
        await handle.signal(OrderWorkflow.payment_completed, f"pay-{order.order_id}")
        print("Sent payment_completed signal automatically")

    result = await handle.result()

    print("Order completed:")
    pprint(asdict(result), sort_dicts=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a sample order workflow")
    parser.add_argument(
        "--auto-signal-after",
        type=float,
        metavar="SECONDS",
        help="send payment_completed from this client after the given delay",
    )
    args = parser.parse_args()
    asyncio.run(main(args.auto_signal_after))
