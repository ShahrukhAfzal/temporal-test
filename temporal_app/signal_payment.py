import argparse
import asyncio

from temporalio.client import Client

from temporal_app.order_workflows import OrderWorkflow


async def main(workflow_id: str, payment_id: str) -> None:
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(OrderWorkflow.payment_completed, payment_id)
    print(f"Sent payment_completed({payment_id!r}) to {workflow_id!r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Confirm payment for a running OrderWorkflow"
    )
    parser.add_argument("workflow_id")
    parser.add_argument("payment_id")
    args = parser.parse_args()
    asyncio.run(main(args.workflow_id, args.payment_id))
