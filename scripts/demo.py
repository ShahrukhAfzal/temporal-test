import asyncio
from temporalio.client import Client

async def main():
    client = await Client.connect("localhost:7233")
    handle = await client.start_workflow("OrderFulfillmentWorkflow", {"order_id": "ORD-DEMO", "sku": "SKU-101", "quantity": 2}, id="order-ORD-DEMO", task_queue="orders")
    print("Started", handle.id)
    print(await handle.result())

if __name__ == "__main__": asyncio.run(main())
