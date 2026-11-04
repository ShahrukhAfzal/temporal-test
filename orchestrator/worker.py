import asyncio, os
from temporalio.client import Client
from temporalio.worker import Worker
from orchestrator.workflows import OrderFulfillmentWorkflow
from orchestrator.activities import reserve, fulfill, ship, release, cancel_wms, cancel_tms, mark_complete, mark_failed

async def main():
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    await Worker(client, task_queue="orders", workflows=[OrderFulfillmentWorkflow], activities=[reserve, fulfill, ship, release, cancel_wms, cancel_tms, mark_complete, mark_failed]).run()
if __name__ == "__main__": asyncio.run(main())
