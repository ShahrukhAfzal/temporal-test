import asyncio

from temporalio.client import Client

from temporal_app.worker import TASK_QUEUE
from temporal_app.workflows import GreetingWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    result = await client.execute_workflow(
        GreetingWorkflow.run,
        "Temporal",
        id="greeting-workflow",
        task_queue=TASK_QUEUE,
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())

