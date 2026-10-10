import unittest

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_app.activities import compose_greeting
from temporal_app.workflows import GreetingWorkflow


class GreetingWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_greeting(self) -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-task-queue",
                workflows=[GreetingWorkflow],
                activities=[compose_greeting],
            ):
                result = await env.client.execute_workflow(
                    GreetingWorkflow.run,
                    "Temporal",
                    id="test-greeting-workflow",
                    task_queue="test-task-queue",
                )

        self.assertEqual("Hello, Temporal!", result)


if __name__ == "__main__":
    unittest.main()

