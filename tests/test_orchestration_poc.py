import asyncio
import unittest
from datetime import timedelta
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from orchestrator.workflows import OrderFulfillmentWorkflow

@activity.defn(name="reserve")
async def reserve(x): return {"reservation_id": "RES-test"}
@activity.defn(name="fulfill")
async def fulfill(x): return {"task_id": "TASK-test"}
@activity.defn(name="ship")
async def ship(x): return {"shipment_id": "SHIP-test", "tracking_number": "TRACK-test"}
@activity.defn(name="mark_complete")
async def mark_complete(x): return {"status": "COMPLETED"}
@activity.defn(name="mark_failed")
async def mark_failed(x): return {"status": "FAILED"}
@activity.defn(name="cancel_wms")
async def cancel_wms(x): return {"status": "CANCELLED"}
@activity.defn(name="release")
async def release(x): return {"status": "RELEASED"}

@activity.defn(name="fulfill")
async def fulfill_business_failure(x): raise ApplicationError("permanent test failure", type="BusinessError", non_retryable=True)

class OrchestrationPOCTest(unittest.IsolatedAsyncioTestCase):
    async def test_workflow_executes_on_real_temporal_test_server(self):
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(env.client, task_queue="orders", workflows=[OrderFulfillmentWorkflow], activities=[reserve, fulfill, ship, mark_complete, mark_failed]):
                result = await env.client.execute_workflow(OrderFulfillmentWorkflow.run, {"order_id": "ORD-test", "sku": "SKU", "quantity": 1}, id="order-ORD-test", task_queue="orders", execution_timeout=timedelta(seconds=30))
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["shipment_id"], "SHIP-test")

    async def test_nonretryable_wms_failure_compensates_inventory(self):
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(env.client, task_queue="orders", workflows=[OrderFulfillmentWorkflow], activities=[reserve, fulfill_business_failure, ship, mark_complete, mark_failed, cancel_wms, release]):
                handle = await env.client.start_workflow(OrderFulfillmentWorkflow.run, {"order_id": "ORD-compensate", "sku": "SKU", "quantity": 1}, id="order-ORD-compensate", task_queue="orders")
                with self.assertRaises(Exception): await handle.result()
                progress = await handle.query(OrderFulfillmentWorkflow.status)
        self.assertEqual(progress["current_step"], "FAILED_COMPENSATED")
        self.assertEqual([x["operation"] for x in progress["compensation_results"]], ["inventory-release", "oms-fail"])

if __name__ == "__main__": unittest.main()
