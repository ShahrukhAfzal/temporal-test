import asyncio
import time
import unittest

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_app.order_activities import (
    charge_payment,
    create_shipment,
    reserve_inventory,
    send_confirmation,
    validate_order,
    verify_customer,
)
from temporal_app.order_models import OrderItem, OrderRequest
from temporal_app.order_workflows import (
    CustomerVerificationWorkflow,
    InventoryWorkflow,
    OrderWorkflow,
    PaymentWorkflow,
    ShippingWorkflow,
)


class OrderWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_order(self) -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="order-test-task-queue",
                workflows=[
                    OrderWorkflow,
                    PaymentWorkflow,
                    InventoryWorkflow,
                    CustomerVerificationWorkflow,
                    ShippingWorkflow,
                ],
                activities=[
                    validate_order,
                    charge_payment,
                    reserve_inventory,
                    verify_customer,
                    create_shipment,
                    send_confirmation,
                ],
            ):
                started_at = time.monotonic()
                handle = await env.client.start_workflow(
                    OrderWorkflow.run,
                    OrderRequest(
                        order_id="test-order-1",
                        customer_email="buyer@example.com",
                        items=[
                            OrderItem(
                                sku="KEYBOARD",
                                quantity=2,
                                unit_price_cents=2500,
                            )
                        ],
                    ),
                    id="test-order-1",
                    task_queue="order-test-task-queue",
                )
                while (
                    await handle.query(OrderWorkflow.payment_status)
                    != "WAITING_FOR_PAYMENT_CONFIRMATION"
                ):
                    await asyncio.sleep(0.05)

                # Reaching this query state proves shipping has not started and the
                # workflow is durably blocked on its signal condition.
                await handle.signal(
                    OrderWorkflow.payment_completed,
                    "pay-test-order-1",
                )
                result = await handle.result()
                elapsed = time.monotonic() - started_at

        self.assertEqual("COMPLETED", result.status)
        self.assertEqual(5000, result.total_cents)
        self.assertEqual("pay-test-order-1", result.payment_id)
        self.assertEqual("res-test-order-1", result.reservation_id)
        self.assertEqual("APPROVED", result.verification_status)
        self.assertEqual("TRACK-test-order-1", result.tracking_number)
        self.assertIn("buyer@example.com", result.confirmation)
        # Each parallel activity waits one second. Sequential execution would take
        # roughly three seconds, so this proves the branches overlapped.
        self.assertLess(elapsed, 2.5)


if __name__ == "__main__":
    unittest.main()
