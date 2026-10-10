import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal_app.order_activities import (
        charge_payment,
        create_shipment,
        reserve_inventory,
        send_confirmation,
        validate_order,
        verify_customer,
    )
    from temporal_app.order_models import (
        ConfirmationRequest,
        FulfillmentRequest,
        InventoryReservation,
        OrderRequest,
        OrderResult,
        PaymentReceipt,
        PaymentRequest,
        Shipment,
        ShipmentRequest,
        VerificationRequest,
        VerificationResult,
    )


ACTIVITY_TIMEOUT = timedelta(seconds=10)
ACTIVITY_RETRY_POLICY = RetryPolicy(maximum_attempts=3)


@workflow.defn
class PaymentWorkflow:
    @workflow.run
    async def run(self, request: PaymentRequest) -> PaymentReceipt:
        return await workflow.execute_activity(
            charge_payment,
            request,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )


@workflow.defn
class InventoryWorkflow:
    @workflow.run
    async def run(self, request: FulfillmentRequest) -> InventoryReservation:
        return await workflow.execute_activity(
            reserve_inventory,
            request,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )


@workflow.defn
class CustomerVerificationWorkflow:
    @workflow.run
    async def run(self, request: VerificationRequest) -> VerificationResult:
        return await workflow.execute_activity(
            verify_customer,
            request,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )


@workflow.defn
class ShippingWorkflow:
    @workflow.run
    async def run(self, request: ShipmentRequest) -> Shipment:
        return await workflow.execute_activity(
            create_shipment,
            request,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.confirmed_payment_id: str | None = None
        self.order_status = "CREATED"

    @workflow.signal
    async def payment_completed(self, payment_id: str) -> None:
        """Record an external payment confirmation in workflow history."""
        self.confirmed_payment_id = payment_id

    @workflow.query
    def payment_status(self) -> str:
        """Expose the current gate status for clients and tests."""
        return self.order_status

    @workflow.run
    async def run(self, order: OrderRequest) -> OrderResult:
        self.order_status = "VALIDATING"
        await workflow.execute_activity(
            validate_order,
            order,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )

        total_cents = sum(
            item.quantity * item.unit_price_cents for item in order.items
        )

        # These three independent child workflows are scheduled together. Temporal
        # records and supervises each execution separately while they run in parallel.
        payment, reservation, verification = await asyncio.gather(
            workflow.execute_child_workflow(
                PaymentWorkflow.run,
                PaymentRequest(order_id=order.order_id, amount_cents=total_cents),
                id=f"{order.order_id}-payment",
            ),
            workflow.execute_child_workflow(
                InventoryWorkflow.run,
                FulfillmentRequest(order_id=order.order_id, items=order.items),
                id=f"{order.order_id}-inventory",
            ),
            workflow.execute_child_workflow(
                CustomerVerificationWorkflow.run,
                VerificationRequest(
                    order_id=order.order_id,
                    customer_email=order.customer_email,
                ),
                id=f"{order.order_id}-verification",
            ),
        )

        # Signals are durable: this condition remains waiting without polling or
        # holding a Python thread, and an early signal is remembered on replay.
        self.order_status = "WAITING_FOR_PAYMENT_CONFIRMATION"
        await workflow.wait_condition(
            lambda: self.confirmed_payment_id == payment.payment_id
        )
        self.order_status = "PAYMENT_CONFIRMED"

        # Shipping depends on inventory and only starts after all parallel checks pass.
        shipment = await workflow.execute_child_workflow(
            ShippingWorkflow.run,
            ShipmentRequest(
                order_id=order.order_id,
                reservation_id=reservation.reservation_id,
                simulate_failure=order.simulate_shipping_failure,
            ),
            id=f"{order.order_id}-shipping",
        )

        confirmation = await workflow.execute_activity(
            send_confirmation,
            ConfirmationRequest(
                order_id=order.order_id,
                customer_email=order.customer_email,
                payment_id=payment.payment_id,
                tracking_number=shipment.tracking_number,
            ),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )

        self.order_status = "COMPLETED"

        return OrderResult(
            order_id=order.order_id,
            status="COMPLETED",
            total_cents=total_cents,
            payment_id=payment.payment_id,
            reservation_id=reservation.reservation_id,
            verification_status=verification.status,
            tracking_number=shipment.tracking_number,
            confirmation=confirmation,
        )
