import asyncio

from temporalio import activity

from temporal_app.order_models import (
    ConfirmationRequest,
    FulfillmentRequest,
    InventoryReservation,
    OrderRequest,
    PaymentReceipt,
    PaymentRequest,
    Shipment,
    ShipmentRequest,
    VerificationRequest,
    VerificationResult,
)


@activity.defn
async def validate_order(order: OrderRequest) -> str:
    if not order.order_id.strip():
        raise ValueError("Order ID is required")
    if "@" not in order.customer_email:
        raise ValueError("A valid customer email is required")
    if not order.items:
        raise ValueError("An order must contain at least one item")
    if any(item.quantity <= 0 or item.unit_price_cents <= 0 for item in order.items):
        raise ValueError("Item quantity and price must be positive")
    return "VALID"


@activity.defn
async def charge_payment(request: PaymentRequest) -> PaymentReceipt:
    # A real implementation would call a payment provider here.
    await asyncio.sleep(1)
    return PaymentReceipt(
        payment_id=f"pay-{request.order_id}",
        amount_cents=request.amount_cents,
        status="CAPTURED",
    )


@activity.defn
async def reserve_inventory(request: FulfillmentRequest) -> InventoryReservation:
    # A real implementation would reserve each SKU in an inventory system.
    await asyncio.sleep(1)
    return InventoryReservation(reservation_id=f"res-{request.order_id}")


@activity.defn
async def verify_customer(request: VerificationRequest) -> VerificationResult:
    # A real implementation could call a fraud or identity verification service.
    await asyncio.sleep(1)
    return VerificationResult(
        verification_id=f"verify-{request.order_id}",
        status="APPROVED",
    )


@activity.defn
async def create_shipment(request: ShipmentRequest) -> Shipment:
    # A real implementation would call a warehouse or carrier API.
    if request.simulate_failure:
        1 / 0
    return Shipment(
        shipment_id=f"ship-{request.order_id}",
        tracking_number=f"TRACK-{request.order_id}",
    )


@activity.defn
async def send_confirmation(request: ConfirmationRequest) -> str:
    # A real implementation would send an email or notification.
    return (
        f"Confirmation sent to {request.customer_email} for order "
        f"{request.order_id} with tracking {request.tracking_number}"
    )
