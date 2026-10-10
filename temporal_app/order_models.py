from dataclasses import dataclass


@dataclass
class OrderItem:
    sku: str
    quantity: int
    unit_price_cents: int


@dataclass
class OrderRequest:
    order_id: str
    customer_email: str
    items: list[OrderItem]
    simulate_shipping_failure: bool = False


@dataclass
class PaymentRequest:
    order_id: str
    amount_cents: int


@dataclass
class PaymentReceipt:
    payment_id: str
    amount_cents: int
    status: str


@dataclass
class FulfillmentRequest:
    order_id: str
    items: list[OrderItem]


@dataclass
class InventoryReservation:
    reservation_id: str


@dataclass
class VerificationRequest:
    order_id: str
    customer_email: str


@dataclass
class VerificationResult:
    verification_id: str
    status: str


@dataclass
class ShipmentRequest:
    order_id: str
    reservation_id: str
    simulate_failure: bool = False


@dataclass
class Shipment:
    shipment_id: str
    tracking_number: str


@dataclass
class ConfirmationRequest:
    order_id: str
    customer_email: str
    payment_id: str
    tracking_number: str


@dataclass
class OrderResult:
    order_id: str
    status: str
    total_cents: int
    payment_id: str
    reservation_id: str
    verification_status: str
    tracking_number: str
    confirmation: str
