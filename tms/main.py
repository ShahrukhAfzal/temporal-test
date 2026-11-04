from shared.mock_service import create_app
app = create_app("tms", {"shipments": ("shipment_id", "SHIP"), "shipments/cancel": ("status", "CANCELLED")})
