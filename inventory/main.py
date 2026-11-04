from shared.mock_service import create_app
app = create_app("inventory", {"reserve": ("reservation_id", "RES"), "release": ("status", "RELEASED")})
