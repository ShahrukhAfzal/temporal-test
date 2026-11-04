from shared.mock_service import create_app
app = create_app("wms", {"fulfill": ("task_id", "TASK"), "cancel": ("status", "CANCELLED")})
