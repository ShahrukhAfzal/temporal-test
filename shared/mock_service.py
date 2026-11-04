import asyncio
from collections import defaultdict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class Failure(BaseModel):
    operation: str
    mode: str = "none"
    fail_count: int = 1
    delay_seconds: float = 0


class Request(BaseModel):
    order_id: str
    operation_id: str
    sku: str = "SKU-101"
    quantity: int = 1


def create_app(service: str, operations: dict[str, tuple[str, str]]):
    app = FastAPI(title=f"{service} mock")
    app.state.failures, app.state.attempts = {}, defaultdict(int)
    app.state.results, app.state.state = {}, {}

    @app.post("/admin/failure")
    async def configure(config: Failure):
        if config.operation not in operations or config.mode not in {
            "none", "http_500", "http_503", "fail_n_times", "timeout",
            "delay", "business_error", "connection_drop",
        }:
            raise HTTPException(400, "Unknown operation or failure mode")
        app.state.failures[config.operation] = config.model_dump()
        app.state.attempts[config.operation] = 0
        return {"configured": config}

    @app.post("/admin/reset")
    async def reset():
        app.state.failures.clear()
        app.state.attempts.clear()
        return {"reset": True}

    @app.get("/admin/state")
    async def state():
        return {"failures": app.state.failures, "attempts": app.state.attempts,
                "business_state": app.state.state}

    async def perform(op: str, req: Request):
        cfg = app.state.failures.get(op, {"mode": "none"})
        n = app.state.attempts[op]
        app.state.attempts[op] += 1
        mode = cfg["mode"]
        print(f"{service} {op} order={req.order_id} operation={req.operation_id} attempt={n+1} mode={mode}", flush=True)
        if mode == "fail_n_times" and n < cfg.get("fail_count", 1):
            raise HTTPException(503, "injected transient failure")
        if mode == "http_500": raise HTTPException(500, "injected failure")
        if mode == "http_503": raise HTTPException(503, "injected unavailable")
        if mode == "business_error": raise HTTPException(422, "injected permanent business error")
        if mode == "connection_drop":
            raise HTTPException(503, "connection_drop is approximated by service failure")
        if mode == "timeout":
            # Simulate a committed operation whose response exceeds the caller timeout.
            result = app.state.results.get(req.operation_id)
            if result is None:
                result = {"order_id": req.order_id, operations[op][0]: f"{operations[op][1]}-{req.order_id}"}
                result["status"] = {"reserve": "RESERVED", "release": "RELEASED", "fulfill": "FULFILLED", "cancel": "CANCELLED", "shipments": "SHIPMENT_CREATED", "shipments/cancel": "CANCELLED"}[op]
                if op == "shipments": result["tracking_number"] = f"TRACK-{req.order_id}"
                app.state.results[req.operation_id] = result
                app.state.state[req.operation_id] = result
                await asyncio.sleep(40)
            return result
        if mode == "delay": await asyncio.sleep(cfg.get("delay_seconds", 1))
        if req.operation_id not in app.state.results:
            result = {"order_id": req.order_id, operations[op][0]: f"{operations[op][1]}-{req.order_id}"}
            result["status"] = {"reserve": "RESERVED", "release": "RELEASED", "fulfill": "FULFILLED", "cancel": "CANCELLED", "shipments": "SHIPMENT_CREATED", "shipments/cancel": "CANCELLED"}[op]
            if op == "shipments": result["tracking_number"] = f"TRACK-{req.order_id}"
            app.state.results[req.operation_id] = result
            app.state.state[req.operation_id] = result
        return app.state.results[req.operation_id]

    def endpoint(op):
        async def call(req: Request): return await perform(op, req)
        return call
    for op in operations:
        app.add_api_route(f"/{op}", endpoint(op), methods=["POST"], name=op)
    return app
