import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError
from orchestrator.workflows import OrderFulfillmentWorkflow

app = FastAPI(title="OMS")

class Order(BaseModel):
    order_id: str | None = None
    sku: str = "SKU-101"
    quantity: int = 1

@app.post("/orders")
async def create_order(order: Order):
    from uuid import uuid4
    order_id = order.order_id or f"ORD-{uuid4().hex[:8]}"
    workflow_id = f"order-{order_id}"
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    try:
        await client.start_workflow(OrderFulfillmentWorkflow.run, {**order.model_dump(), "order_id": order_id}, id=workflow_id, task_queue="orders")
    except WorkflowAlreadyStartedError:
        raise HTTPException(409, {"message": "Workflow already started", "workflow_id": workflow_id})
    return {"order_id": order_id, "workflow_id": workflow_id, "status": "WORKFLOW_STARTED"}

@app.get("/orders/{order_id}/workflow")
async def workflow_status(order_id: str):
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    handle = client.get_workflow_handle(f"order-{order_id}")
    try:
        desc = await handle.describe()
        progress = await handle.query(OrderFulfillmentWorkflow.status)
    except Exception as e:
        raise HTTPException(404, str(e))
    return {"workflow_id": f"order-{order_id}", "workflow_status": desc.status.name, **progress}

@app.post("/orders/{order_id}/complete")
async def complete(order_id: str): return {"order_id": order_id, "status": "COMPLETED"}

@app.post("/orders/{order_id}/fail")
async def fail(order_id: str): return {"order_id": order_id, "status": "FAILED"}
