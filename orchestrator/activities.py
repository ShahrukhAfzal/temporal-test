import os
import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

URLS = {"inventory": os.getenv("INVENTORY_URL", "http://localhost:8002"), "wms": os.getenv("WMS_URL", "http://localhost:8003"), "tms": os.getenv("TMS_URL", "http://localhost:8004"), "oms": os.getenv("OMS_URL", "http://localhost:8001")}
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "25"))

async def post(service, path, data):
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.post(f"{URLS[service]}/{path}", json=data)
            if response.status_code == 422:
                raise ApplicationError(response.text, type="BusinessError", non_retryable=True)
            response.raise_for_status()
            return response.json()
    except ApplicationError: raise
    except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as e:
        raise ApplicationError(str(e), type="DownstreamError") from e

@activity.defn
async def reserve(x): return await post("inventory", "reserve", x)
@activity.defn
async def fulfill(x): return await post("wms", "fulfill", x)
@activity.defn
async def ship(x): return await post("tms", "shipments", x)
@activity.defn
async def release(x): return await post("inventory", "release", x)
@activity.defn
async def cancel_wms(x): return await post("wms", "cancel", x)
@activity.defn
async def cancel_tms(x): return await post("tms", "shipments/cancel", x)
@activity.defn
async def mark_complete(x): return await post("oms", f"orders/{x['order_id']}/complete", x)
@activity.defn
async def mark_failed(x): return await post("oms", f"orders/{x['order_id']}/fail", x)
