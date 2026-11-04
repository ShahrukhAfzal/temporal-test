from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
with workflow.unsafe.imports_passed_through():
    from orchestrator import activities

RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2, maximum_interval=timedelta(seconds=10), maximum_attempts=3)

@workflow.defn
class OrderFulfillmentWorkflow:
    def __init__(self): self.step = "STARTED"; self.completed = []; self.compensation = []
    @workflow.query
    def status(self): return {"current_step": self.step, "completed_steps": self.completed, "compensation_results": self.compensation}

    async def call(self, fn, arg):
        return await workflow.execute_activity(fn, arg, start_to_close_timeout=timedelta(seconds=30), schedule_to_close_timeout=timedelta(seconds=100), retry_policy=RETRY)

    @workflow.run
    async def run(self, order):
        order_id = order["order_id"]
        base = {**order, "operation_id": f"{order_id}-"}
        reserved = fulfilled = shipped = False
        try:
            self.step = "INVENTORY_RESERVE"
            r = await self.call(activities.reserve, {**base, "operation_id": f"{order_id}-inventory-reserve"}); reserved = True; self.completed.append("INVENTORY_RESERVED")
            self.step = "WMS_FULFILL"
            w = await self.call(activities.fulfill, {**base, "operation_id": f"{order_id}-wms-fulfill"}); fulfilled = True; self.completed.append("WMS_FULFILLED")
            self.step = "TMS_SHIPMENT"
            s = await self.call(activities.ship, {**base, "operation_id": f"{order_id}-tms-ship"}); shipped = True; self.completed.append("TMS_SHIPPED")
            self.step = "OMS_COMPLETE"
            await self.call(activities.mark_complete, order); self.completed.append("OMS_COMPLETED"); self.step = "COMPLETED"
            return {"order_id": order_id, "status": "COMPLETED", **r, **w, **s}
        except Exception:
            self.step = "COMPENSATING"
            for fn, active, key in [(activities.cancel_tms, shipped, "tms-cancel"), (activities.cancel_wms, fulfilled, "wms-cancel"), (activities.release, reserved, "inventory-release")]:
                if active:
                    try:
                        await workflow.execute_activity(fn, {**base, "operation_id": f"{order_id}-{key}"}, start_to_close_timeout=timedelta(seconds=30), schedule_to_close_timeout=timedelta(seconds=100), retry_policy=RETRY)
                        self.compensation.append({"operation": key, "status": "SUCCEEDED"})
                    except Exception as error: self.compensation.append({"operation": key, "status": "FAILED", "error": str(error)})
            try:
                await self.call(activities.mark_failed, order); self.compensation.append({"operation": "oms-fail", "status": "SUCCEEDED"})
            except Exception as error: self.compensation.append({"operation": "oms-fail", "status": "FAILED", "error": str(error)})
            self.step = "COMPENSATION_INCOMPLETE" if any(x["status"] == "FAILED" for x in self.compensation) else "FAILED_COMPENSATED"
            raise
