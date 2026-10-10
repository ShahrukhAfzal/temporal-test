# Temporal Python examples

This project contains a small greeting example and a multi-step order example.

The order example uses one parent and four child workflows. Three independent
children run in parallel, and shipping begins only after all three succeed:

```text
OrderWorkflow
├── validate_order activity
├── parallel stage
│   ├── PaymentWorkflow
│   │   └── charge_payment activity
│   ├── InventoryWorkflow
│   │   └── reserve_inventory activity
│   └── CustomerVerificationWorkflow
│       └── verify_customer activity
├── wait for payment_completed signal
├── ShippingWorkflow (after the parallel stage)
│   └── create_shipment activity
└── send_confirmation activity
```

Set `simulate_shipping_failure=True` on an `OrderRequest` to see the shipping
activity fail and retry three times. Normal sample orders leave it disabled.

## Set up

The virtual environment and Python SDK are installed in `.venv`. A project-local
Temporal CLI is installed in `.tools/temporal` (and ignored by Git).

Start a local Temporal development server in a separate terminal:

```powershell
.\.tools\temporal\temporal.exe server start-dev
```

Then start the worker:

```powershell
.\.venv\Scripts\python.exe -m temporal_app.worker
```

In another terminal, start the order workflow. It prints its generated workflow
ID and then durably waits for payment confirmation:

```powershell
.\.venv\Scripts\python.exe -m temporal_app.order_starter
```

Copy the workflow ID and expected payment ID from that terminal. In a fourth
terminal, send the signal (replace the sample values with the printed ones):

```powershell
.\.venv\Scripts\python.exe -m temporal_app.signal_payment order-ab12cd34 pay-order-ab12cd34
```

The starter terminal then prints the completed result. To exercise the same
flow with one client, ask it to signal automatically after two seconds:

```powershell
.\.venv\Scripts\python.exe -m temporal_app.order_starter --auto-signal-after 2
```

Open Temporal UI at `http://localhost:8233`, select the order workflow, and
inspect its Event History. Before signaling, the workflow remains Running and
its `payment_status` query returns `WAITING_FOR_PAYMENT_CONFIRMATION`. After the
signal, look for `WorkflowExecutionSignaled`, followed by the shipping child
workflow events and final completion. The payment, inventory, and customer
verification child workflows still appear as parallel child executions before
the signal gate.

Or run the smaller greeting workflow:

```powershell
.\.venv\Scripts\python.exe -m temporal_app.starter
```

Expected output:

```text
Hello, Temporal!
```

The sample order is defined in `temporal_app/order_starter.py`. It generates a
new order ID each time so you can run it repeatedly against the same server.

## Test

The test uses Temporal's ephemeral test server, so it does not need a separately
running local server:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -v
```
