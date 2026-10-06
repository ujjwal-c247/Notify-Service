# Email Queue & Worker

**Folders:** `src/infrastructure/queue/` & `src/workers/`

Once the `NotificationService` has deduplicated the event and saved a `Notification` to the DB, it offloads the actual heavy lifting of sending the email to Redis via BullMQ.

## Important Logic & Conditions

### 1. Redis Queue (BullMQ)
**File:** `email.queue.ts`

When adding a job to BullMQ, we set `attempts` and `backoff` options.
By default, the job will retry 5 times with exponential backoff (e.g. waits 1s, then 2s, then 4s) if the email provider goes offline.

### 2. The Worker Lifecycle
**File:** `email.worker.ts`

The `EmailWorker` constantly listens to Redis. When it gets a job:
1. It calls `NotificationService.processEmailJob()`.
2. That method does the "Stale Event Check" (described in the previous doc).
3. If valid, it invokes the `EmailProvider`.

### 3. Handling Provider Failures
**File:** `notification.service.ts`

If the `EmailProvider` throws a network error:
**Condition:** We check if the error is "Retryable" (like a 503 Service Unavailable) or "Permanent" (like a 400 Bad Request / Invalid Email).
*   **If Permanent:** We immediately mark the DB Notification as `FAILED` (Dead Letter Queue) and tell BullMQ not to bother retrying.
*   **If Retryable:** We mark the DB Notification as `RETRYING` and throw the error back to BullMQ. BullMQ will automatically put the job back into the `delayed` queue and try again later.

### 4. Mock Provider (Testing)
**File:** `mock-email.provider.ts`

To test this retry logic locally, we use a `MockEmailProvider` that can be configured to randomly fail a certain percentage of the time (e.g. 30% failure rate). This proves that BullMQ actually catches the errors and retries successfully.
