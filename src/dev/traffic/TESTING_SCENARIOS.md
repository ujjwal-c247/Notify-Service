# Traffic Simulator & Testing Guide

This guide contains predefined `curl` commands to easily test the Notification & Booking system under various scenarios. Ensure your development server is running (`npm run start:dev`) before executing these commands.

---

## 1. Simple Smoke Test (Small Traffic)
Generates 10 bookings over 10 seconds to ensure the basic confirmation pipeline is working.
**Using cURL:**
```bash
curl -X POST http://localhost:3000/api/dev/traffic/start \
-H "Content-Type: application/json" \
-d '{
  "mode": "normal",
  "rate": 1,
  "durationSeconds": 10,
  "cancellationRate": 0
}'
```
**Using NPM CLI Script:**
```bash
npm run traffic:test -- --mode=normal --rate=1 --duration=10 --cancellationRate=0
```

## 2. Reminder Testing
Tests the scheduled cron reminders. Books appointments for exactly 7 minutes in the future. Because reminders trigger 5 minutes before the booking, the reminder email will fire in exactly **2 minutes**.
**Using cURL:**
```bash
curl -X POST http://localhost:3000/api/dev/traffic/start \
-H "Content-Type: application/json" \
-d '{
  "mode": "burst",
  "rate": 1,
  "durationSeconds": 10,
  "cancellationRate": 0,
  "startAtOffsetMs": 420000
}'
```
**Using NPM CLI Script:**
```bash
npm run traffic:test -- --mode=burst --rate=1 --duration=10 --startAtOffsetMs=420000
```

## 3. High Volume Burst Testing
Blasts 200 booking requests immediately (in a burst). Useful for ensuring Kafka and BullMQ properly queue large spikes of traffic without crashing or losing data.
**Using cURL:**
```bash
curl -X POST http://localhost:3000/api/dev/traffic/start \
-H "Content-Type: application/json" \
-d '{
  "mode": "burst",
  "rate": 200,
  "durationSeconds": 1,
  "cancellationRate": 0
}'
```
**Using NPM CLI Script:**
```bash
npm run traffic:test -- --mode=burst --rate=200 --duration=1 --cancellationRate=0
```

## 4. Cancellation & Stale Event Testing
Creates bookings but cancels 50% of them with a slight delay. This ensures our **Stale Event Protection** works correctly.
**Using cURL:**
```bash
curl -X POST http://localhost:3000/api/dev/traffic/start \
-H "Content-Type: application/json" \
-d '{
  "mode": "mixed",
  "rate": 5,
  "durationSeconds": 10,
  "cancellationRate": 50,
  "delayMs": 500
}'
```
**Using NPM CLI Script:**
```bash
npm run traffic:test -- --mode=mixed --rate=5 --duration=10 --cancellationRate=50 --delayMs=500
```

## 5. Email Provider Outage Testing (Failure Mode)
First, configure the mock email provider to fail 100% of emails to test BullMQ retry logic and the Dead Letter Queue.
*(Only configurable via curl endpoint directly at this time)*
```bash
curl -X POST http://localhost:3000/api/dev/traffic/config \
-H "Content-Type: application/json" \
-d '{
  "failureRate": 100
}'
```
Then run a burst test. Watch the BullMQ worker attempt to send, fail, and enter exponential backoff `RETRYING` state.
**Using NPM CLI Script:**
```bash
npm run traffic:test -- --mode=burst --rate=20 --duration=2
```
*(Don't forget to set `"failureRate": 0` afterwards to restore normal behavior).*

---

## Direct Verification Endpoints (Unit Tests via API)
If you want to run the hardcoded internal validation tests without simulating HTTP traffic, use these GET endpoints:

**Verify Duplicate Event Deduplication:**
```bash
curl http://localhost:3000/api/dev/traffic/verify/dedupe
```
**CLI Equivalent:** `npm run traffic:test -- --verify=dedupe`

**Verify Stale Notification Handling:**
```bash
curl http://localhost:3000/api/dev/traffic/verify/stale
```
**CLI Equivalent:** `npm run traffic:test -- --verify=stale`

**Verify Reminder Cron Logic (Simulated):**
```bash
curl http://localhost:3000/api/dev/traffic/verify/reminder
```
**CLI Equivalent:** `npm run traffic:test -- --verify=reminder`
