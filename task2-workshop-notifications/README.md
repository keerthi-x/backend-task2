# GDG Tech Wildcard Entry — Task 2: Workshop Registration and Notifications

A fault-tolerant, zero-external-runtime-dependency **TypeScript** backend comprising a **Workshop Registration & Transactional Outbox Service** and a **Chaos-Injecting Mock HTTP Notification Service**.

Built in strict compliance with **General Submission Guideline #4** (*"Do not replace required core logic with a library when the task explicitly requires a custom implementation"*). Every reliability primitive—CRC32 Write-Ahead Logging, ACID Transactional Outbox commits, lease-based concurrent worker lanes, Full-Jitter Exponential Backoff, per-member failure isolation, and `Idempotency-Key` deduplication—is implemented from scratch in clean TypeScript.

---

## Submission Links (Fill Before Submitting)

- **GitHub Repository URL:** `__________________________________________________`
- **Live Interactive Demo / Deployment URL (Optional):** `__________________________________________________`

---

## 1. Prerequisites, Installation & Configuration

### Prerequisites
- **Node.js** `>= 18.0.0` (tested on Node.js `v18.20.4`)
- No external message broker (RabbitMQ, Kafka, Redis/BullMQ) is required; durability and crash recovery are handled by the custom CRC32 Write-Ahead Log (`src/shared/wal-engine.ts`).

### Installation & Build
```bash
# Clone the repository
git clone <YOUR_TASK_2_GITHUB_REPO_URL>
cd task2-workshop-notifications-ts

# Compile TypeScript source files (src/**/*.ts -> dist/**/*.js)
npm run build
```

### Configuration (Environment Variables)
| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `4003` | Workshop Registration HTTP API listening port |
| `MOCK_PORT` | `4002` | Chaos-Injecting Mock HTTP Notification Service listening port |

---

## 2. Execution & End-to-End Chaos Demonstration Instructions

### Run the Automated Chaos, Outage, Cancellation & Crash-Recovery Demonstration
```bash
npm test
# Alias: npm run demo
```
This automated demonstration script (`src/chaos-demo.ts`) executes all required scenarios end-to-end and saves a JSON audit report to `data/task2-chaos-results.json`:
1. **Workshop Creation & Non-Blocking Registration:** Creates workshop `ws-distributed-systems` (`capacity = 25`) and registers 15 members while the Mock Notification Service injects **25% HTTP 503 errors**, **15% network timeouts (`500ms`)**, and **4 forced consecutive HTTP 502 failures on `member-flaky-01`**. Verifies registration API responses return in **`7.89 ms` average** without waiting for notification delivery, and proves `member-flaky-01` never blocks `member-02..member-15`.
2. **Duplicate & Invalid Registration Filtering:** Verifies duplicate registration (`member-02` registering twice $\rightarrow$ `409 Conflict`), non-existent workshop registration (`404 Not Found`), and capacity overflow (`26th` member $\rightarrow$ `422 Unprocessable Entity`) are rejected atomically and produce **`0` confirmations**.
3. **100% Notification Outage + Simulated Backend Crash (`SIGKILL`) & Restart:** Switches the Mock Notification Service into **100% Outage Mode (`HTTP 503`)**, saves 10 new registrations (`member-16..member-25`), and immediately terminates Backend Instance #1 ungracefully (`simulateCrash()`). Boots Backend Instance #2 attached to the same WAL file, replays **112 CRC32-verified WAL frames**, restores the mock service, and verifies all 10 pending confirmations are delivered automatically.
4. **Workshop Cancellation Fan-Out Under Chaos:** Cancels the workshop (`POST /api/workshops/:id/cancel`), atomically queuing 25 `WORKSHOP_CANCELLATION` notifications while 45% combined error/timeout chaos is active, and verifies **50 / 50 (`100%`) unique notifications** are delivered with **0 lost messages and 0 duplicate deliveries**.

### Start Services Separately for Manual Testing
```bash
# Terminal 1: Start Chaos Mock HTTP Notification Service (:4002)
npm run start:mock

# Terminal 2: Start Workshop Registration & Outbox API Server (:4003)
npm run start:api
```

---

## 3. REST API Reference

### Workshop Registration Service (`:4003`)
- `POST /api/workshops` — Create a workshop (`{ "id": "ws-1", "title": "System Design", "organizer": "GDG", "capacity": 25 }`)
- `GET /api/workshops` — List all workshops and registration counts
- `POST /api/workshops/:id/register` — Register a member (`{ "memberId": "m-01", "memberName": "Aarav", "memberEmail": "aarav@campus.edu" }`) — returns `201 Created` immediately with `{ registration, notificationStatus: "PENDING", idempotencyKey }`
- `POST /api/workshops/:id/cancel` — Cancel a workshop (`{ "reason": "Venue unavailable" }`) and atomically fan out cancellation notifications to every registered member
- `GET /api/outbox` — Inspect transactional outbox queue (`PENDING`, `IN_FLIGHT`, `DELIVERED`, `DEAD_LETTER`)
- `GET /api/health` — Inspect WAL frame counters and outbox delivery telemetry

### Mock HTTP Notification Service (`:4002`)
- `POST /api/notify` — Receives notification payloads (`Idempotency-Key` header deduplicated; randomly returns `200 OK`, `502/503 Error`, or hangs to simulate network timeout)
- `GET /api/mock/stats` — View `totalRequestsReceived`, `simulated503Errors`, `simulatedTimeouts`, `uniqueDeliveries`, and `idempotentDuplicateReplays`
- `POST /api/mock/chaos` — Dynamically update chaos settings (`{ "errorRate": 0.3, "timeoutRate": 0.15, "outageMode": false }`)

---

## 4. How the Design Handles Lost Notifications, Retries, Duplicate Delivery & Backend Restarts

| Design Challenge | Architectural Mechanism (`src/transactional-outbox-engine.ts`) |
| :--- | :--- |
| **1. Non-Blocking Registration** | `registerMember()` never performs network I/O to the notification service inline. It validates constraints in memory, writes a single atomic WAL frame (`TX_REGISTER_MEMBER`), returns HTTP `201` in **`7.89 ms` avg**, and triggers the background worker via `setImmediate()`. |
| **2. Failed Registrations Never Notify** | Uniqueness (`${workshopId}::${memberId}`), workshop status (`OPEN` vs `CANCELLED`), and capacity checks run synchronously before appending to the WAL. Rejected requests (`409`, `404`, `422`) abort before any `OutboxMessage` is created. |
| **3. Backend Restarts & Lost Notifications** | Both the `Registration` and the `PENDING` `OutboxMessage` are persisted inside a single atomic, CRC32-checksummed WAL frame (`TX_REGISTER_MEMBER`) via synchronous write (`fs.writeSync` + `fsync`) before returning HTTP `201`. If the backend crashes 1ms after saving a registration—or mid-flight while waiting for an HTTP response—`recoverFromWal()` replays all valid frames on startup and resets any `IN_FLIGHT` messages to `PENDING`. |
| **4. Retries & Per-Member Failure Isolation** | The outbox worker pool operates `12` concurrent non-blocking HTTP delivery lanes with strict per-request socket timeouts (`req.setTimeout(120ms)`). Pending items are fair-scheduled by `attempts ASC, nextAttemptAtMs ASC`. When one member's notification fails or times out, only that item is backed off using **Full-Jitter Exponential Backoff** (`base/2 + random(0, min(maxCap, base * 1.6^attempt))`), so a failing member never blocks other members. |
| **5. Duplicate Delivery Prevention** | Every outbox message generates a deterministic key (`confirm:<workshopId>:<memberId>` or `cancel:<workshopId>:<memberId>`) sent in the `Idempotency-Key` HTTP header. If the mock service processes a request but the response times out or the backend crashes before logging `OUTBOX_DELIVERED`, the retry presents the identical `Idempotency-Key` and is absorbed idempotently by the receiver. |
| **6. Workshop Cancellation Fan-Out** | `cancelWorkshop()` writes a single atomic `TX_CANCEL_WORKSHOP` WAL frame that marks the workshop `CANCELLED` and enqueues a `WORKSHOP_CANCELLATION` outbox record for every registered member. |

See [`RELIABILITY_AND_RECOVERY_REPORT.md`](./RELIABILITY_AND_RECOVERY_REPORT.md) for the full empirical chaos test log.
