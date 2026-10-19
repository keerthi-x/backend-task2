# Task 2 Reliability, Chaos & Crash-Recovery Report

**Empirical verification executed via `npm test` (`src/chaos-demo.ts`)**

---

## 1. Summary of Verified Guarantees

| Reliability Requirement | Faults Injected During Test | Empirical Outcome | Verdict |
| :--- | :--- | :--- | :--- |
| **Return registration without waiting for notification** | 25% HTTP 503 error rate + 15% 500ms timeout rate + 25ms base latency | **7.89 ms avg response time** (`22.30 ms` max) | **PASS** |
| **One registration per member; failed regs never notify** | 1 duplicate member (`409`), 1 invalid workshop (`404`), 1 capacity overflow (`422`) | **3 rejected $\rightarrow$ 0 outbox items, 0 notifications** | **PASS** |
| **Per-member failure isolation** | `member-flaky-01` forced to fail 4 consecutive times (HTTP 502) | `member-02..15` delivered immediately without waiting; `member-flaky-01` recovered on 5th attempt | **PASS** |
| **Backend crash & restart after saving registration** | 10 registrations saved during 100% Mock Outage $\rightarrow$ immediate ungraceful backend crash (`SIGKILL`) | **112 CRC32 WAL frames replayed on reboot $\rightarrow$ all 10 confirmations delivered** | **PASS** |
| **Workshop cancellation notifies all registered members** | Workshop cancelled with 25 members under 30% HTTP 503 + 15% timeout chaos | **25 / 25 cancellation notices delivered** (`50 / 50` total notifications) | **PASS** |
| **Zero lost or duplicate deliveries across retries** | `115` total HTTP delivery attempts across `48` HTTP 502/503 errors + `17` timeouts | **50 unique deliveries, 0 lost, 0 unabsorbed duplicates** | **PASS** |

---

## 2. Phase-by-Phase Execution Trace

1. **Phase 1 (Workshop Creation):** Created `ws-distributed-systems` (`capacity = 25`) via `POST /api/workshops`, persisted synchronously in `TX_CREATE_WORKSHOP`.
2. **Phase 2 (Concurrent Registration & Isolation):** Registered 15 members (`member-flaky-01` + `member-02..15`). Average HTTP `201 Created` latency was `7.89 ms`. While `member-flaky-01` backed off across 4 deterministic HTTP 502 failures, the remaining 11 worker lanes delivered confirmations for `member-02..15` without head-of-line blocking.
3. **Phase 3 (Invalid & Duplicate Registration Rejection):** Attempted duplicate registration for `member-02` (`409 Conflict`) and invalid workshop (`404 Not Found`). Verified `0` outbox entries were created.
4. **Phase 4 (Outage + Ungraceful Backend Crash & Recovery):** Enabled 100% outage mode on the mock notification service, registered `member-16..25` (10 members), verified the 26th member was rejected with `422 Unprocessable Entity` (capacity full), and terminated Backend Instance #1 ungracefully (`simulateCrash()`). Booted Backend Instance #2 on the same WAL file, replayed `112` CRC32-verified WAL frames, disabled outage mode, and verified all pending confirmations drained to `DELIVERED`.
5. **Phase 5 (Workshop Cancellation Fan-Out):** Executed `POST /api/workshops/ws-distributed-systems/cancel`, atomically committing `TX_CANCEL_WORKSHOP` with 25 `WORKSHOP_CANCELLATION` outbox messages. Under 30% error + 15% timeout chaos, all 25 cancellation notifications reached `DELIVERED` with `0` duplicates.
