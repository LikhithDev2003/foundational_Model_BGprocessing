# Reliable iOS Background Processing Under On-Device Model Constraints

**Design doc — React Native iOS work-item processing pipeline**

---

## Governing Principle

> Every unit of work must be safely interruptible at any point, and resumable without re-doing completed work or losing in-flight work.

---

## 1. Architecture

### 1.1 High-Level Flow



Each arrow in this diagram is a change saved to the local database. If the app gets killed between any two boxes, the item's saved status tells us exactly where it stopped.

### 1.2 Database Schema

One table, with status as a column — keeps things simple and avoids extra failure points.

```sql
CREATE TABLE work_items (
  id                  TEXT PRIMARY KEY,
  priority            TEXT NOT NULL,
  created_at          TEXT NOT NULL,
  payload_size_bytes  INTEGER NOT NULL,
  payload             TEXT,
  reduced_payload     TEXT,

  status              TEXT NOT NULL,
  is_high_priority    INTEGER NOT NULL,

  attempt_count       INTEGER NOT NULL DEFAULT 0,
  last_attempt_at     TEXT,
  last_error          TEXT,

  result              TEXT,
  notified_at         TEXT,

  discovered_at       TEXT NOT NULL,
  updated_at          TEXT NOT NULL
);

CREATE INDEX idx_status_priority ON work_items(status, is_high_priority);
CREATE INDEX idx_updated_at      ON work_items(updated_at);
```

- `is_high_priority` is worked out once, at triage time, and saved — not recalculated every time we read the queue. Checks for `priority === "high"` or the word `"URGENT"`.
- `payload` and `reduced_payload` are kept as two separate columns, just to cross-check if we're losing anything while extracting high signal.
- `updated_at` is to spot items stuck mid-way through something for too long.
- One shared table also makes it easy to check for duplicates — before inserting a new `discovered` row, we just check `WHERE id`. If it's already there, we skip the insert.

### 1.3 Background Process Flow

We register two background task types, but both run the same drain function — only the time budget passed in is different.

| Background Task | Execution Characteristics | Primary Purpose |
|---|---|---|
| `BGAppRefreshTask` | Frequent attempts by iOS. Short execution window (~30 seconds). | Primarily drains the existing queue. If very little time is available, sync is skipped — processing already-queued high-priority work is more valuable than discovering new work. |
| `BGProcessingTask` | Opportunistic execution when the device is idle (often while charging). Longer window (typically minutes, not guaranteed). | Performs both Sync (discover new work) and Drain (process queued work), since there's usually enough time for both. |

- We use a single drain loop to keep consistency between the two background tasks.
- We must re-register a new background task every single time, whether the last one succeeded or failed. `BGTaskScheduler` tasks only fire once — if we don't call `BGTaskScheduler.submit()` again before the handler returns, the app won't be woken again this way.

### 1.4 Foreground Process Flow

When the app is opened — freshly launched or resumed from the background — the drain loop starts immediately.

**Notifications:** whether a notification has been sent is tracked with the `notified_at` column, not as one of the states in the processing flow. Sending a notification happens *after* an item is done — it's not a step in processing it.

### 1.5 Swift vs. JS/TypeScript Responsibility Split

Swift talks to iOS; JS decides what to do. Swift has no business logic about triage, retries, or priority.

| Layer | Owns |
|---|---|
| **Swift** | `BGTaskScheduler` registration & submission, task expiration handler wiring, signaling "about to expire" to JS across the bridge, calling `task.setTaskCompleted()` once JS reports back (or on its own hard timeout if JS doesn't respond), native notification delivery if/when triggered |
| **JS/TypeScript** | Sync orchestration, triage, preprocessing orchestration, the drain loop itself, rate-limit tracking, all state machine transitions, all DB reads/writes, retry policy and backoff |

**Handling "task is about to expire":**

1. Swift's expiration handler fires on its own timer, no matter what JS is doing at that moment.
2. Swift tells JS, across the bridge, that expiration is close.
3. JS tries to respond cleanly — cancels any model call in progress, saves the current state, and responds back to Swift.
4. Swift has its own timeout and calls `task.setTaskCompleted(false)` regardless of whether JS responded, because iOS will kill the process on its own schedule either way. The main reason for this: make sure `setTaskCompleted` always gets called.

---

## 2. Queue and State Management

### 2.1 States

![State machine](./images/state-management.png)

| State | Meaning |
|---|---|
| `discovered` | Row inserted from sync; not yet evaluated |
| `queued` | Triaged; priority known; waiting its turn |
| `preprocessing` | `extractHighSignal()` running/about to run |
| `ready_for_model` | Model input is finalized, waiting for a rate-limited slot |
| `model_processing` | A model call is currently in flight |
| `throttled` | Rejected specifically due to a rate limit |
| `completed` | Result durably saved |
| `failed_retryable` | Transient failure, eligible for retry |
| `failed_terminal` | Retries exhausted or unrecoverable failure |
| `skipped` | Never needed the model (`payloadSizeBytes < 50`) |

### 2.2 Transitions

| From | To | Trigger |
|---|---|---|
| — | `discovered` | Sync inserts a new item ID not already present in the table |
| `discovered` | `skipped` | `payloadSizeBytes < 50` |
| `discovered` | `queued` | Triage runs; `is_high_priority` computed and cached |
| `queued` | `preprocessing` | `payloadSizeBytes > 10_000` |
| `queued` | `ready_for_model` | Size is in normal range, no reduction needed |
| `preprocessing` | `ready_for_model` | `extractHighSignal()` returns successfully |
| `preprocessing` | `failed_retryable` | `extractHighSignal()` throws or returns an empty/invalid result |
| `ready_for_model` | `model_processing` | Drain loop selects it (priority order) and the rate limiter allows a call |
| `model_processing` | `completed` | Model call succeeds and the result write to DB succeeds |
| `model_processing` | `throttled` | Model rejects specifically due to rate ceiling |
| `model_processing` | `failed_retryable` | Timeout, cancellation due to expiration, or model-unavailable error |
| `throttled` | `ready_for_model` | Rate limiter's rolling window has room again; `attempt_count` not incremented |
| `failed_retryable` | `ready_for_model` / `preprocessing` | Backoff elapsed and `attempt_count` < max |
| `failed_retryable` | `failed_terminal` | `attempt_count` ≥ max |
| `failed_terminal` | `ready_for_model` | Manual, user-triggered re-queue only (optional UI hook, not automatic) |

- The `payloadSizeBytes < 50` check runs right away, as soon as an item is discovered from the remote source — before it ever enters the queue, and is skipped. This is quick.
- `throttled` doesn't mean failed. It does not count toward `attempt_count`, because here the device made too many model calls and hit the rate ceiling. Once a slot opens up, throttled items go to the front of the queue, ahead of items that haven't been tried at all, since they were already next in line before they got bumped.
- Every other state can resume on its own — if the app dies while something is `queued`, `preprocessing`, or `ready_for_model`, the next run just picks up from there, because the status already says what's next. But `model_processing` is different: looking at the row alone, we can't tell whether the call was about to succeed or got killed in the middle. So every time the drain loop starts, any row stuck in `model_processing` for longer than a model call should ever take (> 3–5 sec) is treated as abandoned and reset to `ready_for_model` to be tried again.
- We need to handle the race condition that could happen between background and foreground tasks, so we use a flag — `currentlyProcessingId` — that's checked and set before any model call, and cleared right after.

---

## 3. Item Prioritisation

### 3.1 Rules

These three checks are simple, fast, and need no network or database calls — everything they need is already in the row from the sync step. So triage can run right away for every `discovered` item, no matter how little time is left.

Instead of one queue sorted by some priority score, we use two:

- **High-priority queue:** `is_high_priority = 1`
- **Normal queue:** `is_high_priority = 0`

The drain loop always empties the high-priority lane first.

If high-priority items keep showing up, normal items could wait a long time. Given how few items the model can process per minute, and how few background windows we get per day, this is a tradeoff in this approach — normal items may wait longer to get processed, but we always prioritise `"URGENT"` items first.

---

## 4. Payload Reduction

`extractHighSignal()` runs as its own step — the `preprocessing` state — strictly between triage and the model, since it can be interrupted separately from the model call's time budget. If the app crashes mid-reduction, recovery only needs to retry the reduction step — no need to re-fetch.

---

## 5. Rate-Limit-Aware Model Processing

The model's limit, as given, is single digits per rolling minute. So we keep a rolling list of recent call times and check it before every call.

We set the ceiling a bit below the maximum limit (~5/min), and we save this limit to our DB to maintain consistency even if the app is killed during background processing and reopened.

One model session is created per drain run, not one per item.

| Model response | State transition | Retry behavior |
|---|---|---|
| Rejected due to rate ceiling | `model_processing` → `throttled` | Returns to `ready_for_model` once rolling window has room; no `attempt_count` increment |
| Times out / unavailable | `model_processing` → `failed_retryable` | Exponential backoff, `attempt_count` incremented, capped at a configured max |
| Returns successfully | `model_processing` → `completed` | — |

---

## 6. Failure Handling

### 6.1 The app is killed while an item is `model_processing`

We recover by checking afterward, since there's no guaranteed signal when iOS kills the app — we can't rely on catching that moment.

### 6.2 Expiration warning arrives with 3 seconds left

- Swift's expiration handler fires and sends a signal to JS.
- If JS is in the middle of a model call, it cancels it right away rather than waiting for it to finish naturally.
- JS saves the item's current state, most likely back to `ready_for_model`, or if there's truly no time even for that, leaves it for the stale-sweep to catch later.
- JS responds back if it manages to finish in time.

### 6.3 The user opens the app after 48 hours

No special handling needed — this is just the regular foreground fallback, but with a bigger backlog than usual. The drain loop doesn't know or care how long items have been waiting; it works through high-priority first, then normal, against the rate limit, for as long as the foreground time allows.

### 6.4 Model returns throttled/unavailable

Throttled goes to `throttled` (doesn't need a retry); unavailable goes to `failed_retryable` (does need a retry, with backoff).

### 6.5 The same item is fetched twice

We prevent duplicates at the point of inserting new rows, using the item's ID. The `WHERE id = ?` check makes the insert naturally safe against duplicates, no matter why they showed up.

### 6.6 JS runtime is not ready when Swift receives a background task

This is our cold-start case.

- Swift receives the background wake immediately — this always happens, since it's native code and doesn't depend on JS being ready.
- Swift checks and starts up the JS runtime explicitly, with its own timeout.
- If JS isn't ready within a short window, Swift doesn't keep calling into JS at all this time. It calls `task.setTaskCompleted(success: false)` and registers the next background task, so we don't miss the next opportunity either.
- If JS does become ready in time, Swift passes along the remaining time allotted.

### 6.7 Database write fails after processing succeeds

This is the main reason for treating `model_processing` → `completed` as one single update.

If this one write fails, the item's status stays at `model_processing`. We just reset the state, even if the model call was successful — it costs us an extra model call rather than marking it as complete when no work would've actually been saved.

---

## 7. Pseudocode

### 7.1 Background Task Registration (Swift)

```swift
func registerBackgroundTasks() {
    BGTaskScheduler.shared.register(forTaskWithIdentifier: "com.app.refresh", using: nil) { task in
        handleBackgroundTask(task as! BGAppRefreshTask, budgetSeconds: 25) // safety margin below ~30s
    }
    BGTaskScheduler.shared.register(forTaskWithIdentifier: "com.app.processing", using: nil) { task in
        handleBackgroundTask(task as! BGProcessingTask, budgetSeconds: 120) // opportunistic, not guaranteed
    }
}

// Called at the END of every task handler — tasks are one-shot, must re-submit each time
func scheduleNextRefresh() {
    let request = BGAppRefreshTaskRequest(identifier: "com.app.refresh")
    request.earliestBeginDate = Date(timeIntervalSinceNow: 15 * 60)
    try? BGTaskScheduler.shared.submit(request)
}

func scheduleNextProcessing() {
    let request = BGProcessingTaskRequest(identifier: "com.app.processing")
    request.requiresNetworkConnectivity = true
    try? BGTaskScheduler.shared.submit(request)
}
```

### 7.2 Swift → JS Handoff, Including Cold-Start (Swift)

```swift
func handleBackgroundTask(_ task: BGTask, budgetSeconds: TimeInterval) {
    var jsResponded = false

    // Swift's own hard fallback — fires no matter what JS is doing
    task.expirationHandler = {
        signalExpiringSoonToJS()
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
            if !jsResponded { task.setTaskCompleted(success: false) }
        }
    }

    ensureJSRuntimeReady(timeoutSeconds: 4.0) { ready in
        guard ready else {
            // Cold-start case (6.6): fail fast, don't risk a half-ready handoff
            task.setTaskCompleted(success: false)
            scheduleNextRefresh()
            return
        }
        callIntoJS_runDrainLoop(deadlineSeconds: budgetSeconds) { result in
            jsResponded = true
            task.setTaskCompleted(success: result.success)
            scheduleNextRefresh()
            scheduleNextProcessing()
        }
    }
}
```

### 7.3 Sync — Fetching New Items (JS)

```typescript
async function syncNewItems() {
  const cursor = await store.getCursor()
  const remoteItems = await remote.fetchNewItemIds({ since: cursor })

  for (const item of remoteItems) {
    const existing = await db.get("work_items", item.id)
    if (existing) continue // duplicate fetch (6.5) — never re-insert

    await db.insert("work_items", {
      id: item.id,
      priority: item.priority,
      payload_size_bytes: item.payloadSizeBytes,
      payload: item.payload,
      status: "discovered",
      attempt_count: 0,
      discovered_at: now(),
      updated_at: now(),
    })
  }

  if (remoteItems.length > 0) {
    await store.advanceCursor(max(remoteItems.map(i => i.createdAt)))
  }
}
```

### 7.4 Triage (JS)

```typescript
function classify(item: WorkItem) {
  if (item.payloadSizeBytes < 50) return "skip"
  if (item.payloadSizeBytes > 10_000) return "needs_reduction"
  return "ready"
}

function isHighPriority(item: WorkItem) {
  return item.priority === "high" || item.payload.includes("URGENT")
}

async function triageDiscoveredItems() {
  const items = await db.query("work_items", { status: "discovered" })

  for (const item of items) {
    const highPriority = isHighPriority(item) ? 1 : 0
    const classification = classify(item)

    const nextStatus =
      classification === "skip" ? "skipped" :
      classification === "needs_reduction" ? "preprocessing" :
      "ready_for_model"

    await db.update(item.id, { status: nextStatus, is_high_priority: highPriority, updated_at: now() })
  }
}
```

### 7.5 Payload Reduction (JS)

```typescript
async function runPreprocessing() {
  const items = await db.query("work_items", { status: "preprocessing" })

  for (const item of items) {
    try {
      const reduced = extractHighSignal(item.payload)
      if (!reduced?.trim()) throw new Error("empty result")

      await db.update(item.id, { status: "ready_for_model", reduced_payload: reduced, updated_at: now() })
    } catch (err) {
      await markFailedRetryable(item.id, String(err))
    }
  }
}

async function markFailedRetryable(id: string, error: string) {
  const item = await db.get("work_items", id)
  const attempts = item.attempt_count + 1
  await db.update(id, {
    status: attempts >= MAX_ATTEMPTS ? "failed_terminal" : "failed_retryable",
    attempt_count: attempts,
    last_error: error,
    updated_at: now(),
  })
}
```

### 7.6 Draining the Queue Under a Time Budget (JS)

```typescript
let currentlyProcessingId: string | null = null // single-flight guard

async function runDrainLoop(deadline: number) {
  await sweepStaleProcessingItems() // recover from any prior crash first

  while (now() < deadline) {
    if (!rateLimiter.canMakeCall(now())) {
      const wait = rateLimiter.msUntilNextSlot(now())
      if (now() + wait >= deadline) break
      await sleep(wait)
      continue
    }

    const next = await selectNextReadyItem() // high-priority lane first
    if (!next) break

    currentlyProcessingId = next.id
    await processOneItem(next, deadline)
    currentlyProcessingId = null
  }
}

async function selectNextReadyItem() {
  return db.queryOne("work_items", {
    status: "ready_for_model",
    orderBy: [["is_high_priority", "DESC"], ["discovered_at", "ASC"]],
  })
}

async function sweepStaleProcessingItems() {
  const stale = await db.query("work_items", {
    status: "model_processing",
    updatedAtBefore: now() - STALE_TIMEOUT_MS, // e.g. 15s
  })
  for (const item of stale) {
    await db.update(item.id, { status: "ready_for_model", attempt_count: item.attempt_count + 1, updated_at: now() })
  }
}
```

### 7.7 Rate-Limited Model Processing (JS)

```typescript
class RollingRateLimiter {
  private calls: number[] = []
  constructor(private ceiling: number, private windowMs = 60_000) {}

  canMakeCall(nowMs: number) {
    this.calls = this.calls.filter(t => nowMs - t < this.windowMs)
    return this.calls.length < this.ceiling
  }

  recordCall(nowMs: number) {
    this.calls.push(nowMs)
    db.setKeyValue("recent_call_timestamps", JSON.stringify(this.calls)) // survives restart
  }
}

let sharedSession: ModelSession | null = null // one per drain run, not per item

async function processOneItem(item: WorkItem, deadline: number) {
  if (!sharedSession) sharedSession = model.newSession()

  const input = item.reduced_payload ?? item.payload
  await db.update(item.id, { status: "model_processing", updated_at: now() })
  rateLimiter.recordCall(now())

  try {
    const result = await withTimeout(sharedSession.process(input), deadline - now(), () => sharedSession?.cancel())
    await db.update(item.id, { status: "completed", result: JSON.stringify(result), updated_at: now() })
  } catch (err) {
    if (isThrottlingError(err)) {
      await db.update(item.id, { status: "throttled", updated_at: now() }) // no attempt_count increment
    } else {
      await markFailedRetryable(item.id, String(err))
    }
  }
}
```

### 7.8 Task Expiration Handling (JS responds to Swift's signal)

```typescript
async function onExpirationWarning() {
  if (currentlyProcessingId) {
    sharedSession?.cancel()
    await db.update(currentlyProcessingId, { status: "ready_for_model", updated_at: now() })
    currentlyProcessingId = null
  }
  reportBackToSwift({ acknowledged: true })
  // No guarantee this finishes before Swift's hard timeout — the stale-sweep is the real safety net
}
```

### 7.9 Foreground Resume Behavior (JS)

```typescript
async function onAppForeground() {
  const deadline = now() + FOREGROUND_BUDGET_MS // e.g. 5 minutes, or queue-empty

  await syncNewItems()
  await triageDiscoveredItems()
  await runPreprocessing()
  await runDrainLoop(deadline)

  emitQueueStatusForUI(await getQueueSummary())
}
```

---

## 8. Validation Plan

Tested on a real iPhone, not the simulator, since background behavior isn't the same on simulator.

**Metrics we track:** queue age, time to complete an item, retry counts, duplicate items caught, how often expiration interrupts us, how many model calls happen per minute, and whether the backlog is growing or shrinking over time.

**Overnight test:** Seed a mix of items (small, large, normal, high priority, a few marked `"URGENT"`) and leave the phone untouched overnight — once plugged in, once not. Next morning, check how many background runs happened, how much got processed, and whether opening the app finishes the rest cleanly.

**Suspension & expiration test:** Use Xcode's debugger to trigger a background task on demand, and temporarily shrink the time budget (e.g. 5 seconds) so expiration happens often during testing. Confirm items interrupted mid-`model_processing` correctly go back to `ready_for_model`, and that `task.setTaskCompleted` always gets called.

**Throttling test:** Seed more items than the per-minute limit allows, run a drain, and check that calls stay under the limit, throttled items don't lose retry attempts, and the throttle detection actually matches real model rejections.

**No loss / no duplication test:** Force-kill the app at different points (mid-sync, mid-preprocessing, mid-model-call) and confirm every item still ends up `completed`, `skipped`, or `failed_terminal` exactly once — nothing missing, nothing processed twice.

