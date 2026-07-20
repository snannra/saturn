# Saturn

A distributed delayed job scheduler built in Rust.

Saturn accepts jobs over HTTP, persists them durably, schedules them for a future execution time, and runs them across a pool of workers that can crash, hang, or be killed at any moment without losing work or running a job twice concurrently. It sustains 53,000 job submissions per second on a single machine while acknowledging every request only after its job is durably committed.

I built Saturn to learn what it actually takes to make distributed infrastructure correct and fast, not just functional. The interesting parts are not the happy path. They are what happens when a worker dies mid job, when the disk becomes the bottleneck, and when two processes both believe they own the same work.

---

## Architecture

```text
                    Client
                       │
                       ▼
                  NGINX Load Balancer
                       │
          ┌────────────┴────────────┐
          │            │            │
        API 1        API 2        API 3
          │            │            │
          └────────────┬────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
   PostgreSQL                    Redis
 (Source of Truth)     (Scheduling Index + Streams)
        ▲                             ▲
        │                             │
   Fault Tolerance               Scheduler
    (3 sweepers)                      │
        │                             ▼
        │                    Redis Stream (ready_jobs)
        │                             │
        └──────────────┬──────────────┘
                       │
        ┌──────────────┴──────────────┐
        │              │              │
     Worker 1      Worker 2      Worker N
   (consumer group, leases, fencing)
```

| Service         | Responsibility                                                        |
| --------------- | --------------------------------------------------------------------- |
| API             | Accepts submissions, batches inserts, acknowledges after durable commit |
| Scheduler       | Moves due jobs from the sorted set into the execution stream          |
| Worker          | Claims, executes, retries, and completes jobs under a renewable lease |
| Fault Tolerance | Three sweepers, one per non terminal state, that repair stuck jobs    |
| Migrations      | Applies schema migrations before startup                              |

PostgreSQL is the source of truth for every job's state. Redis is a rebuildable index: a sorted set orders delayed jobs by execution time, and a Redis Stream with a consumer group distributes ready jobs to workers. If Redis loses data, the sweepers reconstruct the index from PostgreSQL. Nothing about correctness depends on Redis being durable.

---

## The Performance Story

This is the part of the project I learned the most from, so it gets its own section. Every number below is measured, not estimated.

**Starting point: 2,600 req/sec.** One INSERT per request, one commit per INSERT. Latency was around 23ms per request and I assumed Postgres was slow.

**Instrumentation first.** I added Prometheus histograms for every stage: connection pool acquire time, insert time, Redis time, total time. The split immediately showed that 19 of the 23 milliseconds were spent waiting for a pool connection, not executing the insert. The insert itself was 2.5ms. The database was mostly idle. The queue in front of it was the problem.

**The load balancer was eating throughput.** Working through the layers with Little's Law (in flight = throughput x latency) showed the server was loafing while the benchmark maxed out. The cause was NGINX opening a fresh TCP connection to the backend for every proxied request. Enabling upstream keepalive (proxy_http_version 1.1, an empty Connection header, and a keepalive pool) took throughput from about 4,000 to 13,800 req/sec.

**The real ceiling was the disk.** pg_test_fsync measured the disk at 413 fsyncs per second, 2.4ms each. Every commit requires an fsync. At 16,000 requests per second the system was demanding roughly 32,000 commits per second (each request did an insert plus a bookkeeping update) against a disk that could confirm 413. The only reason it worked at all is that Postgres internally groups concurrent commits onto shared fsyncs, packing roughly 80 transactions per flush. The queue of transactions waiting for the next flush was the latency.

**Group commit batching.** If the scarce resource is durable confirmations per second, the fix is to spend fewer of them. Request handlers now send their row into a channel along with a oneshot reply handle. A single flusher task collects up to 200 rows or waits at most 5ms, writes them all in one multi row INSERT via UNNEST, commits once, and then resolves every oneshot. Each client is still only acknowledged after its data is durably committed, so durability is unchanged. The commit rate dropped from tens of thousands per second to a few hundred, comfortably inside the disk's budget.

**Result: 53,128 req/sec at 6.9ms p50.** Twenty times the starting throughput, with lower latency than the original system had at one twentieth the load, and the exact same durability guarantee. No Postgres tuning, no hardware changes. The bottlenecks, in the order they were found: a proxy default, the measurement harness itself, and a physical property of the disk.

The bookkeeping update that marked jobs as indexed in Redis got the same treatment: instead of one transaction per request, job ids flow into a second channel and a background task flushes them with a single UPDATE WHERE id = ANY($1) every 100ms.

---

## Failure Semantics

Saturn provides at least once execution with at most one active executor per job. Exactly once execution is not possible in a distributed system and Saturn does not pretend otherwise. Instead, every mechanism is built so that duplicate delivery is safe and duplicate execution is prevented while a worker is alive and detected quickly when it is not.

### Job lifecycle

```text
pending ──> queued ──> executing ──> complete
   ▲                       │
   │                       ├──> pending      (retry with backoff)
   │                       └──> failed       (dead letter)
   └── retries re-enter the normal pipeline
```

| Transition            | Performed by | Protected by                                  |
| --------------------- | ------------ | --------------------------------------------- |
| pending to queued     | Scheduler    | Conditional UPDATE (status = 'pending')       |
| queued to executing   | Worker       | Conditional UPDATE claim (status = 'queued')  |
| executing to complete | Worker       | Fenced UPDATE (attempt_id + claimed_by match) |
| executing to pending  | Worker       | Fenced UPDATE, backoff applied                |
| executing to failed   | Worker       | Fenced UPDATE, dead letter                    |

### Leases and fencing

When a worker claims a job it writes its node id, a fresh attempt id, and a lease expiry, and increments the attempt counter, all in one conditional UPDATE. While executing, a background task renews the lease every 5 seconds. Every subsequent write the worker makes, renewal, completion, retry, or dead letter, is conditional on the same node id and attempt id still being on the row.

This matters because a worker that looks dead might not be. It might be paused, slow, or partitioned. When its lease expires and a sweeper hands the job to someone else, the original worker may eventually wake up and try to write results. The fence rejects that write: the UPDATE matches zero rows and the zombie learns it lost ownership. If the renewal task detects the loss first, it fires a cancellation token that aborts the execution mid flight, and the worker walks away without writing anything.

### Retries and dead lettering

Every trip through a worker costs an attempt, incremented at claim time so that jobs which crash their workers spend the same budget as jobs that fail politely. A failed job is not retried by special machinery. It is simply turned back into a scheduled job: status back to pending, scheduled_for pushed out by exponential backoff with jitter (capped at 5 minutes), and its Redis index marker cleared so the normal pipeline picks it up again. Retry is just rescheduling.

Workers capture three kinds of failure: handlers that return an error, handlers that panic (isolated via a spawned task so a panicking job cannot kill the worker), and handlers that hang (a timeout manufactures the verdict, since lease renewal measures worker liveness, not job progress, and is blind to hangs). Errors are classified as retryable or permanent. Permanent failures and exhausted attempt budgets go straight to a failed state with the last error recorded, where a human can inspect and requeue them.

### The sweepers

Every non terminal state has exactly one recovery owner:

| Stuck state                          | Cause                                  | Sweeper                                            |
| ------------------------------------ | -------------------------------------- | -------------------------------------------------- |
| pending, not indexed in Redis        | Redis write failed, or a retry         | Re adds to the sorted set, marks indexed           |
| queued, no stream message            | Crash between transition and enqueue   | Re enqueues to the stream                          |
| executing, lease expired             | Worker died or was partitioned         | Resets to queued, re enqueues                      |

All sweeper actions are idempotent. Duplicate stream messages are absorbed by the claim check: a worker that claims a message for a job that is not in the queued state simply acknowledges the stale message and moves on.

The scheduler itself is crash safe by ordering rather than by cleanup. It reads due jobs without removing them, transitions them with a conditional UPDATE, enqueues them to the stream, and only then removes them from the sorted set. A crash at any point leaves the job still in the sorted set, and the next pass retries harmlessly because the conditional UPDATE arbitrates.

### Graceful shutdown

Crash safety is the correctness mechanism. Graceful shutdown is a cost optimization so that routine deploys do not pay crash prices. On SIGTERM the API stops accepting connections, in flight requests complete, the batch channels close as the last senders drop, and the flushers write their final batches before exit. Workers finish their current job to a fenced verdict and claim no more. The scheduler completes its current pass. All of this is verified the blunt way: kill -9 mid load and watch the sweepers recover everything, then SIGTERM mid load and watch nothing need recovery.

---

## What Saturn Guarantees

* A successful API response means the job is durably committed to PostgreSQL. Not queued in memory, not probably written. Committed.
* At least once execution: every accepted job eventually runs, survives worker crashes, Redis data loss, and scheduler restarts.
* At most one active executor: two workers never concurrently hold a valid claim on the same job.
* Stale writers cannot corrupt state: every worker write is fenced by attempt id.
* Bounded retries with exponential backoff, then dead lettering with the failure recorded.

## What Saturn Does Not Guarantee

* Exactly once execution. A worker can die after performing a job's side effects but before recording completion, and the job will run again. Handlers must tolerate re execution.
* Exactly once submission. A client that times out after the commit but before the response may retry and create a duplicate job. Idempotency keys are the fix and are on the roadmap.
* Ordering. Jobs scheduled for the same instant may execute in any order.
* Precise scheduling. A job runs at or shortly after its scheduled time, not exactly at it. Sweeper and scheduler polling add up to a few seconds of slack.

Being explicit about the second list is the point. Systems that only advertise their guarantees are hiding their contract.

---

## Design Decisions Worth Explaining

**Why Postgres arbitrates ownership instead of Redis Streams alone.** Consumer groups and the pending entries list handle delivery, but delivery tracking is not execution tracking. XAUTOCLAIM will hand an idle message to a second worker while the first is still alive and running it, because idle time is a guess about death and the guess is sometimes wrong. The conditional claim in Postgres is the mutual exclusion that message metadata cannot provide, and the attempt id fence covers the case where the guess was wrong in the other direction.

**Why there are no node heartbeats.** An earlier version registered nodes and heartbeated to a table. I removed it. Per job leases already detect failure, and at a 10 second lease they detect it faster than a 15 second heartbeat check would. Two liveness detectors that can disagree is a distributed systems smell, not a feature. Node level heartbeats earn their place when leases are long or when drain orchestration is needed, and neither applies here.

**Why retry is rescheduling.** A failed job re enters the same pipeline every new job flows through, just with a future timestamp. No retry queue, no special machinery, and the backoff logic is one SQL expression. The recovery path, the retry path, and the normal path are the same path with three entrances.

**Why the API acknowledges after batch commit rather than batching fire and forget.** Fire and forget batching trades durability for throughput. Group commit trades a few milliseconds of latency for the same throughput and keeps the durability. Holding the request open until its batch commits means the acknowledgment still means what it says.

---

## Observability

Prometheus metrics cover the full pipeline: batch size (the packing factor), batch flush latency, insert wait time as seen by clients, channel backpressure, Redis write latency, retries, dead letters, and per sweeper recovery counters. A recovery counter moving in steady state means something upstream is broken, which makes the sweepers double as the alerting layer. Dashboards are in Grafana.

The single most useful metric during development was pool acquire time as its own histogram. It is the difference between "the database is slow" and "we are standing in line", which are different problems with different fixes.

---

## Technology

Rust, Tokio, Axum, SQLx, PostgreSQL, Redis (sorted sets and Streams), Docker Compose, NGINX, Prometheus, Grafana.

---

## Running

```bash
docker compose run --rm saturn-migrations

docker compose up \
    --scale saturn-api=3 \
    --scale saturn-worker=3
```

The API is exposed through NGINX on port 8000. Direct API access for load testing bypasses the proxy on the published port.

```bash
# submit a job
curl -X POST localhost:8000/createjob \
  -H 'Content-Type: application/json' \
  -d '{"user": {"username": "sohan"}, "job": {"task": "send_email"}, "scheduled_for": "2026-07-20T12:00:00Z"}'
```

---

## Roadmap

Deliberately deferred, with reasons:

* **Scheduler failover.** Currently one scheduler with supervised restart and an alert on oldest undispatched job age. The upgrade path is a lease based leader election in Postgres using the same lease and fencing pattern the workers already use. Deferred because restart plus alerting covers the failure at this scale.
* **Idempotency keys** for duplicate submission protection.
* **Postgres high availability.** On managed infrastructure this is synchronous replication, which adds latency per commit. The group commit design is what makes that affordable: the cost lands per batch, not per job.
* **API authentication, rate limiting, and payload limits.** The API currently trusts its callers, which is fine for a system with one caller.

---

## What Building This Taught Me

The bottleneck is almost never where intuition points. It was a proxy config, then the benchmark harness itself, then the physics of an fsync. Metrics with a decomposition you can cross check (stage times that sum to the total) beat intuition every time.

Durability has a budget, measured in fsyncs per second, and architecture decides how you spend it.

You cannot detect death in a distributed system, only the expiry of a promise to stay alive. Everything correct flows from designing for that: leases instead of liveness, fences instead of trust, and the discipline to walk away from work you no longer own.