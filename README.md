# Saturn

A distributed delayed job scheduler implemented in Rust.

Saturn is an exploration of the design and implementation of fault-tolerant backend infrastructure. The system separates request ingestion, scheduling, execution, and recovery into independent services that coordinate through PostgreSQL and Redis. It is designed around durable job persistence, renewable execution leases, and horizontal scaling of stateless services.

The project focuses on practical distributed systems concepts including durable storage, scheduling coordination, worker ownership, failure recovery, observability, and service decomposition.

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
 (Source of Truth)          (Scheduling Index)
        ▲                             ▲
        │                             │
   Fault Tolerance              Scheduler(s)
        │                             │
        └──────────────┬──────────────┘
                       │
                  Ready Queue
                       │
        ┌──────────────┴──────────────┐
        │              │              │
     Worker 1      Worker 2      Worker N
```

---

## Design Overview

Saturn separates responsibilities into independent services:

| Service           | Responsibility                                                    |
| ----------------- | ----------------------------------------------------------------- |
| API               | Accepts job submissions and persists them durably                 |
| Scheduler         | Moves eligible jobs from delayed storage into the execution queue |
| Worker            | Claims, executes, and completes jobs                              |
| Fault Tolerance   | Detects expired leases and repairs scheduler state                |
| Migration Service | Applies database schema migrations before startup                 |

Each service is independently deployable and horizontally scalable.

---

## Job Lifecycle

1. Client submits a job through the API.
2. The API persists the job in PostgreSQL.
3. The job is indexed into Redis using its scheduled execution time.
4. Scheduler instances atomically claim due jobs from Redis.
5. Claimed jobs are pushed into the ready queue.
6. Workers atomically claim ownership of queued jobs.
7. Workers periodically renew execution leases while processing.
8. Completed jobs are marked complete in PostgreSQL.

---

## Data Model

### PostgreSQL

PostgreSQL serves as the system of record.

It stores:

* Job metadata
* Execution status
* Worker ownership
* Renewable leases
* Node registration
* Heartbeat information

The scheduler and workers derive their execution state from PostgreSQL rather than Redis.

### Redis

Redis is used as a scheduling index rather than durable storage.

Responsibilities include:

* Delayed job ordering using sorted sets
* Ready queue management
* Scheduler coordination

Because Redis is treated as an index, scheduler state can be reconstructed from PostgreSQL if necessary.

---

## Scheduling

Jobs are stored inside a Redis sorted set keyed by execution timestamp.

Scheduler instances periodically:

1. Retrieve jobs whose scheduled time has elapsed.
2. Atomically remove those jobs using a Lua script.
3. Transition them into the execution queue.

Using Lua ensures multiple scheduler instances cannot schedule the same job simultaneously.

Scheduler polling is batched to reduce Redis load and improve latency under concurrent scheduling.

---

## Worker Ownership

Workers claim jobs using a compare-and-set update against PostgreSQL.

Ownership is represented by:

* node_id
* attempt_id
* execution lease

Only the worker that successfully acquires ownership may renew or complete the job.

Completion updates verify ownership before modifying job state, preventing stale workers from completing jobs after ownership has been lost.

---

## Fault Tolerance

Saturn implements lease-based failure recovery.

Each executing job receives an execution lease.

Workers periodically renew the lease while processing.

If a worker terminates unexpectedly or fails to renew before expiration, the fault tolerance service detects the expired lease and safely returns the job to the scheduler.

This design provides automatic recovery without requiring distributed consensus.

Node heartbeats are maintained independently for observability and health monitoring.

---

## Horizontal Scaling

The system is designed for multiple concurrent instances of:

* API servers
* Scheduler services
* Worker processes

API instances are stateless and deployed behind an NGINX load balancer.

Schedulers coordinate using atomic Redis operations.

Workers coordinate through PostgreSQL ownership semantics and renewable leases.

---

## Observability

Saturn exposes Prometheus metrics for:

* API request latency
* PostgreSQL write latency
* Redis write latency
* Job creation throughput
* Scheduler activity
* Worker execution

Metrics can be visualized using Grafana.

---

## Technology Stack

* Rust
* Tokio
* Axum
* PostgreSQL
* Redis
* SQLx
* Docker Compose
* Prometheus
* Grafana
* NGINX

---

## Running

```bash
docker compose run --rm saturn-migrations

docker compose up \
    --scale saturn-api=3 \
    --scale saturn-worker=3 \
    --scale saturn-scheduler=2
```

The API is exposed through NGINX on port 8000.

---

## Current Guarantees

* Durable job persistence
* Atomic worker ownership
* Renewable execution leases
* Automatic recovery of abandoned jobs
* Horizontal API scaling
* Multiple scheduler instances
* Multiple worker instances
* Atomic scheduler coordination
* Prometheus instrumentation

---

## Future Work

Potential extensions include:

* Leader election for scheduler coordination
* PostgreSQL streaming replication
* Redis Sentinel or Cluster deployment
* Exactly-once execution semantics
* Scheduler sharding
* Distributed tracing
* Backoff and retry policies
* Dead-letter queues
* Quorum-based coordination using Raft or etcd

---

## Project Goals

Saturn was built to explore the implementation of production-oriented distributed systems concepts rather than to serve as a task queue library. The project emphasizes correctness, failure recovery, service decomposition, and explicit ownership semantics over framework abstraction.