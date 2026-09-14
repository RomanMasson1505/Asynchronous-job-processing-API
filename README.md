# jobapi — Asynchronous job processing API

An HTTP API that accepts tasks, queues them, and runs them in the background
on a pool of goroutines. The client never waits: it gets an identifier
immediately and checks its job's state whenever it wants.

**Go standard library only.** No dependencies, no framework — `go.mod` does not
contain a single `require` line.

## Why

A synchronous HTTP request is the wrong shape for work that takes time: the
client times out, retries, and triggers the same work twice. The classic answer
is to decouple acceptance from execution — the pattern behind Celery, Sidekiq,
BullMQ or SQS + Lambda. This project implements its core by hand.

## Architecture

```
                   HTTP
                    │
        ┌───────────▼───────────┐
        │        api            │  net/http routes, JSON encoding
        └─────┬───────────┬─────┘
              │ Create    │ Get / List / Cancel
        ┌─────▼───────────▼─────┐
        │        store          │  map[string]*Job + sync.RWMutex
        │   (source of truth)   │
        └───────────▲───────────┘
                    │ Start / Finish
              ┌─────┴─────┐
              │  worker   │  N goroutines reading the channel
              └─────▲─────┘
                    │
              chan string (queued IDs)
```

`store` imports nobody. `worker` imports `store`. `api` imports `store`.
`main` wires it all together. No cycles: every package is testable in
isolation.

## Running it

```bash
go run ./cmd/server      # listens on :8080
```

## API

| Method | Route | Response |
|---|---|---|
| `POST` | `/jobs` | `202` + the job in the `queued` state |
| `GET` | `/jobs/{id}` | `200` + the job, `404` if unknown |
| `GET` | `/jobs?status=running` | `200` + `{count, jobs}` |
| `DELETE` | `/jobs/{id}` | `200` if the cancellation was accepted, `404` if unknown or already finished |
| `GET` | `/healthz` | `200` |

Built-in job types: `sleep` (payload `{"ms":5000}`) and `uppercase`
(payload `{"text":"..."}`). Adding one is a single line in `DefaultRegistry`,
without touching the pool.

### Example

```bash
# submit — the response is immediate
curl -X POST localhost:8080/jobs -d '{"type":"sleep","payload":{"ms":5000}}'
# {"id":"4db8ed81757589f8","type":"sleep","status":"queued","created_at":"..."}

# check on it
curl localhost:8080/jobs/4db8ed81757589f8
# {"id":"...","status":"running","started_at":"..."}

# cancel it
curl -X DELETE localhost:8080/jobs/4db8ed81757589f8
# the job turns "canceled" as soon as the handler observes ctx.Done()

# list the running jobs
curl 'localhost:8080/jobs?status=running'
```

A job's lifecycle:

```
queued ──> running ──> succeeded
   │          │
   │          └──────> failed
   └─────────────────> canceled
```

## Implementation notes

- **No internal pointer ever leaves the store.** Every read returns a `Clone()`
  made under the lock. Without it, an HTTP handler could serialize a job while
  a worker is writing to it.
- **`RWMutex`**: reads (the common case) run concurrently, only writes are
  exclusive.
- **Buffered queue**: `POST /jobs` never waits for a worker. When the queue is
  full, the API answers `503` instead of blocking the request.
- **Cooperative cancellation**: `DELETE` triggers the job's `context`; it is up
  to the handler to observe `ctx.Done()`. Nothing is killed by force.
- **Graceful shutdown**: on SIGINT/SIGTERM, the HTTP server is closed first (no
  new work comes in), then the queue is closed and we wait for the workers to
  finish the work already accepted.

## Tests

```bash
go test ./...           # unit tests, layer by layer
go test -race ./...     # + the data race detector
```

The HTTP tests use `httptest`: no port is ever opened. The pool tests wait for
a state by short polling rather than a fixed `Sleep`, which keeps them fast
without making them flaky.

## Known limitations

Storage is in memory: restarting loses the jobs. Since every write goes through
the `Store` methods, putting Redis or Postgres behind the same interface would
not touch any other package. There is no retry on failure either, and no
priorities.
