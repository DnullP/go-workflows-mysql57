# go-workflows MySQL 5.7 backend

This package provides a MySQL 5.7-compatible backend for
[`github.com/cschleiden/go-workflows`](https://github.com/cschleiden/go-workflows).

It is a small fork of the upstream `backend/mysql` package. The upstream backend
uses `FOR UPDATE ... SKIP LOCKED`, which is available in MySQL 8+ but not in
MySQL 5.7. This fork replaces workflow and activity task pickup with optimistic
lock claims:

1. Select a small batch of candidate task ids.
2. Claim one row with a conditional `UPDATE` on the queue, lease state, worker,
   and per-lease `lock_token`.
3. Read task details only if the current worker won the claim.

Leases are completed or extended only when the worker name, lock token, workflow
or activity identity, and non-expired lease all match. Lease expiry and renewal
use MySQL's `CURRENT_TIMESTAMP(6)`. Connections pin the MySQL session time zone
to UTC so database-side lease timestamps and Go `time.Time` values share the
same baseline. The schema uses `DATETIME(6)` plus claim indexes for MySQL 5.7.

## Install

```bash
go get github.com/DnullP/go-workflows-mysql57
```

## Usage

```go
import mysql57 "github.com/DnullP/go-workflows-mysql57"

backend := mysql57.NewMysqlBackend(
    "127.0.0.1",
    3306,
    "root",
    "root",
    "workflow_demo_mysql57",
    mysql57.WithTaskClaimBatchSize(32),
)
```

`WithTaskClaimBatchSize` controls how many candidate rows are inspected during
optimistic task pickup. The default is 16.

## Status

This package is intended for MySQL 5.7 compatibility with `go-workflows v1.4.1`.
It is covered by the upstream backend contract tests plus MySQL 5.7 integration
tests for concurrent claims, delayed activity visibility, expired leases, stale
lease reclaim, UTC session time, and deleted/completed lease handling.

## License

MIT. This fork includes code derived from `github.com/cschleiden/go-workflows`,
which is also MIT licensed.
