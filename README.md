# go-workflows MySQL 5.7 backend

This package provides a MySQL 5.7-compatible backend for
[`github.com/cschleiden/go-workflows`](https://github.com/cschleiden/go-workflows).

It is a small fork of the upstream `backend/mysql` package. The upstream backend
uses `FOR UPDATE ... SKIP LOCKED`, which is available in MySQL 8+ but not in
MySQL 5.7. This fork replaces workflow and activity task pickup with optimistic
lock claims:

1. Select a candidate task id.
2. Claim it with a conditional `UPDATE` on `locked_until` and `worker`.
3. Read the task details only if the current worker won the claim.

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
)
```

## Status

This package is intended for MySQL 5.7 compatibility with `go-workflows v1.4.1`.
It has been verified with a local demo workflow, but it should still be tested
under the expected worker concurrency and lock-timeout behavior before production
use.

## License

MIT. This fork includes code derived from `github.com/cschleiden/go-workflows`,
which is also MIT licensed.
