package mysql57

import (
	"database/sql"

	"github.com/cschleiden/go-workflows/backend"
)

type options struct {
	*backend.Options

	MySQLOptions func(db *sql.DB)

	// ApplyMigrations automatically applies database migrations on startup.
	ApplyMigrations bool

	// TaskClaimBatchSize controls how many candidate rows are inspected when
	// optimistically claiming workflow or activity tasks.
	TaskClaimBatchSize int
}

type option func(*options)

// WithApplyMigrations automatically applies database migrations on startup.
func WithApplyMigrations(applyMigrations bool) option {
	return func(o *options) {
		o.ApplyMigrations = applyMigrations
	}
}

func WithMySQLOptions(f func(db *sql.DB)) option {
	return func(o *options) {
		o.MySQLOptions = f
	}
}

// WithTaskClaimBatchSize sets how many candidates are tried during optimistic
// task claiming. Values less than 1 keep the default.
func WithTaskClaimBatchSize(size int) option {
	return func(o *options) {
		if size > 0 {
			o.TaskClaimBatchSize = size
		}
	}
}

// WithBackendOptions allows to pass generic backend options.
func WithBackendOptions(opts ...backend.BackendOption) option {
	return func(o *options) {
		for _, opt := range opts {
			opt(o.Options)
		}
	}
}
