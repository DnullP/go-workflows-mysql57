package mysql57

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/metadata"
	"github.com/cschleiden/go-workflows/backend/metrics"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/workflow"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed db/migrations/*.sql
var migrationsFS embed.FS

const defaultTaskClaimBatchSize = 16

func NewMysqlBackend(host string, port int, user, password, database string, opts ...option) *mysqlBackend {
	options := &options{
		Options:            backend.ApplyOptions(),
		ApplyMigrations:    true,
		TaskClaimBatchSize: defaultTaskClaimBatchSize,
	}

	for _, opt := range opts {
		opt(options)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&interpolateParams=true&time_zone=%%27%%2B00%%3A00%%27", user, password, host, port, database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	if options.MySQLOptions != nil {
		options.MySQLOptions(db)
	}

	b := &mysqlBackend{
		dsn:        dsn,
		db:         db,
		workerName: getWorkerName(options),
		options:    options,
	}

	if options.ApplyMigrations {
		if err := b.Migrate(); err != nil {
			panic(err)
		}
	}

	return b
}

type mysqlBackend struct {
	dsn        string
	db         *sql.DB
	workerName string
	options    *options
}

func (mb *mysqlBackend) FeatureSupported(feature backend.Feature) bool {
	return true
}

func (mb *mysqlBackend) Close() error {
	return mb.db.Close()
}

// Migrate applies any pending database migrations.
func (mb *mysqlBackend) Migrate() (err error) {
	schemaDsn := mb.dsn + "&multiStatements=true"
	db, err := sql.Open("mysql", schemaDsn)
	if err != nil {
		return fmt.Errorf("opening schema database: %w", err)
	}

	dbOwnedByMigrationDriver := false
	defer func() {
		if !dbOwnedByMigrationDriver {
			if closeErr := db.Close(); err == nil && closeErr != nil {
				err = fmt.Errorf("closing schema database: %w", closeErr)
			}
		}
	}()

	dbi, err := mysql.WithInstance(db, &mysql.Config{})
	if err != nil {
		return fmt.Errorf("creating migration instance: %w", err)
	}
	dbOwnedByMigrationDriver = true

	driverOwnedByMigrate := false
	defer func() {
		if !driverOwnedByMigrate {
			if closeErr := dbi.Close(); err == nil && closeErr != nil {
				err = fmt.Errorf("closing migration database: %w", closeErr)
			}
		}
	}()

	migrations, err := iofs.New(migrationsFS, "db/migrations")
	if err != nil {
		return fmt.Errorf("creating migration source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", migrations, "mysql", dbi)
	if err != nil {
		return fmt.Errorf("creating migration: %w", err)
	}
	driverOwnedByMigrate = true
	defer func() {
		sourceErr, databaseErr := m.Close()
		if err == nil {
			if sourceErr != nil {
				err = fmt.Errorf("closing migration source: %w", sourceErr)
			} else if databaseErr != nil {
				err = fmt.Errorf("closing migration database: %w", databaseErr)
			}
		}
	}()

	if err := m.Up(); err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("running migrations: %w", err)
		}
	}

	return nil
}

func (mb *mysqlBackend) Tracer() trace.Tracer {
	return mb.options.TracerProvider.Tracer(backend.TracerName)
}

func (mb *mysqlBackend) Metrics() metrics.Client {
	return mb.options.Metrics.WithTags(metrics.Tags{"backend": "mysql57"})
}

func (mb *mysqlBackend) Options() *backend.Options {
	return mb.options.Options
}

func (mb *mysqlBackend) CreateWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	tx, err := mb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	a := event.Attributes.(*history.ExecutionStartedAttributes)

	// Create workflow instance
	if err := createInstance(ctx, tx, a.Queue, instance, a.Metadata); err != nil {
		return err
	}

	// Initial history is empty, store only new events
	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting new event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("creating workflow instance: %w", err)
	}

	return nil
}

func (mb *mysqlBackend) RemoveWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) error {
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := mb.removeWorkflowInstance(ctx, instance, tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (mb *mysqlBackend) removeWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance, tx *sql.Tx) error {
	row := tx.QueryRowContext(ctx, "SELECT state FROM `instances` WHERE instance_id = ? AND execution_id = ? LIMIT 1", instance.InstanceID, instance.ExecutionID)
	var state core.WorkflowInstanceState
	if err := row.Scan(&state); err != nil {
		if err == sql.ErrNoRows {
			return backend.ErrInstanceNotFound
		}
		return err
	}

	if state == core.WorkflowInstanceStateActive {
		return backend.ErrInstanceNotFinished
	}

	// Delete from instances and history tables
	if _, err := tx.ExecContext(ctx, "DELETE FROM `instances` WHERE instance_id = ? AND execution_id = ?", instance.InstanceID, instance.ExecutionID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM `history` WHERE instance_id = ? AND execution_id = ?", instance.InstanceID, instance.ExecutionID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM `attributes` WHERE instance_id = ? AND execution_id = ?", instance.InstanceID, instance.ExecutionID); err != nil {
		return err
	}

	return nil
}

func (mb *mysqlBackend) RemoveWorkflowInstances(ctx context.Context, options ...backend.RemovalOption) error {
	ro := backend.DefaultRemovalOptions
	for _, opt := range options {
		opt(&ro)
	}

	rows, err := mb.db.QueryContext(ctx, `SELECT instance_id, execution_id FROM instances WHERE completed_at < ?`, ro.FinishedBefore)
	if err != nil {
		return err
	}
	defer rows.Close()

	instanceIDs := []string{}
	executionIDs := []string{}
	for rows.Next() {
		var id, executionID string
		if err := rows.Scan(&id, &executionID); err != nil {
			return err
		}

		instanceIDs = append(instanceIDs, id)
		executionIDs = append(executionIDs, executionID)
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	batchSize := ro.BatchSize
	if batchSize <= 0 {
		batchSize = backend.DefaultRemovalOptions.BatchSize
	}
	for i := 0; i < len(instanceIDs); i += batchSize {
		instanceIDs := instanceIDs[i:min(i+batchSize, len(instanceIDs))]
		executionIDs := executionIDs[i:min(i+batchSize, len(executionIDs))]

		tx, err := mb.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		placeholders := strings.Repeat(", (?, ?)", len(instanceIDs)-1)
		whereCondition := fmt.Sprintf("(instance_id, execution_id) IN ((?, ?)%v)", placeholders)
		args := make([]interface{}, 0, len(instanceIDs)*2)
		for i := range instanceIDs {
			args = append(args, instanceIDs[i], executionIDs[i])
		}

		// Delete from instances, history and attributes tables
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM `instances` WHERE %v", whereCondition), args...); err != nil {
			_ = tx.Rollback()
			return err
		}

		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM `history` WHERE %v", whereCondition), args...); err != nil {
			_ = tx.Rollback()
			return err
		}

		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM `attributes` WHERE %v", whereCondition), args...); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return nil
}

func (mb *mysqlBackend) CancelWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	tx, err := mb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Cancel workflow instance
	// TODO: Combine this with the event insertion
	res := tx.QueryRowContext(ctx, "SELECT 1 FROM `instances` WHERE instance_id = ? AND execution_id = ? LIMIT 1", instance.InstanceID, instance.ExecutionID)
	if err := res.Scan(new(int)); err != nil {
		if err == sql.ErrNoRows {
			return backend.ErrInstanceNotFound
		}

		return err
	}

	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting cancellation event: %w", err)
	}

	return tx.Commit()
}

func (mb *mysqlBackend) GetWorkflowInstanceHistory(ctx context.Context, instance *workflow.Instance, lastSequenceID *int64) ([]*history.Event, error) {
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var historyEvents *sql.Rows
	if lastSequenceID != nil {
		historyEvents, err = tx.QueryContext(
			ctx,
			"SELECT h.event_id, h.sequence_id, h.event_type, h.timestamp, h.schedule_event_id, a.data, h.visible_at FROM `history` h JOIN `attributes` a ON h.event_id = a.event_id AND a.instance_id = h.instance_id AND a.execution_id = h.execution_id WHERE h.instance_id = ? AND h.execution_id = ? AND h.sequence_id > ? ORDER BY h.sequence_id",
			instance.InstanceID,
			instance.ExecutionID,
			*lastSequenceID,
		)
	} else {
		historyEvents, err = tx.QueryContext(
			ctx,
			"SELECT h.event_id, h.sequence_id, h.event_type, h.timestamp, h.schedule_event_id, a.data, h.visible_at FROM `history` h JOIN `attributes` a ON h.event_id = a.event_id AND a.instance_id = h.instance_id AND a.execution_id = h.execution_id WHERE h.instance_id = ? AND h.execution_id = ? ORDER BY h.sequence_id",
			instance.InstanceID,
			instance.ExecutionID,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("getting history: %w", err)
	}

	defer historyEvents.Close()

	h := make([]*history.Event, 0)

	for historyEvents.Next() {
		var attributes []byte

		historyEvent := &history.Event{}

		if err := historyEvents.Scan(
			&historyEvent.ID,
			&historyEvent.SequenceID,
			&historyEvent.Type,
			&historyEvent.Timestamp,
			&historyEvent.ScheduleEventID,
			&attributes,
			&historyEvent.VisibleAt,
		); err != nil {
			return nil, fmt.Errorf("scanning event: %w", err)
		}

		a, err := history.DeserializeAttributes(historyEvent.Type, attributes)
		if err != nil {
			return nil, fmt.Errorf("deserializing attributes: %w", err)
		}

		historyEvent.Attributes = a

		h = append(h, historyEvent)
	}

	if historyEvents.Err() != nil {
		return nil, historyEvents.Err()
	}

	return h, nil
}

func (mb *mysqlBackend) GetWorkflowInstanceState(ctx context.Context, instance *workflow.Instance) (core.WorkflowInstanceState, error) {
	row := mb.db.QueryRowContext(
		ctx,
		"SELECT state FROM instances WHERE instance_id = ? AND execution_id = ?",
		instance.InstanceID,
		instance.ExecutionID,
	)

	var state core.WorkflowInstanceState
	if err := row.Scan(&state); err != nil {
		if err == sql.ErrNoRows {
			return core.WorkflowInstanceStateActive, backend.ErrInstanceNotFound
		}
		return core.WorkflowInstanceStateActive, err
	}

	return state, nil
}

func createInstance(ctx context.Context, tx *sql.Tx, queue workflow.Queue, wfi *workflow.Instance, metadata *workflow.Metadata) error {
	// Check for existing instance
	err := tx.QueryRowContext(
		ctx,
		"SELECT 1 FROM `instances` WHERE instance_id = ? AND state = ? LIMIT 1",
		wfi.InstanceID,
		core.WorkflowInstanceStateActive).
		Scan(new(int))
	if err == nil {
		return backend.ErrInstanceAlreadyExists
	}
	if err != sql.ErrNoRows {
		return err
	}

	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	if wfi.SubWorkflow() {
		parentInstanceID = &wfi.Parent.InstanceID
		parentExecutionID = &wfi.Parent.ExecutionID
		parentEventID = &wfi.ParentEventID
	}

	metadataJson, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO `instances` (queue, instance_id, execution_id, parent_instance_id, parent_execution_id, parent_schedule_event_id, metadata, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		string(queue),
		wfi.InstanceID,
		wfi.ExecutionID,
		parentInstanceID,
		parentExecutionID,
		parentEventID,
		string(metadataJson),
		core.WorkflowInstanceStateActive,
	)
	if err != nil {
		return fmt.Errorf("inserting workflow instance: %w", err)
	}

	return nil
}

// SignalWorkflow signals a running workflow instance
func (mb *mysqlBackend) SignalWorkflow(ctx context.Context, instanceID string, event *history.Event) error {
	tx, err := mb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO: Combine this with the event insertion
	res := tx.QueryRowContext(ctx, "SELECT execution_id FROM `instances` WHERE instance_id = ? AND state = ? LIMIT 1", instanceID, core.WorkflowInstanceStateActive)
	var executionID string
	if err := res.Scan(&executionID); err != nil {
		if err == sql.ErrNoRows {
			return backend.ErrInstanceNotFound
		}
		return err
	}

	instance := core.NewWorkflowInstance(instanceID, executionID)

	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting signal event: %w", err)
	}

	return tx.Commit()
}

func (mb *mysqlBackend) PrepareWorkflowQueues(ctx context.Context, queues []workflow.Queue) error {
	return nil
}

func (mb *mysqlBackend) PrepareActivityQueues(ctx context.Context, queues []workflow.Queue) error {
	return nil
}

// GetWorkflowInstance returns a pending workflow task or nil if there are no pending workflow executions.
//
// MySQL 5.7 does not support SKIP LOCKED, so this uses an optimistic claim:
// select a candidate, then lock it with a conditional UPDATE. If another poller
// wins the race first, RowsAffected is 0 and the worker can poll again later.
func (mb *mysqlBackend) GetWorkflowTask(ctx context.Context, queues []workflow.Queue) (*backend.WorkflowTask, error) {
	if len(queues) == 0 {
		return nil, nil
	}

	tx, err := mb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now()
	lockToken := uuid.NewString()
	args := []any{
		core.WorkflowInstanceStateActive, // state
		now,                              // event.visible_at
		mb.workerName,                    // worker
	}

	queuePlaceholders := strings.Repeat(",?", len(queues)-1)
	for _, q := range queues {
		args = append(args, string(q))
	}

	// Find candidate workflow tasks. This is intentionally not FOR UPDATE:
	// MySQL 5.7 cannot skip locked rows, so the conditional UPDATE below is
	// the actual claim step.
	rows, err := tx.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT DISTINCT i.id
			FROM instances i
			INNER JOIN pending_events pe ON i.instance_id = pe.instance_id AND i.execution_id = pe.execution_id
			WHERE
				i.state = ? AND i.completed_at IS NULL
				AND (pe.visible_at IS NULL OR pe.visible_at <= ?)
				AND (i.locked_until IS NULL OR i.locked_until < CURRENT_TIMESTAMP(6))
				AND (i.sticky_until IS NULL OR i.sticky_until < CURRENT_TIMESTAMP(6) OR i.worker = ?)
				AND (i.queue in (?%s))
			ORDER BY i.id
			LIMIT ?
			`, queuePlaceholders),
		append(args, mb.options.TaskClaimBatchSize)...,
	)
	if err != nil {
		return nil, fmt.Errorf("finding workflow instances to lock: %w", err)
	}

	candidateIDs := make([]int64, 0, mb.options.TaskClaimBatchSize)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning workflow instance candidate: %w", err)
		}

		candidateIDs = append(candidateIDs, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("reading workflow instance candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing workflow instance candidates: %w", err)
	}

	var id int64
	for _, candidateID := range candidateIDs {
		res, err := tx.ExecContext(
			ctx,
			`UPDATE instances i
				SET locked_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? MICROSECOND), worker = ?, lock_token = ?
				WHERE id = ?
					AND state = ?
					AND completed_at IS NULL
					AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP(6))
					AND (sticky_until IS NULL OR sticky_until < CURRENT_TIMESTAMP(6) OR worker = ?)
					AND EXISTS (
						SELECT 1 FROM pending_events pe
						WHERE pe.instance_id = i.instance_id
							AND pe.execution_id = i.execution_id
							AND (pe.visible_at IS NULL OR pe.visible_at <= ?)
					)`,
			durationMicros(mb.options.WorkflowLockTimeout),
			mb.workerName,
			lockToken,
			candidateID,
			core.WorkflowInstanceStateActive,
			mb.workerName,
			now,
		)
		if err != nil {
			return nil, fmt.Errorf("locking workflow instance: %w", err)
		}

		affectedRows, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("locking workflow instance: %w", err)
		}
		if affectedRows == 1 {
			id = candidateID
			break
		}
		if affectedRows > 1 {
			return nil, fmt.Errorf("locking workflow instance: updated %d rows", affectedRows)
		}
	}
	if id == 0 {
		return nil, nil
	}

	var queue, instanceID, executionID string
	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	var metadataJson sql.NullString
	var stickyUntil *time.Time
	row := tx.QueryRowContext(
		ctx,
		`SELECT i.queue, i.instance_id, i.execution_id, i.parent_instance_id, i.parent_execution_id, i.parent_schedule_event_id, i.metadata, i.sticky_until
			FROM instances i
			WHERE i.id = ? AND i.worker = ? AND i.lock_token = ?`,
		id,
		mb.workerName,
		lockToken,
	)
	if err := row.Scan(&queue, &instanceID, &executionID, &parentInstanceID, &parentExecutionID, &parentEventID, &metadataJson, &stickyUntil); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, fmt.Errorf("scanning workflow instance: %w", err)
	}

	var wfi *workflow.Instance
	if parentInstanceID != nil {
		wfi = core.NewSubWorkflowInstance(instanceID, executionID, core.NewWorkflowInstance(*parentInstanceID, *parentExecutionID), *parentEventID)
	} else {
		wfi = core.NewWorkflowInstance(instanceID, executionID)
	}

	var metadata *metadata.WorkflowMetadata
	if metadataJson.Valid {
		if err := json.Unmarshal([]byte(metadataJson.String), &metadata); err != nil {
			return nil, fmt.Errorf("parsing workflow metadata: %w", err)
		}
	}

	t := &backend.WorkflowTask{
		ID:                    wfi.InstanceID,
		WorkflowInstance:      wfi,
		WorkflowInstanceState: core.WorkflowInstanceStateActive,
		Metadata:              metadata,
		NewEvents:             []*history.Event{},
		Queue:                 workflow.Queue(queue),
		CustomData:            lockToken,
	}

	// Get new events
	events, err := tx.QueryContext(
		ctx,
		"SELECT pe.event_id, pe.sequence_id, pe.event_type, pe.timestamp, pe.schedule_event_id, a.data, pe.visible_at FROM `pending_events` pe LEFT JOIN `attributes` a ON pe.instance_id = a.instance_id AND pe.execution_id = a.execution_id AND pe.event_id = a.event_id WHERE pe.instance_id = ? AND pe.execution_id = ? AND (pe.visible_at IS NULL OR pe.visible_at <= ?) ORDER BY pe.id",
		instanceID,
		executionID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("getting new events: %w", err)
	}

	defer events.Close()

	for events.Next() {
		var attributes []byte

		historyEvent := &history.Event{}

		if err := events.Scan(
			&historyEvent.ID,
			&historyEvent.SequenceID,
			&historyEvent.Type,
			&historyEvent.Timestamp,
			&historyEvent.ScheduleEventID,
			&attributes,
			&historyEvent.VisibleAt,
		); err != nil {
			return nil, fmt.Errorf("scanning event: %w", err)
		}

		a, err := history.DeserializeAttributes(historyEvent.Type, attributes)
		if err != nil {
			return nil, fmt.Errorf("deserializing attributes: %w", err)
		}

		historyEvent.Attributes = a

		t.NewEvents = append(t.NewEvents, historyEvent)
	}

	if events.Err() != nil {
		return nil, events.Err()
	}

	// Return if there aren't any new events
	if len(t.NewEvents) == 0 {
		return nil, nil
	}

	// Get most recent sequence id
	var lastSequenceID sql.NullInt64
	row = tx.QueryRowContext(ctx, "SELECT MAX(sequence_id) FROM `history` WHERE instance_id = ? AND execution_id = ?", instanceID, executionID)
	if err := row.Scan(
		&lastSequenceID,
	); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("getting most recent sequence id: %w", err)
		}
	}

	if lastSequenceID.Valid {
		t.LastSequenceID = lastSequenceID.Int64
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return t, nil
}

// CompleteWorkflowTask completes a workflow task retrieved using GetWorkflowTask
//
// This checkpoints the execution. events are new events from the last workflow execution
// which will be added to the workflow instance history. workflowEvents are new events for the
// completed or other workflow instances.
func (mb *mysqlBackend) CompleteWorkflowTask(
	ctx context.Context,
	task *backend.WorkflowTask,
	state core.WorkflowInstanceState,
	executedEvents, activityEvents, timerEvents []*history.Event,
	workflowEvents []*history.WorkflowEvent,
) error {
	tx, err := mb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	instance := task.WorkflowInstance
	lockToken, err := workflowTaskLockToken(task)
	if err != nil {
		return err
	}

	// Unlock instance, but keep it sticky to the current worker
	var completedAt *time.Time
	if state == core.WorkflowInstanceStateContinuedAsNew || state == core.WorkflowInstanceStateFinished {
		t := time.Now()
		completedAt = &t
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE instances
			SET locked_until = NULL,
				lock_token = NULL,
				sticky_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? MICROSECOND),
				completed_at = ?,
				state = ?
			WHERE instance_id = ? AND execution_id = ? AND worker = ? AND lock_token = ? AND locked_until > CURRENT_TIMESTAMP(6)`,
		durationMicros(mb.options.StickyTimeout),
		completedAt,
		state,
		instance.InstanceID,
		instance.ExecutionID,
		mb.workerName,
		lockToken,
	)
	if err != nil {
		return fmt.Errorf("unlocking instance: %w", err)
	}

	changedRows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking for unlocked workflow instances: %w", err)
	} else if changedRows != 1 {
		return errors.New("could not find workflow instance to unlock")
	}

	// Remove handled events from task
	if len(executedEvents) > 0 {
		args := make([]interface{}, 0, len(executedEvents)+1)
		args = append(args, instance.InstanceID, instance.ExecutionID)
		for _, e := range executedEvents {
			args = append(args, e.ID)
		}

		if _, err := tx.ExecContext(
			ctx,
			fmt.Sprintf(`DELETE FROM pending_events WHERE instance_id = ? AND execution_id = ? AND event_id IN (?%v)`, strings.Repeat(",?", len(executedEvents)-1)),
			args...,
		); err != nil {
			return fmt.Errorf("deleting handled new events: %w", err)
		}
	}

	// Insert new events generated during this workflow execution to the history
	if err := insertHistoryEvents(ctx, tx, instance, executedEvents); err != nil {
		return fmt.Errorf("inserting new history events: %w", err)
	}

	// Schedule activities
	for _, e := range activityEvents {
		a := e.Attributes.(*history.ActivityScheduledAttributes)
		queue := a.Queue
		if queue == "" {
			// Default to workflow queue
			queue = task.Queue
		}

		if err := scheduleActivity(ctx, tx, queue, instance, e); err != nil {
			return fmt.Errorf("scheduling activity: %w", err)
		}
	}

	// Timer events
	if err := insertPendingEvents(ctx, tx, instance, timerEvents); err != nil {
		return fmt.Errorf("scheduling timers: %w", err)
	}

	for _, event := range executedEvents {
		switch event.Type {
		case history.EventType_TimerCanceled:
			if err := removeFutureEvent(ctx, tx, instance, event.ScheduleEventID); err != nil {
				return fmt.Errorf("removing future event: %w", err)
			}
		}
	}

	// Insert new workflow events
	groupedEvents := history.EventsByWorkflowInstance(workflowEvents)

	for targetInstance, events := range groupedEvents {
		// Are we creating a new sub-workflow instance?
		m := events[0]
		if m.HistoryEvent.Type == history.EventType_WorkflowExecutionStarted {
			a := m.HistoryEvent.Attributes.(*history.ExecutionStartedAttributes)

			queue := a.Queue
			if queue == "" {
				queue = task.Queue
			}

			// Create new instance
			if err := createInstance(ctx, tx, queue, m.WorkflowInstance, a.Metadata); err != nil {
				if err == backend.ErrInstanceAlreadyExists {
					if err := insertPendingEvents(ctx, tx, instance, []*history.Event{
						history.NewPendingEvent(time.Now(), history.EventType_SubWorkflowFailed, &history.SubWorkflowFailedAttributes{
							Error: toWorkflowError(backend.ErrInstanceAlreadyExists),
						}, history.ScheduleEventID(m.WorkflowInstance.ParentEventID)),
					}); err != nil {
						return fmt.Errorf("inserting sub-workflow failed event: %w", err)
					}

					continue
				}

				return fmt.Errorf("creating sub-workflow instance: %w", err)
			}
		}

		// Insert pending events for target instance
		historyEvents := []*history.Event{}
		for _, m := range events {
			historyEvents = append(historyEvents, m.HistoryEvent)
		}
		if err := insertPendingEvents(ctx, tx, &targetInstance, historyEvents); err != nil {
			return fmt.Errorf("inserting messages: %w", err)
		}
	}

	if mb.options.RemoveContinuedAsNewInstances && state == core.WorkflowInstanceStateContinuedAsNew {
		if err := mb.removeWorkflowInstance(ctx, instance, tx); err != nil {
			return fmt.Errorf("removing old instance: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing complete workflow transaction: %w", err)
	}

	return nil
}

func (mb *mysqlBackend) ExtendWorkflowTask(ctx context.Context, task *backend.WorkflowTask) error {
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	lockToken, err := workflowTaskLockToken(task)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE instances
			SET locked_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? MICROSECOND)
			WHERE instance_id = ? AND execution_id = ? AND worker = ? AND lock_token = ? AND locked_until > CURRENT_TIMESTAMP(6)`,
		durationMicros(mb.options.WorkflowLockTimeout),
		task.WorkflowInstance.InstanceID,
		task.WorkflowInstance.ExecutionID,
		mb.workerName,
		lockToken,
	)
	if err != nil {
		return fmt.Errorf("extending workflow task lock: %w", err)
	}

	if rowsAffected, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("determining if workflow task was extended: %w", err)
	} else if rowsAffected != 1 {
		return fmt.Errorf("could not extend workflow task, updated %d rows", rowsAffected)
	}

	return tx.Commit()
}

// GetActivityTask returns a pending activity task or nil if there are no pending activities.
func (mb *mysqlBackend) GetActivityTask(ctx context.Context, queues []workflow.Queue) (*backend.ActivityTask, error) {
	if len(queues) == 0 {
		return nil, nil
	}

	tx, err := mb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	queuePlaceholders := strings.Repeat(",?", len(queues)-1)

	now := time.Now()
	lockToken := uuid.NewString()

	args := make([]interface{}, 0, len(queues)+1)
	args = append(args, now)
	for _, q := range queues {
		args = append(args, string(q))
	}

	rows, err := tx.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT a.id
			FROM activities a
			WHERE (a.locked_until IS NULL OR a.locked_until < CURRENT_TIMESTAMP(6))
				AND (a.visible_at IS NULL OR a.visible_at <= ?)
				AND a.queue IN (?%s)
			ORDER BY a.id
			LIMIT ?
			`, queuePlaceholders),
		append(args, mb.options.TaskClaimBatchSize)...,
	)
	if err != nil {
		return nil, fmt.Errorf("finding activity tasks to lock: %w", err)
	}

	candidateIDs := make([]int64, 0, mb.options.TaskClaimBatchSize)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning activity candidate: %w", err)
		}

		candidateIDs = append(candidateIDs, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("reading activity candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing activity candidates: %w", err)
	}

	var id int64
	for _, candidateID := range candidateIDs {
		update, err := tx.ExecContext(
			ctx,
			`UPDATE activities
				SET locked_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? MICROSECOND), worker = ?, lock_token = ?
				WHERE id = ?
					AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP(6))
					AND (visible_at IS NULL OR visible_at <= ?)`,
			durationMicros(mb.options.ActivityLockTimeout),
			mb.workerName,
			lockToken,
			candidateID,
			now,
		)
		if err != nil {
			return nil, fmt.Errorf("locking activity: %w", err)
		}

		affectedRows, err := update.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("locking activity: %w", err)
		}
		if affectedRows == 1 {
			id = candidateID
			break
		}
		if affectedRows > 1 {
			return nil, fmt.Errorf("locking activity: updated %d rows", affectedRows)
		}
	}
	if id == 0 {
		return nil, nil
	}

	res := tx.QueryRowContext(
		ctx,
		`SELECT a.activity_id, a.instance_id, a.execution_id, a.queue,
			a.event_type, a.timestamp, a.schedule_event_id, at.data, a.visible_at
			FROM activities a
			JOIN attributes at ON at.event_id = a.activity_id AND at.instance_id = a.instance_id AND at.execution_id = a.execution_id
			WHERE a.id = ? AND a.worker = ? AND a.lock_token = ?`,
		id,
		mb.workerName,
		lockToken,
	)

	var instanceID, executionID, queue string
	var attributes []byte
	event := &history.Event{}

	if err := res.Scan(
		&event.ID, &instanceID, &executionID, &queue, &event.Type,
		&event.Timestamp, &event.ScheduleEventID, &attributes, &event.VisibleAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, fmt.Errorf("reading locked activity task: %w", err)
	}

	a, err := history.DeserializeAttributes(event.Type, attributes)
	if err != nil {
		return nil, fmt.Errorf("deserializing attributes: %w", err)
	}

	event.Attributes = a

	t := &backend.ActivityTask{
		ID:               lockToken,
		ActivityID:       event.ID,
		Queue:            workflow.Queue(queue),
		WorkflowInstance: core.NewWorkflowInstance(instanceID, executionID),
		Event:            event,
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return t, nil
}

// CompleteActivityTask completes a activity task retrieved using GetActivityTask
func (mb *mysqlBackend) CompleteActivityTask(ctx context.Context, task *backend.ActivityTask, result *history.Event) error {
	tx, err := mb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Remove activity
	lockToken, err := activityTaskLockToken(task)
	if err != nil {
		return err
	}

	if res, err := tx.ExecContext(
		ctx,
		`DELETE FROM activities
			WHERE activity_id = ? AND instance_id = ? AND execution_id = ? AND worker = ? AND queue = ? AND lock_token = ? AND locked_until > CURRENT_TIMESTAMP(6)`,
		task.ActivityID,
		task.WorkflowInstance.InstanceID,
		task.WorkflowInstance.ExecutionID,
		mb.workerName,
		task.Queue,
		lockToken,
	); err != nil {
		return fmt.Errorf("completing activity: %w", err)
	} else {
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("checking for completed activity: %w", err)
		}

		if affected != 1 {
			return fmt.Errorf("could not find locked activity, deleted %d rows", affected)
		}
	}

	// Insert new event generated during this workflow execution
	if err := insertPendingEvents(ctx, tx, task.WorkflowInstance, []*history.Event{result}); err != nil {
		return fmt.Errorf("inserting new events for completed activity: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (mb *mysqlBackend) ExtendActivityTask(ctx context.Context, task *backend.ActivityTask) error {
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	lockToken, err := activityTaskLockToken(task)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE activities
			SET locked_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? MICROSECOND)
			WHERE activity_id = ?
				AND instance_id = ?
				AND execution_id = ?
				AND worker = ?
				AND queue = ?
				AND lock_token = ?
				AND locked_until > CURRENT_TIMESTAMP(6)`,
		durationMicros(mb.options.ActivityLockTimeout),
		task.ActivityID,
		task.WorkflowInstance.InstanceID,
		task.WorkflowInstance.ExecutionID,
		mb.workerName,
		task.Queue,
		lockToken,
	)
	if err != nil {
		return fmt.Errorf("extending activity lock: %w", err)
	}

	if rowsAffected, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("determining if activity task was extended: %w", err)
	} else if rowsAffected != 1 {
		return fmt.Errorf("could not extend activity task, updated %d rows", rowsAffected)
	}

	return tx.Commit()
}

func scheduleActivity(ctx context.Context, tx *sql.Tx, queue workflow.Queue, instance *core.WorkflowInstance, event *history.Event) error {
	// Attributes are already persisted via the history, we do not need to add them again.
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO activities
			(activity_id, instance_id, execution_id, queue, event_type, timestamp, schedule_event_id, visible_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID,
		instance.InstanceID,
		instance.ExecutionID,
		string(queue),
		event.Type,
		event.Timestamp,
		event.ScheduleEventID,
		event.VisibleAt,
	)

	return err
}

// getWorkerName returns the worker name from options, or generates a UUID-based name if not set.
func getWorkerName(options *options) string {
	if options.WorkerName != "" {
		return options.WorkerName
	}
	return fmt.Sprintf("worker-%v", uuid.NewString())
}

func workflowTaskLockToken(task *backend.WorkflowTask) (string, error) {
	lockToken, ok := task.CustomData.(string)
	if !ok || lockToken == "" {
		return "", errors.New("workflow task is missing lock token")
	}

	return lockToken, nil
}

func activityTaskLockToken(task *backend.ActivityTask) (string, error) {
	if task.ID == "" {
		return "", errors.New("activity task is missing lock token")
	}

	return task.ID, nil
}

func durationMicros(duration time.Duration) int64 {
	return duration.Microseconds()
}

func toWorkflowError(err error) *workflow.Error {
	if err == nil {
		return nil
	}

	if workflowErr, ok := err.(*workflow.Error); ok {
		return workflowErr
	}

	return &workflow.Error{
		Message: err.Error(),
	}
}
