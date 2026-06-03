package mysql57

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	backendtest "github.com/cschleiden/go-workflows/backend/test"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	testHost     = "127.0.0.1"
	testPort     = 3306
	testUser     = "root"
	testPassword = "root"
)

func TestMysqlBackendContract(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	backendtest.BackendTest(t, func(options ...backend.BackendOption) backendtest.TestBackend {
		dbName := createTestDatabase(t)
		t.Cleanup(func() { dropTestDatabase(t, dbName) })

		options = append(options, backend.WithStickyTimeout(0))
		return NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName, WithBackendOptions(options...))
	}, func(b backendtest.TestBackend) {
		require.NoError(t, b.Close())
	})
}

func TestMysqlBackendEmptyQueues(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t)

	workflowTask, err := b.GetWorkflowTask(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, workflowTask)

	activityTask, err := b.GetActivityTask(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, activityTask)
}

func TestMysqlBackendSessionTimeZoneIsUTC(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t)

	var timeZone string
	var timestampDiff int
	require.NoError(t, b.db.QueryRowContext(
		ctx,
		"SELECT @@session.time_zone, TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), CURRENT_TIMESTAMP())",
	).Scan(&timeZone, &timestampDiff))

	require.Equal(t, "+00:00", timeZone)
	require.Zero(t, timestampDiff)
}

func TestMysqlBackendConcurrentWorkflowClaims(t *testing.T) {
	ctx := context.Background()
	const taskCount = 8

	dbName := createTestDatabase(t)
	t.Cleanup(func() { dropTestDatabase(t, dbName) })

	seed := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
		WithBackendOptions(backend.WithStickyTimeout(0), backend.WithWorkerName("seed")),
		WithTaskClaimBatchSize(taskCount),
	)
	t.Cleanup(func() { require.NoError(t, seed.Close()) })

	for i := 0; i < taskCount; i++ {
		createWorkflow(t, ctx, seed, workflow.QueueDefault)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan *backend.WorkflowTask, taskCount)
	errs := make(chan error, taskCount)

	for i := 0; i < taskCount; i++ {
		i := i
		worker := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
			WithApplyMigrations(false),
			WithBackendOptions(backend.WithStickyTimeout(0), backend.WithWorkerName(fmt.Sprintf("workflow-worker-%d", i))),
			WithTaskClaimBatchSize(taskCount),
		)
		t.Cleanup(func() { require.NoError(t, worker.Close()) })

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			task, err := worker.GetWorkflowTask(ctx, []workflow.Queue{workflow.QueueDefault})
			if err != nil {
				errs <- err
				return
			}
			results <- task
		}()
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	require.Empty(t, collectErrors(errs))

	claimed := map[string]struct{}{}
	for task := range results {
		require.NotNil(t, task)
		claimed[task.WorkflowInstance.InstanceID] = struct{}{}
	}
	require.Len(t, claimed, taskCount)
}

func TestMysqlBackendConcurrentActivityClaims(t *testing.T) {
	ctx := context.Background()
	const taskCount = 8

	dbName := createTestDatabase(t)
	t.Cleanup(func() { dropTestDatabase(t, dbName) })

	seed := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
		WithBackendOptions(backend.WithStickyTimeout(0), backend.WithWorkerName("seed")),
		WithTaskClaimBatchSize(taskCount),
	)
	t.Cleanup(func() { require.NoError(t, seed.Close()) })

	for i := 0; i < taskCount; i++ {
		scheduleActivityTask(t, ctx, seed, workflow.QueueDefault, workflow.QueueDefault, nil)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan *backend.ActivityTask, taskCount)
	errs := make(chan error, taskCount)

	for i := 0; i < taskCount; i++ {
		i := i
		worker := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
			WithApplyMigrations(false),
			WithBackendOptions(backend.WithStickyTimeout(0), backend.WithWorkerName(fmt.Sprintf("activity-worker-%d", i))),
			WithTaskClaimBatchSize(taskCount),
		)
		t.Cleanup(func() { require.NoError(t, worker.Close()) })

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			task, err := worker.GetActivityTask(ctx, []workflow.Queue{workflow.QueueDefault})
			if err != nil {
				errs <- err
				return
			}
			results <- task
		}()
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	require.Empty(t, collectErrors(errs))

	claimed := map[string]struct{}{}
	for task := range results {
		require.NotNil(t, task)
		claimed[task.ActivityID] = struct{}{}
	}
	require.Len(t, claimed, taskCount)
}

func TestMysqlBackendActivityVisibleAtIsRespected(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t)

	future := time.Now().Add(time.Hour)
	scheduleActivityTask(t, ctx, b, workflow.QueueDefault, workflow.QueueDefault, &future)

	task, err := b.GetActivityTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.Nil(t, task)
}

func TestMysqlBackendActivityLeaseCannotBeExtendedAfterCompletion(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t)

	scheduleActivityTask(t, ctx, b, workflow.QueueDefault, workflow.QueueDefault, nil)

	task, err := b.GetActivityTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, task)

	result := history.NewPendingEvent(time.Now(), history.EventType_ActivityCompleted, &history.ActivityCompletedAttributes{}, history.ScheduleEventID(task.Event.ScheduleEventID))
	require.NoError(t, b.CompleteActivityTask(ctx, task, result))

	err = b.ExtendActivityTask(ctx, task)
	require.Error(t, err)
}

func TestMysqlBackendExpiredLeasesCannotBeExtendedOrCompleted(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t,
		backend.WithWorkflowLockTimeout(50*time.Millisecond),
		backend.WithActivityLockTimeout(50*time.Millisecond),
	)

	wfi := createWorkflow(t, ctx, b, workflow.QueueDefault)
	workflowTask, err := b.GetWorkflowTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, workflowTask)
	require.Equal(t, wfi.InstanceID, workflowTask.WorkflowInstance.InstanceID)

	time.Sleep(120 * time.Millisecond)
	require.Error(t, b.ExtendWorkflowTask(ctx, workflowTask))
	require.Error(t, b.CompleteWorkflowTask(ctx, workflowTask, core.WorkflowInstanceStateActive, workflowTask.NewEvents, nil, nil, nil))

	b = newTestBackend(t,
		backend.WithWorkflowLockTimeout(time.Second),
		backend.WithActivityLockTimeout(50*time.Millisecond),
	)

	scheduleActivityTask(t, ctx, b, workflow.QueueDefault, workflow.QueueDefault, nil)
	activityTask, err := b.GetActivityTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, activityTask)

	time.Sleep(120 * time.Millisecond)
	require.Error(t, b.ExtendActivityTask(ctx, activityTask))

	result := history.NewPendingEvent(time.Now(), history.EventType_ActivityCompleted, &history.ActivityCompletedAttributes{}, history.ScheduleEventID(activityTask.Event.ScheduleEventID))
	require.Error(t, b.CompleteActivityTask(ctx, activityTask, result))
}

func TestMysqlBackendStaleWorkflowLeaseCannotCompleteAfterReclaim(t *testing.T) {
	ctx := context.Background()

	dbName := createTestDatabase(t)
	t.Cleanup(func() { dropTestDatabase(t, dbName) })

	worker1 := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
		WithBackendOptions(
			backend.WithStickyTimeout(0),
			backend.WithWorkerName("workflow-stale-worker-1"),
			backend.WithWorkflowLockTimeout(50*time.Millisecond),
		),
	)
	t.Cleanup(func() { require.NoError(t, worker1.Close()) })

	wfi := createWorkflow(t, ctx, worker1, workflow.QueueDefault)
	staleTask, err := worker1.GetWorkflowTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, staleTask)
	require.Equal(t, wfi.InstanceID, staleTask.WorkflowInstance.InstanceID)

	time.Sleep(120 * time.Millisecond)

	worker2 := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
		WithApplyMigrations(false),
		WithBackendOptions(
			backend.WithStickyTimeout(0),
			backend.WithWorkerName("workflow-stale-worker-2"),
			backend.WithWorkflowLockTimeout(time.Second),
		),
	)
	t.Cleanup(func() { require.NoError(t, worker2.Close()) })

	reclaimedTask, err := worker2.GetWorkflowTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, reclaimedTask)
	require.Equal(t, staleTask.WorkflowInstance.InstanceID, reclaimedTask.WorkflowInstance.InstanceID)

	require.Error(t, worker1.ExtendWorkflowTask(ctx, staleTask))
	require.Error(t, worker1.CompleteWorkflowTask(ctx, staleTask, core.WorkflowInstanceStateActive, staleTask.NewEvents, nil, nil, nil))
	require.NoError(t, worker2.CompleteWorkflowTask(ctx, reclaimedTask, core.WorkflowInstanceStateActive, reclaimedTask.NewEvents, nil, nil, nil))
}

func TestMysqlBackendStaleActivityLeaseCannotCompleteAfterReclaim(t *testing.T) {
	ctx := context.Background()

	dbName := createTestDatabase(t)
	t.Cleanup(func() { dropTestDatabase(t, dbName) })

	seed := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
		WithBackendOptions(backend.WithStickyTimeout(0), backend.WithWorkerName("activity-seed")),
	)
	t.Cleanup(func() { require.NoError(t, seed.Close()) })
	scheduleActivityTask(t, ctx, seed, workflow.QueueDefault, workflow.QueueDefault, nil)

	worker1 := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
		WithApplyMigrations(false),
		WithBackendOptions(
			backend.WithWorkerName("activity-stale-worker-1"),
			backend.WithActivityLockTimeout(50*time.Millisecond),
		),
	)
	t.Cleanup(func() { require.NoError(t, worker1.Close()) })

	staleTask, err := worker1.GetActivityTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, staleTask)

	time.Sleep(120 * time.Millisecond)

	worker2 := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName,
		WithApplyMigrations(false),
		WithBackendOptions(
			backend.WithWorkerName("activity-stale-worker-2"),
			backend.WithActivityLockTimeout(time.Second),
		),
	)
	t.Cleanup(func() { require.NoError(t, worker2.Close()) })

	reclaimedTask, err := worker2.GetActivityTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, reclaimedTask)
	require.Equal(t, staleTask.ActivityID, reclaimedTask.ActivityID)
	require.NotEqual(t, staleTask.ID, reclaimedTask.ID)

	staleResult := history.NewPendingEvent(time.Now(), history.EventType_ActivityCompleted, &history.ActivityCompletedAttributes{}, history.ScheduleEventID(staleTask.Event.ScheduleEventID))
	require.Error(t, worker1.ExtendActivityTask(ctx, staleTask))
	require.Error(t, worker1.CompleteActivityTask(ctx, staleTask, staleResult))

	reclaimedResult := history.NewPendingEvent(time.Now(), history.EventType_ActivityCompleted, &history.ActivityCompletedAttributes{}, history.ScheduleEventID(reclaimedTask.Event.ScheduleEventID))
	require.NoError(t, worker2.CompleteActivityTask(ctx, reclaimedTask, reclaimedResult))
}

var _ backendtest.TestBackend = (*mysqlBackend)(nil)

func (mb *mysqlBackend) GetFutureEvents(ctx context.Context) ([]*history.Event, error) {
	tx, err := mb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(
		ctx,
		`SELECT pe.event_id, pe.sequence_id, pe.instance_id, pe.execution_id, pe.event_type, pe.timestamp, pe.schedule_event_id, pe.visible_at, a.data
			FROM pending_events pe
			JOIN attributes a ON a.event_id = pe.event_id AND a.instance_id = pe.instance_id AND a.execution_id = pe.execution_id
			WHERE pe.visible_at IS NOT NULL`,
	)
	if err != nil {
		return nil, fmt.Errorf("getting future events: %w", err)
	}
	defer rows.Close()

	futureEvents := make([]*history.Event, 0)
	for rows.Next() {
		var instanceID, executionID string
		var attributes []byte
		event := &history.Event{}

		if err := rows.Scan(
			&event.ID,
			&event.SequenceID,
			&instanceID,
			&executionID,
			&event.Type,
			&event.Timestamp,
			&event.ScheduleEventID,
			&event.VisibleAt,
			&attributes,
		); err != nil {
			return nil, fmt.Errorf("scanning future event: %w", err)
		}

		a, err := history.DeserializeAttributes(event.Type, attributes)
		if err != nil {
			return nil, fmt.Errorf("deserializing future event attributes: %w", err)
		}
		event.Attributes = a

		futureEvents = append(futureEvents, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return futureEvents, nil
}

func newTestBackend(t *testing.T, options ...backend.BackendOption) *mysqlBackend {
	t.Helper()

	dbName := createTestDatabase(t)
	t.Cleanup(func() { dropTestDatabase(t, dbName) })

	options = append(options, backend.WithStickyTimeout(0))
	b := NewMysqlBackend(testHost, testPort, testUser, testPassword, dbName, WithBackendOptions(options...))
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	return b
}

func createTestDatabase(t *testing.T) string {
	t.Helper()

	db := openServerDB(t)
	defer db.Close()

	dbName := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err := db.Exec("CREATE DATABASE " + dbName + " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
	require.NoError(t, err)

	return dbName
}

func dropTestDatabase(t *testing.T, dbName string) {
	t.Helper()

	db := openServerDB(t)
	defer db.Close()

	_, err := db.Exec("DROP DATABASE IF EXISTS " + dbName)
	require.NoError(t, err)
}

func openServerDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&interpolateParams=true", testUser, testPassword, testHost, testPort))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("mysql test server unavailable: %v", err)
	}

	return db
}

func createWorkflow(t *testing.T, ctx context.Context, b *mysqlBackend, queue workflow.Queue) *workflow.Instance {
	t.Helper()

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := b.CreateWorkflowInstance(ctx, wfi, history.NewHistoryEvent(1, time.Now(), history.EventType_WorkflowExecutionStarted, &history.ExecutionStartedAttributes{
		Queue: queue,
	}))
	require.NoError(t, err)

	return wfi
}

func scheduleActivityTask(t *testing.T, ctx context.Context, b *mysqlBackend, workflowQueue workflow.Queue, activityQueue workflow.Queue, visibleAt *time.Time) *workflow.Instance {
	t.Helper()

	startedEvent := history.NewHistoryEvent(1, time.Now(), history.EventType_WorkflowExecutionStarted, &history.ExecutionStartedAttributes{
		Queue: workflowQueue,
	})
	activityScheduledEvent := history.NewPendingEvent(time.Now(), history.EventType_ActivityScheduled, &history.ActivityScheduledAttributes{
		Queue: activityQueue,
	}, history.ScheduleEventID(1))
	activityScheduledEvent.VisibleAt = visibleAt

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	require.NoError(t, b.CreateWorkflowInstance(ctx, wfi, startedEvent))

	task, err := b.GetWorkflowTask(ctx, []workflow.Queue{workflowQueue})
	require.NoError(t, err)
	require.NotNil(t, task)

	events := []*history.Event{
		history.NewPendingEvent(time.Now(), history.EventType_WorkflowTaskStarted, &history.WorkflowTaskStartedAttributes{}),
		startedEvent,
		activityScheduledEvent,
	}
	for i, event := range events {
		event.SequenceID = int64(i + 1)
	}

	require.NoError(t, b.CompleteWorkflowTask(ctx, task, core.WorkflowInstanceStateActive, events, []*history.Event{activityScheduledEvent}, nil, nil))

	return wfi
}

func collectErrors(errs <-chan error) []error {
	var out []error
	for err := range errs {
		out = append(out, err)
	}
	return out
}
