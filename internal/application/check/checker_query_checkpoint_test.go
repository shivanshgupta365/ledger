package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeQueryCheckpointRow persists a query-checkpoint row exactly as the FSM's
// saveQueryCheckpoint does (ZoneGlobal/SubGlobQueryCheckpoint, id BE tail).
func writeQueryCheckpointRow(t *testing.T, store *dal.Store, id uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	batch.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint).PutUint64(id)
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), &raftcmdpb.QueryCheckpointState{CheckpointId: id}))
	require.NoError(t, batch.Commit())
}

// collectQueryCheckpointEvents runs compareQueryCheckpoints against the store's
// live checkpoint rows with the given audit-derived live set and returns only
// the QUERY_CHECKPOINT_MISMATCH errors.
func collectQueryCheckpointEvents(t *testing.T, store *dal.Store, derived map[uint64]struct{}) []*servicepb.CheckStoreError {
	t.Helper()

	checker := NewChecker(store, attributes.New(), "query-checkpoint-cluster", nil, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	var got []*servicepb.CheckStoreError

	require.NoError(t, checker.compareQueryCheckpoints(handle, derived, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
			e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_QUERY_CHECKPOINT_MISMATCH {
			got = append(got, e.Error)
		}
	}))

	return got
}

// TestCompareQueryCheckpoints_PhantomFlagged: a stored row the audit chain never
// created (or later deleted) is corruption and must emit exactly one mismatch.
func TestCompareQueryCheckpoints_PhantomFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRow(t, store, 5)

	events := collectQueryCheckpointEvents(t, store, map[uint64]struct{}{})
	require.Len(t, events, 1)
	require.Contains(t, events[0].GetMessage(), "5")
}

// TestCompareQueryCheckpoints_ConsistentPasses: every stored row justified by
// the derived set emits nothing.
func TestCompareQueryCheckpoints_ConsistentPasses(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeQueryCheckpointRow(t, store, 5)
	writeQueryCheckpointRow(t, store, 6)

	events := collectQueryCheckpointEvents(t, store, map[uint64]struct{}{5: {}, 6: {}})
	require.Empty(t, events)
}

// TestCompareQueryCheckpoints_MissingRowNotFlagged: an audit-derived checkpoint
// with no stored row is a legitimate post-restore state (checkpoints are not
// rebuilt on restore), so the reverse direction is intentionally not flagged.
func TestCompareQueryCheckpoints_MissingRowNotFlagged(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	events := collectQueryCheckpointEvents(t, store, map[uint64]struct{}{7: {}})
	require.Empty(t, events)
}
