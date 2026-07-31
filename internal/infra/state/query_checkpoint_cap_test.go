package state

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// TestWriteSetQueryCheckpointOverlayAndMerge pins the deterministic live-count
// overlay: QueryCheckpointCount/Exists reflect uncommitted creates and deletes,
// Merge swaps the set into FSMState and persists the Pebble rows, and a later
// proposal sees the committed set.
func TestWriteSetQueryCheckpointOverlayAndMerge(t *testing.T) {
	t.Parallel()
	buf, machine, dataStore := newTestBuffer(t)

	require.Equal(t, 0, buf.QueryCheckpointCount())
	require.False(t, buf.QueryCheckpointExists(1))

	buf.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{CheckpointId: 1})
	buf.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{CheckpointId: 2})

	require.Equal(t, 2, buf.QueryCheckpointCount())
	require.True(t, buf.QueryCheckpointExists(1))
	require.True(t, buf.QueryCheckpointExists(2))
	// Uncommitted: FSMState is untouched until Merge.
	require.Empty(t, machine.State.LiveQueryCheckpointIDs)

	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	require.Equal(t, map[uint64]struct{}{1: {}, 2: {}}, machine.State.LiveQueryCheckpointIDs)

	rh, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rh.Close() })

	ids, err := query.ReadLiveQueryCheckpointIDs(rh)
	require.NoError(t, err)
	require.Equal(t, map[uint64]struct{}{1: {}, 2: {}}, ids, "recovery read must see the committed rows")

	// Proposal 2: the committed set is visible, and deleting one frees a slot.
	buf2 := NewWriteSet(machine)
	buf2.Reset(&commonpb.Timestamp{Data: 1700000001})

	require.Equal(t, 2, buf2.QueryCheckpointCount())
	require.True(t, buf2.QueryCheckpointExists(1))

	buf2.DeleteQueryCheckpoint(1)
	require.Equal(t, 1, buf2.QueryCheckpointCount())
	require.False(t, buf2.QueryCheckpointExists(1))

	batch2 := dataStore.OpenWriteSession()
	require.NoError(t, buf2.Merge(batch2, nil))
	require.NoError(t, batch2.Commit())

	require.Equal(t, map[uint64]struct{}{2: {}}, machine.State.LiveQueryCheckpointIDs)

	rh2, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rh2.Close() })

	ids2, err := query.ReadLiveQueryCheckpointIDs(rh2)
	require.NoError(t, err)
	require.Equal(t, map[uint64]struct{}{2: {}}, ids2)
}

// TestWriteSetQueryCheckpointRollback pins that a proposal aborted via Reset
// (no Merge) leaves the committed live-checkpoint set untouched.
func TestWriteSetQueryCheckpointRollback(t *testing.T) {
	t.Parallel()
	buf, machine, _ := newTestBuffer(t)

	buf.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{CheckpointId: 9})
	require.True(t, buf.QueryCheckpointExists(9))

	buf.Reset(&commonpb.Timestamp{Data: 1700000002})

	require.Equal(t, 0, buf.QueryCheckpointCount())
	require.Empty(t, machine.State.LiveQueryCheckpointIDs)
}

// TestIsCheckpointLimitReached pins the scheduler's reason classification: only
// the typed CHECKPOINT_LIMIT_REACHED business rejection is treated as the
// expected cap steady-state; every other error (and nil) is not.
func TestIsCheckpointLimitReached(t *testing.T) {
	t.Parallel()

	require.True(t, isCheckpointLimitReached(&domain.ErrCheckpointLimitReached{Limit: 10}))
	require.True(t, isCheckpointLimitReached(&domain.BusinessError{Err: &domain.ErrCheckpointLimitReached{Limit: 10}}))
	require.True(t, isCheckpointLimitReached(fmt.Errorf("propose: %w", &domain.ErrCheckpointLimitReached{Limit: 10})))

	require.False(t, isCheckpointLimitReached(&domain.ErrCheckpointNotFound{CheckpointID: 3}))
	require.False(t, isCheckpointLimitReached(errors.New("transient")))
	require.False(t, isCheckpointLimitReached(nil))
}
