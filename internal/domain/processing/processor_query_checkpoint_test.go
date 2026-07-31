package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func createQueryCheckpointOrder() *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		},
	}
}

func deleteQueryCheckpointOrder(id uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint{
					DeleteQueryCheckpoint: &raftcmdpb.DeleteQueryCheckpointOrder{CheckpointId: id},
				},
			},
		},
	}
}

func TestProcessCreateQueryCheckpoint_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 42}

	mockStore.EXPECT().QueryCheckpointCount().Return(MaxLiveQueryCheckpoints - 1)
	mockStore.EXPECT().IncrementNextQueryCheckpointID().Return(uint64(7))
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	mockStore.EXPECT().SaveQueryCheckpoint(gomock.Any())

	result, err := processor.ProcessOrder(createQueryCheckpointOrder(), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createdLog := result.GetCreatedQueryCheckpoint()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(7), createdLog.GetCheckpointId())
	require.Equal(t, uint64(99), createdLog.GetMaxSequence())
}

func TestProcessCreateQueryCheckpoint_LimitReached(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// At the cap, creation is rejected before any ID is allocated: no
	// IncrementNextQueryCheckpointID / SaveQueryCheckpoint calls are expected.
	mockStore.EXPECT().QueryCheckpointCount().Return(MaxLiveQueryCheckpoints)

	result, err := processor.ProcessOrder(createQueryCheckpointOrder(), mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var limitErr *domain.ErrCheckpointLimitReached
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, uint64(MaxLiveQueryCheckpoints), limitErr.Limit)
}

func TestProcessDeleteQueryCheckpoint_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().QueryCheckpointExists(uint64(5)).Return(true)
	mockStore.EXPECT().DeleteQueryCheckpoint(uint64(5))

	result, err := processor.ProcessOrder(deleteQueryCheckpointOrder(5), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deletedLog := result.GetDeletedQueryCheckpoint()
	require.NotNil(t, deletedLog)
	require.Equal(t, uint64(5), deletedLog.GetCheckpointId())
}

func TestProcessDeleteQueryCheckpoint_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// A non-live id must not tombstone or emit a log: only the existence check
	// runs, then the typed not-found error is returned.
	mockStore.EXPECT().QueryCheckpointExists(uint64(5)).Return(false)

	result, err := processor.ProcessOrder(deleteQueryCheckpointOrder(5), mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var notFoundErr *domain.ErrCheckpointNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(5), notFoundErr.CheckpointID)
}

func TestProcessDeleteQueryCheckpoint_IDRequired(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	result, err := processor.ProcessOrder(deleteQueryCheckpointOrder(0), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, domain.ErrCheckpointIDRequired)
}
