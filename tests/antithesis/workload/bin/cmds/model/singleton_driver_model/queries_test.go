package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// buildLedger applies reqs as one committed bulk on an empty-chart ledger "L"
// and returns its state. An empty chart disables account-type enforcement, so
// transactions to arbitrary addresses commit and populate volumes.
func buildLedger(t *testing.T, reqs ...*servicepb.Request) oracle.LedgerState {
	t.Helper()

	res := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: reqs})
	require.True(t, res.OK, "setup bulk rejected: %s", res.Reason)

	return res.State.Ledger("L")
}

const (
	accounts = commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	txns     = commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
)

func TestFilterNeedsIndex(t *testing.T) {
	t.Parallel()

	require.False(t, filterNeedsIndex(nil, accounts))
	require.False(t, filterNeedsIndex(nil, txns))

	// Address is index-free on accounts, index-backed on transactions.
	require.False(t, filterNeedsIndex(filterAddrPrefix("acc:"), accounts))
	require.True(t, filterNeedsIndex(filterAddrPrefix("acc:"), txns))

	require.False(t, filterNeedsIndex(filterReverted(true), txns))
	require.False(t, filterNeedsIndex(filterTxIDRange(1, 5), txns))

	// Metadata fields are index-backed on either target.
	require.True(t, filterNeedsIndex(filterMetaExists("k"), accounts))
	require.True(t, filterNeedsIndex(filterMetaExists("k"), txns))

	// Boolean nodes propagate their children's index needs.
	require.False(t, filterNeedsIndex(filterAnd(filterReverted(true), filterTxIDRange(1, 5)), txns))
	require.True(t, filterNeedsIndex(filterAnd(filterReverted(true), filterMetaExists("k")), txns))
	require.True(t, filterNeedsIndex(filterNot(filterMetaExists("k")), accounts))
	require.False(t, filterNeedsIndex(filterNot(filterReverted(true)), txns))
}

func TestFilterInvalidForTarget(t *testing.T) {
	t.Parallel()

	// Transactions-only conditions are invalid on the accounts target.
	require.True(t, filterInvalidForTarget(filterReverted(true), accounts))
	require.True(t, filterInvalidForTarget(filterTxIDRange(1, 5), accounts))
	// The accounts-only condition is invalid on the transactions target.
	require.True(t, filterInvalidForTarget(filterHasAsset("USD", 2), txns))

	// Each is valid on its own target.
	require.False(t, filterInvalidForTarget(filterReverted(true), txns))
	require.False(t, filterInvalidForTarget(filterTxIDRange(1, 5), txns))
	require.False(t, filterInvalidForTarget(filterHasAsset("USD", 2), accounts))

	// Address is valid on both targets; nil (universe) is always valid.
	require.False(t, filterInvalidForTarget(filterAddrPrefix("acc:"), accounts))
	require.False(t, filterInvalidForTarget(filterAddrPrefix("acc:"), txns))
	require.False(t, filterInvalidForTarget(nil, accounts))

	// Combinators propagate an invalid child; a fully-valid tree stays valid.
	require.True(t, filterInvalidForTarget(filterAnd(filterAddrExact("acc:1"), filterReverted(true)), accounts))
	require.True(t, filterInvalidForTarget(filterNot(filterReverted(true)), accounts))
	require.False(t, filterInvalidForTarget(filterAnd(filterAddrExact("acc:1"), filterAddrPrefix("x:")), accounts))
}

func TestMatchAccountFilter(t *testing.T) {
	t.Parallel()

	require.True(t, matchAccountFilter(nil, "anything")) // universe

	require.True(t, matchAccountFilter(filterAddrPrefix("acc:"), "acc:1"))
	require.False(t, matchAccountFilter(filterAddrPrefix("acc:"), "world"))

	require.True(t, matchAccountFilter(filterAddrExact("acc:1"), "acc:1"))
	require.False(t, matchAccountFilter(filterAddrExact("acc:1"), "acc:2"))

	require.True(t, matchAccountFilter(filterAnd(filterAddrPrefix("acc:"), filterAddrExact("acc:1")), "acc:1"))
	require.False(t, matchAccountFilter(filterAnd(filterAddrPrefix("acc:"), filterAddrExact("acc:1")), "acc:2"))

	require.True(t, matchAccountFilter(filterOr(filterAddrExact("acc:1"), filterAddrExact("acc:2")), "acc:2"))
	require.False(t, matchAccountFilter(filterOr(filterAddrExact("acc:1"), filterAddrExact("acc:2")), "acc:3"))

	require.True(t, matchAccountFilter(filterNot(filterAddrPrefix("acc:")), "world"))
	require.False(t, matchAccountFilter(filterNot(filterAddrPrefix("acc:")), "acc:1"))

	// Empty And/Or match nothing, mirroring the compiler's empty iterator.
	require.False(t, matchAccountFilter(filterAnd(), "acc:1"))
	require.False(t, matchAccountFilter(filterOr(), "acc:1"))
}

func TestMatchTxIDBounds(t *testing.T) {
	t.Parallel()

	lo, hi := uint64(2), uint64(4)
	inclusive := &commonpb.UintCondition{Min: &lo, Max: &hi}
	require.False(t, matchTxIDBounds(inclusive, 1))
	require.True(t, matchTxIDBounds(inclusive, 2))
	require.True(t, matchTxIDBounds(inclusive, 4))
	require.False(t, matchTxIDBounds(inclusive, 5))

	exclusive := &commonpb.UintCondition{Min: &lo, Max: &hi, MinExclusive: true, MaxExclusive: true}
	require.False(t, matchTxIDBounds(exclusive, 2))
	require.True(t, matchTxIDBounds(exclusive, 3))
	require.False(t, matchTxIDBounds(exclusive, 4))

	openMax := &commonpb.UintCondition{Min: &lo}
	require.True(t, matchTxIDBounds(openMax, 1000))
	require.False(t, matchTxIDBounds(openMax, 1))
}

func TestMatchTxFilter(t *testing.T) {
	t.Parallel()

	// Two funding transactions, then revert the first: tx 1 is reverted, tx 3 is
	// the compensating transaction (tx 2 is the second funding tx).
	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD", 5),
		oracletest.TxReq("world", "acc:2", "USD", 5),
		oracletest.RevertReqL("L", 1, true),
	)
	txs := ls.Txs()
	require.Len(t, txs, 3)

	require.True(t, matchTxFilter(filterReverted(true), txs[0]))
	require.False(t, matchTxFilter(filterReverted(false), txs[0]))
	require.True(t, matchTxFilter(filterReverted(false), txs[1]))

	require.True(t, matchTxFilter(filterTxIDRange(1, 2), txs[0]))
	require.False(t, matchTxFilter(filterTxIDRange(1, 2), txs[2]))

	require.True(t, matchTxFilter(filterNot(filterReverted(true)), txs[1]))
	require.True(t, matchTxFilter(filterAnd(filterReverted(true), filterTxIDRange(1, 2)), txs[0]))
}

func TestAccountUniverse_OrderIsByteAscending(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:2", "USD", 5),
		oracletest.TxReq("world", "acc:10", "USD", 5),
	)

	// "acc:10" < "acc:2" bytewise ('1' < '2'), and both precede "world".
	require.Equal(t, []string{"acc:10", "acc:2", "world"}, accountUniverse(ls))
}

func TestAccountWindow(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD", 5),
		oracletest.TxReq("world", "acc:2", "USD", 5),
		oracletest.TxReq("world", "acc:3", "USD", 5),
	)
	// Universe: acc:1, acc:2, acc:3, world.

	require.Equal(t, []string{"acc:1", "acc:2"},
		accountWindow(ls, nil, "", 2, false))

	// Cursor is exclusive: forward drops addresses <= cursor.
	require.Equal(t, []string{"acc:2", "acc:3", "world"},
		accountWindow(ls, nil, "acc:1", 10, false))

	require.Equal(t, []string{"world", "acc:3", "acc:2", "acc:1"},
		accountWindow(ls, nil, "", 10, true))

	// Reverse cursor drops addresses >= cursor.
	require.Equal(t, []string{"acc:2", "acc:1"},
		accountWindow(ls, nil, "acc:3", 10, true))

	// An address filter excludes world.
	require.Equal(t, []string{"acc:1", "acc:2", "acc:3"},
		accountWindow(ls, filterAddrPrefix("acc:"), "", 10, false))
}

func TestTransactionWindow(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD", 1),
		oracletest.TxReq("world", "acc:2", "USD", 1),
		oracletest.TxReq("world", "acc:3", "USD", 1),
		oracletest.TxReq("world", "acc:4", "USD", 1),
	)
	// Transactions: ids 1, 2, 3, 4. The endpoint inverts reverse: reverse=false
	// is newest-first (descending); reverse=true is oldest-first (ascending).

	// reverse=false → descending, newest first.
	require.Equal(t, []uint64{4, 3},
		transactionWindow(ls, nil, 0, 2, false))

	// Descending cursor is exclusive older-than: drops ids >= afterID.
	require.Equal(t, []uint64{1},
		transactionWindow(ls, nil, 2, 10, false))

	// reverse=true → ascending, oldest first.
	require.Equal(t, []uint64{1, 2, 3, 4},
		transactionWindow(ls, nil, 0, 10, true))

	// Ascending cursor is exclusive newer-than: drops ids <= afterID.
	require.Equal(t, []uint64{4},
		transactionWindow(ls, nil, 3, 10, true))

	// Filtered, reverse=false → matches ids 2,3 in descending order.
	require.Equal(t, []uint64{3, 2},
		transactionWindow(ls, filterTxIDRange(2, 3), 0, 10, false))
}
