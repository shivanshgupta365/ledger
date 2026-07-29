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

	// world → acc:1 in USD/2, so both hold a USD/2 (base "USD", precision 2)
	// volume cell; neither holds EUR.
	ls := buildLedger(t, oracletest.TxReq("world", "acc:1", "USD/2", 5))

	require.True(t, matchAccountFilter(ls, nil, "anything")) // universe

	require.True(t, matchAccountFilter(ls, filterAddrPrefix("acc:"), "acc:1"))
	require.False(t, matchAccountFilter(ls, filterAddrPrefix("acc:"), "world"))

	require.True(t, matchAccountFilter(ls, filterAddrExact("acc:1"), "acc:1"))
	require.False(t, matchAccountFilter(ls, filterAddrExact("acc:1"), "acc:2"))

	require.True(t, matchAccountFilter(ls, filterAnd(filterAddrPrefix("acc:"), filterAddrExact("acc:1")), "acc:1"))
	require.False(t, matchAccountFilter(ls, filterAnd(filterAddrPrefix("acc:"), filterAddrExact("acc:1")), "acc:2"))

	require.True(t, matchAccountFilter(ls, filterOr(filterAddrExact("acc:1"), filterAddrExact("acc:2")), "acc:2"))
	require.False(t, matchAccountFilter(ls, filterOr(filterAddrExact("acc:1"), filterAddrExact("acc:2")), "acc:3"))

	require.True(t, matchAccountFilter(ls, filterNot(filterAddrPrefix("acc:")), "world"))
	require.False(t, matchAccountFilter(ls, filterNot(filterAddrPrefix("acc:")), "acc:1"))

	// has-asset: matches an account with a volume cell in the (base, precision).
	require.True(t, matchAccountFilter(ls, filterHasAsset("USD", 2), "acc:1"))
	require.True(t, matchAccountFilter(ls, filterHasAsset("USD", 2), "world"))
	require.False(t, matchAccountFilter(ls, filterHasAsset("EUR", 2), "acc:1"))
	require.False(t, matchAccountFilter(ls, filterHasAsset("USD", 2), "acc:absent"))
	// Composed with an index-free address leaf.
	require.True(t, matchAccountFilter(ls, filterAnd(filterHasAsset("USD", 2), filterAddrPrefix("acc:")), "acc:1"))
	require.False(t, matchAccountFilter(ls, filterAnd(filterHasAsset("USD", 2), filterAddrPrefix("acc:")), "world"))

	// Empty And/Or match nothing, mirroring the compiler's empty iterator.
	require.False(t, matchAccountFilter(ls, filterAnd(), "acc:1"))
	require.False(t, matchAccountFilter(ls, filterOr(), "acc:1"))
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

	// These leaves never read a server stamp, so every verdict must be known.
	matchKnown := func(f *commonpb.QueryFilter, rec txRecordView) bool {
		m, known := matchTxFilter(f, rec)
		require.True(t, known)

		return m
	}

	require.True(t, matchKnown(filterReverted(true), txs[0]))
	require.False(t, matchKnown(filterReverted(false), txs[0]))
	require.True(t, matchKnown(filterReverted(false), txs[1]))

	require.True(t, matchKnown(filterTxIDRange(1, 2), txs[0]))
	require.False(t, matchKnown(filterTxIDRange(1, 2), txs[2]))

	require.True(t, matchKnown(filterNot(filterReverted(true)), txs[1]))
	require.True(t, matchKnown(filterAnd(filterReverted(true), filterTxIDRange(1, 2)), txs[0]))
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

// --- Phase 2: tx-builtin leaves and the fuzzy window ----------------------

func stamp(v uint64) *commonpb.Timestamp { return &commonpb.Timestamp{Data: v} }

// buildGlobal applies reqs as one bulk and returns the global state, for tests
// that need LearnTxStamps on top of the applied records.
func buildGlobal(t *testing.T, reqs ...*servicepb.Request) oracle.GlobalState {
	t.Helper()

	res := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: reqs})
	require.True(t, res.OK, "setup bulk rejected: %s", res.Reason)

	return res.State
}

// serverTxFromRec builds the wire transaction the server would return for a
// fully-known model record, so txWindowMatches' content check passes.
func serverTxFromRec(rec txRecordView) *commonpb.Transaction {
	return &commonpb.Transaction{
		Id:                    rec.Id(),
		Reference:             rec.Reference(),
		Reverted:              rec.Reverted(),
		RevertedByTransaction: rec.RevertedBy(),
		RevertsTransaction:    rec.RevertsTransaction(),
		Timestamp:             rec.Timestamp(),
		RevertedAt:            rec.RevertedAt(),
		Postings:              rec.Postings(),
		Metadata:              rec.Metadata(),
	}
}

func TestMatchTxFilter_TxBuiltinLeaves(t *testing.T) {
	t.Parallel()

	// tx 1 (ref r1), tx 2, tx 3 = revert of 2. Stamps learned as the checker
	// would from the commit response; tx 2's reverted_at is tx 3's timestamp.
	gs := buildGlobal(t,
		oracletest.TxReqRefL("L", "r1", "world", "acc:1", "USD", 5),
		oracletest.TxReqL("L", "world", "acc:2", "USD", 5),
		oracletest.RevertReqL("L", 2, true),
	)
	gs.LearnTxStamps("L", 1, stamp(100), stamp(110), nil)
	gs.LearnTxStamps("L", 2, stamp(200), stamp(210), nil)
	gs.LearnTxStamps("L", 3, stamp(300), stamp(310), nil)
	gs.LearnTxStamps("L", 2, nil, nil, stamp(300))

	txs := gs.Ledger("L").Txs()

	known := func(f *commonpb.QueryFilter, rec txRecordView) bool {
		m, k := matchTxFilter(f, rec)
		require.True(t, k)

		return m
	}

	// Reference: exact match; records without one never match, nor does "".
	require.True(t, known(filterReference("r1"), txs[0]))
	require.False(t, known(filterReference("r1"), txs[1]))
	require.False(t, known(filterReference("absent"), txs[0]))
	require.False(t, known(filterReference(""), txs[2]))

	// Learned timestamp / inserted_at ranges.
	require.False(t, known(filterDateRange(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, 150, 250), txs[0]))
	require.True(t, known(filterDateRange(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, 150, 250), txs[1]))
	require.True(t, known(filterDateRange(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, 305, 315), txs[2]))

	// reverted_at: only the reverted original matches; un-reverted is a known miss.
	rvat := filterDateRange(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT, 250, 350)
	require.True(t, known(rvat, txs[1]))
	require.False(t, known(rvat, txs[0]))
	require.False(t, known(rvat, txs[2]))
}

func TestMatchTxFilter_UnknownStamps(t *testing.T) {
	t.Parallel()

	// No stamps learned: date verdicts are unknown, and booleans propagate
	// Kleene-style — decided by a known false (AND) or known true (OR).
	ls := buildLedger(t, oracletest.TxReq("world", "acc:1", "USD", 5))
	rec := ls.Txs()[0]

	tsLeaf := filterDateRange(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, 1, 2)

	_, k := matchTxFilter(tsLeaf, rec)
	require.False(t, k)

	_, k = matchTxFilter(filterNot(tsLeaf), rec)
	require.False(t, k)

	m, k := matchTxFilter(filterAnd(filterReverted(true), tsLeaf), rec)
	require.True(t, k, "AND decided by the known-false reverted leaf")
	require.False(t, m)

	m, k = matchTxFilter(filterOr(filterReverted(false), tsLeaf), rec)
	require.True(t, k, "OR decided by the known-true reverted leaf")
	require.True(t, m)

	_, k = matchTxFilter(filterOr(filterReverted(true), tsLeaf), rec)
	require.False(t, k, "OR undecided when the known leaf misses")
}

func TestTxWindowMatches_OptionalRows(t *testing.T) {
	t.Parallel()

	// Three creates; stamps learned for 1 and 3 only — under a date filter
	// covering both, tx 2 is an optional row (unknown verdict).
	gs := buildGlobal(t,
		oracletest.TxReqL("L", "world", "acc:1", "USD", 5),
		oracletest.TxReqL("L", "world", "acc:2", "USD", 5),
		oracletest.TxReqL("L", "world", "acc:3", "USD", 5),
	)
	gs.LearnTxStamps("L", 1, stamp(100), stamp(100), nil)
	gs.LearnTxStamps("L", 3, stamp(300), stamp(300), nil)

	ls := gs.Ledger("L")
	txs := ls.Txs()
	filter := filterDateRange(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, 50, 400)

	// reverse=true is ascending on the transactions target.
	page := func(ids ...uint64) []*commonpb.Transaction {
		out := make([]*commonpb.Transaction, len(ids))
		for i, id := range ids {
			out[i] = serverTxFromRec(txs[id-1])
		}

		return out
	}

	require.True(t, txWindowMatches(ls, filter, 0, 10, true, page(1, 3)), "optional row absent")
	require.True(t, txWindowMatches(ls, filter, 0, 10, true, page(1, 2, 3)), "optional row present")
	require.False(t, txWindowMatches(ls, filter, 0, 10, true, page(3, 1)), "order violation")
	require.False(t, txWindowMatches(ls, filter, 0, 10, true, page(1)), "required row missing with page room")
	require.True(t, txWindowMatches(ls, filter, 0, 1, true, page(1)), "full page truncates the rest")
	require.False(t, txWindowMatches(ls, filter, 0, 10, true, page(2, 3)), "required first row missing")
	require.True(t, txWindowMatches(ls, filter, 1, 10, true, page(2, 3)), "cursor drops tx 1; optional 2 present")
	require.True(t, txWindowMatches(ls, filter, 1, 10, true, page(3)), "cursor drops tx 1; optional 2 absent")
}
