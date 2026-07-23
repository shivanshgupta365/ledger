package main

import (
	"context"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Query reads exercise ListAccounts / ListTransactions — the filtered,
// paginated, ordered read surface. A list page is a deterministic ordered
// window: given the filter, the sort order, the cursor, the page size, and the
// reverse flag, exactly one slice of the matching entities is correct. The
// model reproduces that slice over a candidate base and checks the streamed
// page element-for-element, so both membership and order are validated.
//
// Ordering (server): accounts by address bytes, transactions by id, ascending;
// reverse flips it. The cursor is exclusive in both directions — forward drops
// keys <= cursor, reverse drops keys >= cursor (PaginateForward / listDescFiltered).
//
// Index gating: the server's filter compiler serves most conditions from a
// created index and returns NotFound when the index is absent. This workload
// builds none, so a filter carrying any index-backed condition (see
// filterNeedsIndex) is predicted to be rejected with NotFound rather than to
// return rows. The index-free conditions — universe, address-on-accounts,
// reverted, the tx-id builtin, and boolean compositions of these — return the
// window the model computes.

// queryPageSize is the page size a query read requests. Kept well under
// MaxPageSize (1000) so the server never clamps it — the model uses the same
// value to size its window, and a clamp would desync the two.
func queryPageSize() int {
	return int(random.RandomChoice([]uint8{1, 2, 3, 5, 10, 50}))
}

// runAccountQuery issues a linearizable ListAccounts and checks the streamed
// page against the model's ordered window (see validateAccountQuery). One-in-six
// queries carry an index-backed filter to exercise the NotFound rejection path.
func runAccountQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)
	filter := genAccountFilter()
	needsIndex := filterNeedsIndex(filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	invalidTarget := filterInvalidForTarget(filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	pageSize := queryPageSize()
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	var cursor string
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		cursor = poolAddress()
	}

	c.mu.Lock()
	readID := c.registerRead()
	minSeq := c.committedSeq
	c.mu.Unlock()
	defer c.finishRead(readID)

	// Pin the read to the model's committed frontier: the server snapshot is then
	// at least modelState, so the ordered window is representable by a candidate
	// base (a snapshot behind modelState would not be — see committedSeq).
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	stream, err := client.ListAccounts(readCtx, &servicepb.ListAccountsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Cursor:   cursor,
			Reverse:  reverse,
			Filter:   filter,
			Read:     &commonpb.ReadOptions{MinLogSequence: minSeq},
		},
	})

	var accounts []*commonpb.Account
	if err == nil {
		accounts, err = drainStream(stream)
	}

	// High-water at the read's completion: only bulks dispatched by now could be
	// reflected in the page. Captured before validation so later dispatches
	// aren't folded into this read's candidate states.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if handleInvalidTargetError(invalidTarget, "account", ledger, filter, err) {
			return
		}
		if handleIndexGatedError(needsIndex, "account", ledger, filter, err) {
			return
		}

		assert.Unreachable("singleton_driver_model: ListAccounts returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	if invalidTarget {
		// The filter carries a condition invalid on this target; the server must
		// reject it, not stream rows.
		assert.Unreachable("singleton_driver_model: target-invalid account query returned results", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"rows":   len(accounts),
		})

		return
	}

	if needsIndex {
		// Predicted NotFound but the server streamed rows: an index-backed filter
		// returned results without the index the compiler requires.
		assert.Unreachable("singleton_driver_model: index-gated account query returned results", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"rows":   len(accounts),
		})

		return
	}

	c.validateAccountQuery(maxTicket, ledger, filter, cursor, pageSize, reverse, accounts)
}

// runTransactionQuery issues a linearizable ListTransactions and checks the
// streamed page against the model's ordered window (see validateTransactionQuery).
// One-in-six queries carry an index-backed filter to exercise the NotFound path.
func runTransactionQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)
	filter := genTransactionFilter()
	needsIndex := filterNeedsIndex(filter, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	invalidTarget := filterInvalidForTarget(filter, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	pageSize := queryPageSize()
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	var (
		cursor  string
		afterID uint64
	)
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		afterID = 1 + internal.Rand().Uint64()%256
		cursor = strconv.FormatUint(afterID, 10)
	}

	c.mu.Lock()
	readID := c.registerRead()
	minSeq := c.committedSeq
	c.mu.Unlock()
	defer c.finishRead(readID)

	// Pin the read to the model's committed frontier: the server snapshot is then
	// at least modelState, so the ordered window is representable by a candidate
	// base (a snapshot behind modelState would not be — see committedSeq).
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	stream, err := client.ListTransactions(readCtx, &servicepb.ListTransactionsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Cursor:   cursor,
			Reverse:  reverse,
			Filter:   filter,
			Read:     &commonpb.ReadOptions{MinLogSequence: minSeq},
		},
	})

	var txs []*commonpb.Transaction
	if err == nil {
		txs, err = drainStream(stream)
	}

	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if handleInvalidTargetError(invalidTarget, "transaction", ledger, filter, err) {
			return
		}
		if handleIndexGatedError(needsIndex, "transaction", ledger, filter, err) {
			return
		}

		assert.Unreachable("singleton_driver_model: ListTransactions returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	if invalidTarget {
		assert.Unreachable("singleton_driver_model: target-invalid transaction query returned results", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"rows":   len(txs),
		})

		return
	}

	if needsIndex {
		assert.Unreachable("singleton_driver_model: index-gated transaction query returned results", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"rows":   len(txs),
		})

		return
	}

	c.validateTransactionQuery(maxTicket, ledger, filter, afterID, pageSize, reverse, txs)
}

// handleIndexGatedError validates the error of an index-backed query. Such a
// filter must be rejected with FailedPrecondition — ErrIndexNotFound and
// ErrIndexBuilding both carry a Kind that maps there (kindToGRPCCode) — since
// the workload never builds the index the compiler requires. Returns true when
// it has fully handled err (predicted rejection reached, or its own finding
// raised); false when err is not this path's concern.
func handleIndexGatedError(needsIndex bool, kind, ledger string, filter *commonpb.QueryFilter, err error) bool {
	if !needsIndex {
		return false
	}

	if status.Code(err) == codes.FailedPrecondition {
		// Coverage: the index-gating rejection is actually exercised. If this
		// stops firing, the generator has stopped emitting index-backed filters.
		assert.Reachable("singleton_driver_model: index-gated query rejected", internal.Details{"kind": kind})

		return true
	}

	assert.Unreachable("singleton_driver_model: index-gated query returned unexpected error", internal.Details{
		"kind":   kind,
		"ledger": ledger,
		"filter": describeFilter(filter),
		"error":  err.Error(),
	})

	return true
}

// handleInvalidTargetError validates the error of a filter carrying a condition
// invalid on its target (e.g. reverted on accounts, account-has-asset on
// transactions). The server's rejectInvalidCondition runs before index checks,
// so this must be consulted before handleIndexGatedError; the expected outcome
// is InvalidArgument. Returns true when it has fully handled err.
func handleInvalidTargetError(invalidTarget bool, kind, ledger string, filter *commonpb.QueryFilter, err error) bool {
	if !invalidTarget {
		return false
	}

	if status.Code(err) == codes.InvalidArgument {
		// Coverage: the per-target validity rejection is actually exercised.
		assert.Reachable("singleton_driver_model: target-invalid query rejected", internal.Details{"kind": kind})

		return true
	}

	assert.Unreachable("singleton_driver_model: target-invalid query returned unexpected error", internal.Details{
		"kind":   kind,
		"ledger": ledger,
		"filter": describeFilter(filter),
		"error":  err.Error(),
	})

	return true
}

// drainStream reads a server stream to exhaustion, returning every item in
// stream order. io.EOF is clean end-of-stream; any other error (a compile
// rejection surfaces on the first Recv) is returned alongside what was read.
func drainStream[T any](stream grpc.ServerStreamingClient[T]) ([]*T, error) {
	var out []*T
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}

		out = append(out, item)
	}
}

// validateAccountQuery checks a ListAccounts page against the model: legal iff
// some candidate base's ordered window — filtered, sorted, cursor-skipped,
// page-capped — equals the streamed accounts position-for-position, each row's
// address AND its whole volumes/metadata snapshot matching on that same base.
func (c *Checker) validateAccountQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool, serverAccts []*commonpb.Account) {
	if c.matchesModel(maxTicket, "AQUERY", func(base oracle.GlobalState) bool {
		ls := base.Ledger(ledger)
		want := accountWindow(ls, filter, cursor, pageSize, reverse)
		if len(want) != len(serverAccts) {
			return false
		}

		for i, addr := range want {
			if serverAccts[i].GetAddress() != addr || !accountMatches(ls, addr, serverAccts[i]) {
				return false
			}
		}

		return true
	}) {
		return
	}

	serverAddrs := make([]string, len(serverAccts))
	for i, a := range serverAccts {
		serverAddrs[i] = a.GetAddress()
	}

	assert.Unreachable("singleton_driver_model: account query outside model", internal.Details{
		"ledger":      ledger,
		"filter":      describeFilter(filter),
		"cursor":      cursor,
		"pageSize":    pageSize,
		"reverse":     reverse,
		"rows":        len(serverAccts),
		"serverAddrs": strings.Join(serverAddrs, ","),
		"modelAddrs":  strings.Join(c.modelAccountWindow(ledger, filter, cursor, pageSize, reverse), ","),
	})
}

// modelAccountWindow returns the account window on the committed modelState — the
// base with no in-flight bulks folded — for a finding's diagnostics. Acquires c.mu.
func (c *Checker) modelAccountWindow(ledger string, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return accountWindow(c.modelState.Ledger(ledger), filter, cursor, pageSize, reverse)
}

// validateTransactionQuery checks a ListTransactions page against the model:
// legal iff some candidate base's ordered window equals the streamed
// transactions position-for-position, each row matching the model record at its
// id (see txRecordMatches) on that same base.
func (c *Checker) validateTransactionQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool, serverTxs []*commonpb.Transaction) {
	if c.matchesModel(maxTicket, "TXQUERY", func(base oracle.GlobalState) bool {
		ls := base.Ledger(ledger)
		txs := ls.Txs()
		want := transactionWindow(ls, filter, afterID, pageSize, reverse)
		if len(want) != len(serverTxs) {
			return false
		}

		for i, id := range want {
			if serverTxs[i].GetId() != id || !txRecordMatches(txs[id-1], serverTxs[i]) {
				return false
			}
		}

		return true
	}) {
		return
	}

	serverIds := make([]uint64, len(serverTxs))
	for i, t := range serverTxs {
		serverIds[i] = t.GetId()
	}

	assert.Unreachable("singleton_driver_model: transaction query outside model", internal.Details{
		"ledger":    ledger,
		"filter":    describeFilter(filter),
		"afterId":   afterID,
		"pageSize":  pageSize,
		"reverse":   reverse,
		"rows":      len(serverTxs),
		"serverIds": joinUint64(serverIds),
		"modelIds":  joinUint64(c.modelTransactionWindow(ledger, filter, afterID, pageSize, reverse)),
	})
}

// modelTransactionWindow returns the transaction window on the committed
// modelState — the base with no in-flight bulks folded — for a finding's
// diagnostics. Acquires c.mu.
func (c *Checker) modelTransactionWindow(ledger string, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool) []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return transactionWindow(c.modelState.Ledger(ledger), filter, afterID, pageSize, reverse)
}

// accountWindow is the model's prediction of a ListAccounts page: the ledger's
// accounts matching filter, in address order (reversed when reverse), with the
// exclusive cursor applied and capped at pageSize. Because the list is sorted,
// dropping every key past the cursor equals the server's contiguous prefix skip.
func accountWindow(ls oracle.LedgerState, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool) []string {
	var window []string
	for _, addr := range accountUniverse(ls) {
		if matchAccountFilter(filter, addr) {
			window = append(window, addr)
		}
	}

	if reverse {
		reverseStrings(window)
	}

	if cursor != "" {
		kept := window[:0]
		for _, addr := range window {
			if reverse && addr >= cursor || !reverse && addr <= cursor {
				continue
			}

			kept = append(kept, addr)
		}
		window = kept
	}

	if len(window) > pageSize {
		window = window[:pageSize]
	}

	return window
}

// transactionWindow is the model's prediction of a ListTransactions page: the
// ledger's transactions matching filter, ordered, with the exclusive afterID
// cursor applied and capped at pageSize.
//
// The transactions endpoint INVERTS the API reverse flag (controller
// ListTransactionsFrom passes reverse=!reverse to listEntities): reverse=false
// yields newest-first (descending id), reverse=true yields oldest-first
// (ascending). This is the opposite of accounts, which follow reverse literally.
// Descending pages drop ids >= afterID (older-than cursor); ascending pages drop
// ids <= afterID (newer-than cursor) — mirroring PaginateReverse / PaginateForward.
func transactionWindow(ls oracle.LedgerState, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool) []uint64 {
	descending := !reverse

	var window []uint64
	for _, rec := range ls.Txs() {
		if matchTxFilter(filter, rec) {
			window = append(window, rec.Id())
		}
	}

	if descending {
		reverseUint64(window)
	}

	if afterID != 0 {
		kept := window[:0]
		for _, id := range window {
			if descending && id >= afterID || !descending && id <= afterID {
				continue
			}

			kept = append(kept, id)
		}
		window = kept
	}

	if len(window) > pageSize {
		window = window[:pageSize]
	}

	return window
}

// accountUniverse returns ls's account addresses — every address carrying a
// volume cell or a metadata entry — in ascending byte order, matching the
// server's merged V+M attribute scan (readstore.NewPebbleAccountIterator).
func accountUniverse(ls oracle.LedgerState) []string {
	seen := map[string]struct{}{}
	for k := range ls.Volumes() {
		seen[k.Address] = struct{}{}
	}
	for k := range ls.Metadata() {
		seen[k.Address] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for addr := range seen {
		out = append(out, addr)
	}

	sort.Strings(out)

	return out
}

// accountMatches reports whether the server account is exactly what the model
// holds for addr: the same uncolored volume cells (per asset, input and output)
// and the same metadata. The workload only exercises uncolored postings, so
// colored buckets are out of scope. metadataMatches is shared with the single
// GetAccount read.
func accountMatches(ls oracle.LedgerState, addr string, serverAcct *commonpb.Account) bool {
	if !metadataMatches(ls, addr, serverAcct.GetMetadata()) {
		return false
	}

	model := map[string]oracle.VolumePair{}
	for k, vp := range ls.Volumes() {
		if k.Address == addr {
			model[k.Asset] = vp
		}
	}

	server := map[string]struct{ in, out uint256.Int }{}
	for _, av := range serverAcct.GetVolumes() {
		if av.GetColor() != "" {
			continue
		}

		var in, out uint256.Int
		if err := in.SetFromDecimal(av.GetVolumes().GetInput()); err != nil {
			return false
		}
		if err := out.SetFromDecimal(av.GetVolumes().GetOutput()); err != nil {
			return false
		}

		server[av.GetAsset()] = struct{ in, out uint256.Int }{in, out}
	}

	if len(model) != len(server) {
		return false
	}

	for asset, vp := range model {
		sv, ok := server[asset]
		if !ok || vp.Input.Cmp(&sv.in) != 0 || vp.Output.Cmp(&sv.out) != 0 {
			return false
		}
	}

	return true
}

// txRecordView is the subset of oracle's (unexported) transaction record that
// the query and single-read validators compare against. Declaring the interface
// here lets both share txRecordMatches without oracle exporting the concrete type.
type txRecordView interface {
	Id() uint64
	Reference() string
	Postings() []*commonpb.Posting
	Metadata() map[string]*commonpb.MetadataValue
	Reverted() bool
	Timestamp() *commonpb.Timestamp
	RevertedBy() uint64
	RevertedAt() *commonpb.Timestamp
	RevertsTransaction() uint64
}

// txRecordMatches reports whether the model record rec is consistent with the
// server transaction: same id, reference, revert relationships, postings, and
// metadata. Timestamps follow the model's nil-means-server-dated convention — a
// nil model timestamp is unpredictable, so it is not checked; reverted_at is the
// same, and only a reverted record may carry one at all.
func txRecordMatches(rec txRecordView, serverTx *commonpb.Transaction) bool {
	tsOK := rec.Timestamp() == nil || rec.Timestamp().GetData() == serverTx.GetTimestamp().GetData()

	var raOK bool
	switch {
	case rec.RevertedAt() != nil:
		raOK = serverTx.GetRevertedAt() != nil && rec.RevertedAt().GetData() == serverTx.GetRevertedAt().GetData()
	case rec.Reverted():
		raOK = true
	default:
		raOK = serverTx.GetRevertedAt() == nil
	}

	return rec.Id() == serverTx.GetId() &&
		rec.Reference() == serverTx.GetReference() &&
		rec.Reverted() == serverTx.GetReverted() &&
		rec.RevertedBy() == serverTx.GetRevertedByTransaction() &&
		rec.RevertsTransaction() == serverTx.GetRevertsTransaction() &&
		tsOK && raOK &&
		postingsEqual(rec.Postings(), serverTx.GetPostings()) &&
		metaMapEqual(rec.Metadata(), serverTx.GetMetadata())
}

// --- Filter generation --------------------------------------------------

// maxQueryGenDepth bounds generated boolean nesting. It must stay well under
// domain.MaxFilterDepth (100), the depth the compiler rejects.
const maxQueryGenDepth = 3

// oneIn reports a 1-in-n chance through the Antithesis chooser, so the platform
// can steer toward the (rare) branch it gates. n must be in [1, 256].
func oneIn(n int) bool {
	choices := make([]uint8, n)
	for i := range choices {
		choices[i] = uint8(i)
	}

	return random.RandomChoice(choices) == 0
}

// genAccountFilter rolls a query filter for ListAccounts. One-in-six is an
// index-backed metadata condition (the NotFound path); ~1-in-16 is a
// transactions-only condition invalid on this target (the InvalidArgument path);
// then one-in-four is the no-filter universe (a nil top-level filter); the rest
// are index-free address filters and boolean compositions.
func genAccountFilter() *commonpb.QueryFilter {
	switch {
	case random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) == 0:
		return filterMetaExists(metaKey())
	case oneIn(16):
		// Target-invalid probe: a transactions-only condition, rejected on accounts.
		if random.RandomChoice([]uint8{0, 1}) == 0 {
			return filterReverted(true)
		}

		return filterTxIDRange(0, 255)
	case random.RandomChoice([]uint8{0, 1, 2, 3}) == 0:
		return nil // top-level universe (no filter)
	default:
		return genAccountFilterFree(0)
	}
}

// genAccountFilterFree rolls a non-nil index-free accounts filter: an address
// prefix/exact leaf, or a boolean composition of the same. It never returns nil
// — universe is expressible only as the absent top-level filter (a nil child in
// a repeated And/Or field marshals as an empty condition the compiler rejects).
func genAccountFilterFree(depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || random.RandomChoice([]uint8{0, 1}) == 0 {
		if random.RandomChoice([]uint8{0, 1}) == 0 {
			return filterAddrPrefix(poolName() + ":")
		}

		return filterAddrExact(poolAddress())
	}

	return genBoolean(depth, genAccountFilterFree)
}

// genTransactionFilter rolls a query filter for ListTransactions. One-in-six is
// an index-backed metadata condition (the NotFound path); ~1-in-16 is an
// accounts-only condition invalid on this target (the InvalidArgument path);
// then one-in-four is the no-filter universe; the rest are index-free reverted /
// tx-id filters and boolean compositions.
func genTransactionFilter() *commonpb.QueryFilter {
	switch {
	case random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) == 0:
		return filterMetaExists(metaKey())
	case oneIn(16):
		// Target-invalid probe: an accounts-only condition, rejected on transactions.
		return filterHasAsset("USD", 2)
	case random.RandomChoice([]uint8{0, 1, 2, 3}) == 0:
		return nil // top-level universe (no filter)
	default:
		return genTransactionFilterFree(0)
	}
}

// genTransactionFilterFree rolls a non-nil index-free transactions filter: a
// reverted or tx-id-range leaf, or a boolean composition of the same. Like the
// accounts variant it never returns nil.
func genTransactionFilterFree(depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || random.RandomChoice([]uint8{0, 1}) == 0 {
		switch random.RandomChoice([]uint8{0, 1, 2}) {
		case 0:
			return filterReverted(true)
		case 1:
			return filterReverted(false)
		default:
			lo := internal.Rand().Uint64() % 256
			return filterTxIDRange(lo, lo+internal.Rand().Uint64()%256)
		}
	}

	return genBoolean(depth, genTransactionFilterFree)
}

// genBoolean wraps two (And/Or) or one (Not) recursively-generated children in a
// boolean combinator. gen never returns nil, so no combinator carries a nil
// child (which would marshal as an empty condition the compiler rejects).
func genBoolean(depth int, gen func(int) *commonpb.QueryFilter) *commonpb.QueryFilter {
	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		return filterAnd(gen(depth+1), gen(depth+1))
	case 1:
		return filterOr(gen(depth+1), gen(depth+1))
	default:
		return filterNot(gen(depth + 1))
	}
}

// --- Filter constructors ------------------------------------------------

func filterAddrPrefix(prefix string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
	}}}
}

func filterAddrExact(addr string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: addr},
	}}}
}

func filterReverted(value bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{
		Reverted: &commonpb.RevertedCondition{Value: value},
	}}
}

// filterTxIDRange matches transactions with lo <= id <= hi. Both bounds are
// inclusive (no exclusive flags), mirroring resolveUintBounds' [min, max+1).
func filterTxIDRange(lo, hi uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
		Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
		Cond:  &commonpb.UintCondition{Min: &lo, Max: &hi},
	}}}
}

func filterAnd(children ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: children}}}
}

func filterOr(children ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: children}}}
}

func filterNot(child *commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: child}}}
}

// filterMetaExists matches entities that carry metadata key — an existence
// condition. It is index-backed on both targets, so in this index-free workload
// it is the NotFound-path probe rather than a result-returning filter.
func filterMetaExists(key string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
	}}}
}

// filterHasAsset matches accounts holding (assetBase, precision). Valid only on
// the ACCOUNTS target, so on transactions it is the target-invalid probe
// (rejected with InvalidArgument before any index check). The values are
// immaterial — rejection happens on target validity, before evaluation.
func filterHasAsset(assetBase string, precision uint32) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{
		AccountHasAsset: &commonpb.AccountHasAssetCondition{AssetBase: assetBase, Precision: precision},
	}}
}

// --- Filter evaluation --------------------------------------------------

// filterNeedsIndex reports whether f contains any condition the server's
// compiler serves from a created index (query.Compile → requireIndexReady).
// This workload builds none, so any such filter makes the list RPC fail with
// NotFound instead of returning rows — the model predicts that outcome rather
// than a result set. The index-free conditions are universe (nil), address on
// accounts, reverted, and the tx-id builtin; every other leaf is index-backed.
//
// This is the seam for creating indexes on the fly later: it would then become
// "needs an index that is not yet READY for the current replica".
func filterNeedsIndex(f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	if f == nil {
		return false
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			if filterNeedsIndex(child, target) {
				return true
			}
		}

		return false
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			if filterNeedsIndex(child, target) {
				return true
			}
		}

		return false
	case *commonpb.QueryFilter_Not:
		return filterNeedsIndex(x.Not.GetFilter(), target)
	case *commonpb.QueryFilter_Reverted:
		return false
	case *commonpb.QueryFilter_Address:
		// Address matching is index-free only on accounts; on transactions it
		// needs the account→tx index.
		return target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	case *commonpb.QueryFilter_BuiltinUint:
		// Only the id builtin scans the always-present Pebble tx keyspace; the
		// timestamp/inserted_at/reverted_at builtins need an index.
		return x.BuiltinUint.GetField() != commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID
	default:
		// Field, Reference, AccountHasAsset, log conditions — all index-backed.
		return true
	}
}

// filterInvalidForTarget reports whether f carries any condition the server's
// per-target validity table rejects (rejectInvalidCondition → InvalidArgument) —
// e.g. reverted / tx-id on accounts, or account-has-asset on transactions. It
// reuses the exact commonpb functions the compiler consults, so the model's
// verdict cannot drift from the server's. Combinators are always valid; recurse
// into their children (mirroring the compiler's per-node check). The server
// checks validity before index availability, so callers must consult this
// before filterNeedsIndex.
func filterInvalidForTarget(f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	if f == nil {
		return false
	}

	if !commonpb.ConditionValidForTarget(target, commonpb.ConditionKindOf(f)) {
		return true
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			if filterInvalidForTarget(child, target) {
				return true
			}
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			if filterInvalidForTarget(child, target) {
				return true
			}
		}
	case *commonpb.QueryFilter_Not:
		return filterInvalidForTarget(x.Not.GetFilter(), target)
	}

	return false
}

// matchAccountFilter evaluates an index-free accounts filter against one
// address. Empty And/Or match nothing, mirroring the compiler's empty-iterator
// treatment; a nil node is the universe (always matches).
func matchAccountFilter(f *commonpb.QueryFilter, addr string) bool {
	if f == nil {
		return true
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Address:
		switch m := x.Address.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedPrefix:
			return strings.HasPrefix(addr, m.HardcodedPrefix)
		case *commonpb.AddressMatch_HardcodedExact:
			return addr == m.HardcodedExact
		}

		return false
	case *commonpb.QueryFilter_And:
		return matchAll(x.And.GetFilters(), func(child *commonpb.QueryFilter) bool { return matchAccountFilter(child, addr) })
	case *commonpb.QueryFilter_Or:
		return matchAny(x.Or.GetFilters(), func(child *commonpb.QueryFilter) bool { return matchAccountFilter(child, addr) })
	case *commonpb.QueryFilter_Not:
		return !matchAccountFilter(x.Not.GetFilter(), addr)
	default:
		return false
	}
}

// matchTxFilter evaluates an index-free transactions filter against one record.
func matchTxFilter(f *commonpb.QueryFilter, rec txRecordView) bool {
	if f == nil {
		return true
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Reverted:
		return rec.Reverted() == x.Reverted.GetValue()
	case *commonpb.QueryFilter_BuiltinUint:
		return matchTxIDBounds(x.BuiltinUint.GetCond(), rec.Id())
	case *commonpb.QueryFilter_And:
		return matchAll(x.And.GetFilters(), func(child *commonpb.QueryFilter) bool { return matchTxFilter(child, rec) })
	case *commonpb.QueryFilter_Or:
		return matchAny(x.Or.GetFilters(), func(child *commonpb.QueryFilter) bool { return matchTxFilter(child, rec) })
	case *commonpb.QueryFilter_Not:
		return !matchTxFilter(x.Not.GetFilter(), rec)
	default:
		return false
	}
}

// matchTxIDBounds mirrors resolveUintBounds: min/max honor their exclusive
// flags, and an absent bound is open on that side.
func matchTxIDBounds(cond *commonpb.UintCondition, id uint64) bool {
	if cond.Min != nil {
		if cond.GetMinExclusive() {
			if id <= cond.GetMin() {
				return false
			}
		} else if id < cond.GetMin() {
			return false
		}
	}

	if cond.Max != nil {
		if cond.GetMaxExclusive() {
			if id >= cond.GetMax() {
				return false
			}
		} else if id > cond.GetMax() {
			return false
		}
	}

	return true
}

// matchAll reports whether every child matches; an empty set matches nothing,
// mirroring the compiler's empty-And iterator.
func matchAll(children []*commonpb.QueryFilter, match func(*commonpb.QueryFilter) bool) bool {
	if len(children) == 0 {
		return false
	}

	for _, child := range children {
		if !match(child) {
			return false
		}
	}

	return true
}

// matchAny reports whether some child matches; an empty set matches nothing.
func matchAny(children []*commonpb.QueryFilter, match func(*commonpb.QueryFilter) bool) bool {
	for _, child := range children {
		if match(child) {
			return true
		}
	}

	return false
}

// --- helpers ------------------------------------------------------------

func reverseStrings(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func reverseUint64(s []uint64) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func joinUint64(s []uint64) string {
	parts := make([]string, len(s))
	for i, v := range s {
		parts[i] = strconv.FormatUint(v, 10)
	}

	return strings.Join(parts, ",")
}

// describeFilter renders a filter as a compact prefix expression for assertion
// details and debug logs.
func describeFilter(f *commonpb.QueryFilter) string {
	if f == nil {
		return "*"
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Address:
		switch m := x.Address.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedPrefix:
			return "addr^" + m.HardcodedPrefix
		case *commonpb.AddressMatch_HardcodedExact:
			return "addr=" + m.HardcodedExact
		}

		return "addr?"
	case *commonpb.QueryFilter_Reverted:
		return "reverted=" + strconv.FormatBool(x.Reverted.GetValue())
	case *commonpb.QueryFilter_BuiltinUint:
		return "id[" + strconv.FormatUint(x.BuiltinUint.GetCond().GetMin(), 10) + "," + strconv.FormatUint(x.BuiltinUint.GetCond().GetMax(), 10) + "]"
	case *commonpb.QueryFilter_And:
		return "and(" + describeChildren(x.And.GetFilters()) + ")"
	case *commonpb.QueryFilter_Or:
		return "or(" + describeChildren(x.Or.GetFilters()) + ")"
	case *commonpb.QueryFilter_Not:
		return "not(" + describeFilter(x.Not.GetFilter()) + ")"
	case *commonpb.QueryFilter_Field:
		return "field:" + x.Field.GetField().GetMetadata()
	case *commonpb.QueryFilter_AccountHasAsset:
		return "hasAsset:" + x.AccountHasAsset.GetAssetBase()
	default:
		return "?"
	}
}

func describeChildren(children []*commonpb.QueryFilter) string {
	parts := make([]string, 0, len(children))
	for _, child := range children {
		parts = append(parts, describeFilter(child))
	}

	return strings.Join(parts, ",")
}
