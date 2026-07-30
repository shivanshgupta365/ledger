package oracle

import (
	"sort"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Exported read accessors over the model's internal state. The driver and the
// replay tool inspect a committed state (chart, volumes, metadata, declared
// field types) to generate operations and compare against the SUT; the maps are
// never mutated through these — every model mutation goes through Apply.

func (g GlobalState) Ledgers() map[string]LedgerState { return g.ledgers }

func (s LedgerState) Types() map[string]TypeState                         { return s.types }
func (s LedgerState) Volumes() map[VolumeKey]VolumePair                   { return s.volumes }
func (s LedgerState) Metadata() map[MetaKey]*commonpb.MetadataValue       { return s.metadata }
func (s LedgerState) LedgerMeta() map[string]*commonpb.MetadataValue      { return s.ledgerMeta }
func (s LedgerState) AccountFieldTypes() map[string]commonpb.MetadataType { return s.accountFieldTypes }
func (s LedgerState) LedgerFieldTypes() map[string]commonpb.MetadataType  { return s.ledgerFieldTypes }
func (s LedgerState) TransactionFieldTypes() map[string]commonpb.MetadataType {
	return s.transactionFieldTypes
}

// Txs is the transaction log; index i holds the transaction with id i+1. TxByRef
// indexes referenced transactions by reference -> id.
func (s LedgerState) Txs() []*txRecord        { return s.txs }
func (s LedgerState) TxByRef() map[string]int { return s.txByRef }

// txRecord accessors expose one committed transaction from the log: its
// server-assigned id, its reference ("" for drains/transients/reverts), its
// postings, its metadata, whether it has been reverted, its user-supplied
// timestamp (nil when the client sent none — see txRecord.timestamp), and its
// revert relationships (see the txRecord fields for the nil/zero conventions).
func (t *txRecord) Id() uint64                                   { return t.id }
func (t *txRecord) Reference() string                            { return t.reference }
func (t *txRecord) Postings() []*commonpb.Posting                { return t.postings }
func (t *txRecord) Metadata() map[string]*commonpb.MetadataValue { return t.metadata }
func (t *txRecord) Reverted() bool                               { return t.reverted }
func (t *txRecord) Timestamp() *commonpb.Timestamp               { return t.timestamp }
func (t *txRecord) InsertedAt() *commonpb.Timestamp              { return t.insertedAt }
func (t *txRecord) RevertedBy() uint64                           { return t.revertedBy }
func (t *txRecord) RevertedAt() *commonpb.Timestamp              { return t.revertedAt }
func (t *txRecord) RevertsTransaction() uint64                   { return t.revertsTransaction }

// IndexedAddrs is the transaction's account→tx index membership, keyed by
// account with AddrIndexedSource/AddrIndexedDestination bits — see
// txRecord.indexedAddrs. Read-only, like the other map accessors.
func (t *txRecord) IndexedAddrs() map[string]uint8 { return t.indexedAddrs }

// HasAccount reports whether the account currently holds a volume cell or a
// metadata entry — membership in the merged V+M attributes universe the
// server's address matching scans (pebbleAccountExists / the account prefix
// iterator). A purged account with no metadata is NOT in the universe even
// when older index rows still reference it.
func (s LedgerState) HasAccount(addr string) bool {
	for k := range s.volumes {
		if k.Address == addr {
			return true
		}
	}
	for k := range s.metadata {
		if k.Address == addr {
			return true
		}
	}

	return false
}

// Indexes returns the ledger's index set keyed by canonical IndexID (value is
// the active flag: true active, false ambiguous). Read-only, like the other map
// accessors — mutate index state through SetIndexActive / SetIndexAmbiguous.
func (s LedgerState) Indexes() map[string]bool { return s.indexes }

// IndexState reports whether the ledger has an index with the given canonical
// IndexID, and if so whether it is active (READY confirmed) or ambiguous
// (created, readiness not yet confirmed — a not-ready error is tolerated).
func (s LedgerState) IndexState(canonical string) (exists, active bool) {
	active, exists = s.indexes[canonical]

	return exists, active
}

// SetIndexActive flips an existing ambiguous index to active on the named
// ledger, for the driver's readiness poller once it has confirmed the index
// READY across replicas. A no-op if the ledger or index is absent (e.g. a
// concurrent DropIndex already removed it). The index map is shared with the
// stored ledger, so this mutates committed state in place; callers hold the
// checker mutex.
func (g GlobalState) SetIndexActive(ledger, canonical string) {
	ls, ok := g.ledgers[ledger]
	if !ok {
		return
	}

	if _, exists := ls.indexes[canonical]; exists {
		ls.indexes[canonical] = true
	}
}

// SetIndexAmbiguous flips an existing active index back to ambiguous on the
// named ledger, for the driver's readiness poller when a replica reports the
// index not-ready again (e.g. a restored node rebuilding its read-store). This
// only ever widens what the model tolerates — an ambiguous index accepts both a
// not-ready error and a validated result window — so it can never manufacture a
// finding. Same no-op / in-place semantics as SetIndexActive.
func (g GlobalState) SetIndexAmbiguous(ledger, canonical string) {
	ls, ok := g.ledgers[ledger]
	if !ok {
		return
	}

	if _, exists := ls.indexes[canonical]; exists {
		ls.indexes[canonical] = false
	}
}

// HasEverAsset reports whether the account has ever touched (base, precision) via
// a committed, non-excluded posting — its membership in the account-by-asset
// index the has-asset filter reads.
func (s LedgerState) HasEverAsset(address, base string, precision uint32) bool {
	_, ok := s.everAsset[assetTouch{address: address, base: base, precision: precision}]

	return ok
}

// EverAssetAccounts returns, sorted ascending by address, every account that has
// ever touched (base, precision) — the exact account set a bare has-asset query
// over (base, precision) returns, in the server's address order.
func (s LedgerState) EverAssetAccounts(base string, precision uint32) []string {
	var out []string
	for k := range s.everAsset {
		if k.base == base && k.precision == precision {
			out = append(out, k.address)
		}
	}

	sort.Strings(out)

	return out
}

func (m *metaEffect) Saved() map[string]*commonpb.MetadataValue { return m.saved }

func (r *revertEffect) RevertedID() uint64            { return r.revertedID }
func (r *revertEffect) Postings() []*commonpb.Posting { return r.postings }

// LearnTxStamps fills the server-stamped dates of transaction id that the model
// could not predict at apply time: a nil timestamp, the always-server-stamped
// insertedAt, and a nil revertedAt on a reverted original. Known (client-
// supplied) values are never overwritten — reads validate those directly. The
// values come from the commit response's logs; they are deterministic FSM
// outputs, so folding them in keeps the model exact and lets later reads and
// filter windows check them for equality instead of skipping.
//
// Mutation safety: the record pointer is replaced, not mutated, but the txs
// backing array IS written in place. The caller must hold the checker's lock
// and call this only on the committed state, in the same critical section that
// advanced it — before any candidate fork of the new state is taken. Forks of
// earlier states own their backing arrays (clone copies the slice), so they are
// unaffected.
func (g GlobalState) LearnTxStamps(ledger string, id uint64, timestamp, insertedAt, revertedAt *commonpb.Timestamp) {
	ls, ok := g.ledgers[ledger]
	if !ok || id == 0 || id > uint64(len(ls.txs)) {
		return
	}

	rec := *ls.txs[id-1]
	if rec.timestamp == nil {
		rec.timestamp = timestamp
	}
	if rec.insertedAt == nil {
		rec.insertedAt = insertedAt
	}
	if rec.revertedAt == nil && rec.reverted {
		rec.revertedAt = revertedAt
	}

	ls.txs[id-1] = &rec
}

// FieldTypesFor returns the declared-type map for a metadata target — the
// schema slice the generator consults for indexable fields. Read-only, like
// the other map accessors.
func (s LedgerState) FieldTypesFor(target commonpb.TargetType) map[string]commonpb.MetadataType {
	return s.fieldTypes(target)
}
