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
func (t *txRecord) RevertedBy() uint64                           { return t.revertedBy }
func (t *txRecord) RevertedAt() *commonpb.Timestamp              { return t.revertedAt }
func (t *txRecord) RevertsTransaction() uint64                   { return t.revertsTransaction }

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
