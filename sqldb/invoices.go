package sqldb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/lightningnetwork/lnd/channeldb/models"
	"github.com/lightningnetwork/lnd/clock"
	invpkg "github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/record"
	"github.com/lightningnetwork/lnd/sqldb/sqlc"
)

// InvoiceQueries is an interface that defines the set of operations that can be
// executed against the invoice database.
type InvoiceQueries interface { //nolint:interfacebloat
	InsertInvoice(ctx context.Context, arg sqlc.InsertInvoiceParams) (int64,
		error)

	InsertInvoiceFeature(ctx context.Context,
		arg sqlc.InsertInvoiceFeatureParams) error

	InsertInvoiceHTLC(ctx context.Context,
		arg sqlc.InsertInvoiceHTLCParams) (int64, error)

	InsertInvoiceHTLCCustomRecord(ctx context.Context,
		arg sqlc.InsertInvoiceHTLCCustomRecordParams) error

	FilterInvoices(ctx context.Context,
		arg sqlc.FilterInvoicesParams) ([]sqlc.Invoice, error)

	GetInvoice(ctx context.Context,
		arg sqlc.GetInvoiceParams) ([]sqlc.Invoice, error)

	GetInvoiceFeatures(ctx context.Context,
		invoiceID int64) ([]sqlc.InvoiceFeature, error)

	GetInvoiceHTLCCustomRecords(ctx context.Context,
		invoiceID int64) ([]sqlc.GetInvoiceHTLCCustomRecordsRow, error)

	GetInvoiceHTLCs(ctx context.Context,
		invoiceID int64) ([]sqlc.InvoiceHtlc, error)

	UpdateInvoiceState(ctx context.Context,
		arg sqlc.UpdateInvoiceStateParams) (sql.Result, error)

	UpdateInvoiceAmountPaid(ctx context.Context,
		arg sqlc.UpdateInvoiceAmountPaidParams) (sql.Result, error)

	NextInvoiceSettledIndex(ctx context.Context) (int64, error)

	UpdateInvoiceHTLC(ctx context.Context,
		arg sqlc.UpdateInvoiceHTLCParams) error

	UpdateInvoiceHTLCs(ctx context.Context,
		arg sqlc.UpdateInvoiceHTLCsParams) error

	DeleteInvoice(ctx context.Context, arg sqlc.DeleteInvoiceParams) (
		sql.Result, error)

	DeleteCanceledInvoices(ctx context.Context) (sql.Result, error)

	DeleteInvoiceFeatures(ctx context.Context, invoiceID int64) error

	DeleteInvoiceHTLC(ctx context.Context, htlcID int64) error

	DeleteInvoiceHTLCCustomRecords(ctx context.Context,
		invoiceID int64) error

	DeleteInvoiceHTLCs(ctx context.Context, invoiceID int64) error

	// AMP specific methods.
	UpsertAmpInvoice(ctx context.Context,
		arg sqlc.UpsertAmpInvoiceParams) (sql.Result, error)

	GetAmpInvoiceID(ctx context.Context, setID []byte) (int64, error)

	UpdateAmpInvoiceState(ctx context.Context,
		arg sqlc.UpdateAmpInvoiceStateParams) error

	InsertAmpInvoiceHtlc(ctx context.Context,
		arg sqlc.InsertAmpInvoiceHtlcParams) error

	FetchAMPInvoices(ctx context.Context,
		arg sqlc.FetchAMPInvoicesParams) ([]sqlc.AmpInvoice, error)

	FetchAMPInvoiceHTLCs(ctx context.Context,
		arg sqlc.FetchAMPInvoiceHTLCsParams) (
		[]sqlc.FetchAMPInvoiceHTLCsRow, error)

	FetchSettledAmpInvoices(ctx context.Context,
		arg sqlc.FetchSettledAmpInvoicesParams) (
		[]sqlc.FetchSettledAmpInvoicesRow, error)

	UpdateAmpInvoiceHtlcPreimage(ctx context.Context,
		arg sqlc.UpdateAmpInvoiceHtlcPreimageParams) (sql.Result, error)

	// Event specific methods.
	InsertInvoiceEvent(ctx context.Context,
		arg sqlc.InsertInvoiceEventParams) error

	SelectInvoiceEvents(ctx context.Context,
		arg sqlc.SelectInvoiceEventsParams) ([]sqlc.InvoiceEvent, error)

	DeleteInvoiceEvents(ctx context.Context, invoiceID int64) error
}

var _ invpkg.InvoiceDB = (*InvoiceStore)(nil)

// InvoiceQueriesTxOptions defines the set of db txn options the InvoiceQueries
// understands.
type InvoiceQueriesTxOptions struct {
	// readOnly governs if a read only transaction is needed or not.
	readOnly bool
}

// ReadOnly returns true if the transaction should be read only.
//
// NOTE: This implements the TxOptions.
func (a *InvoiceQueriesTxOptions) ReadOnly() bool {
	return a.readOnly
}

// NewInvoiceQueryReadTx creates a new read transaction option set.
func NewInvoiceQueryReadTx() InvoiceQueriesTxOptions {
	return InvoiceQueriesTxOptions{
		readOnly: true,
	}
}

// BatchedInvoiceQueries is a version of the InvoiceQueries that's capable of
// batched database operations.
type BatchedInvoiceQueries interface {
	InvoiceQueries

	BatchedTx[InvoiceQueries]
}

// InvoiceStore represents a storage backend.
type InvoiceStore struct {
	db    BatchedInvoiceQueries
	clock clock.Clock
}

// InvoiceEventType is the type of an invoice event.
type InvoiceEventType uint16

const (
	// InvoiceCreated is the event type emitted when an invoice is created.
	InvoiceCreated InvoiceEventType = iota

	// InvoiceCanceled is the event type emitted when an invoice is
	// canceled.
	InvoiceCanceled

	// InvoiceSettled is the event type emitted when an invoice is settled.
	InvoiceSettled

	// InvoiceSetIDCreated is the event type emitted when the first htlc of
	// a set id is accepted.
	InvoiceSetIDCreated

	// InvoiceSetIDCanceled is the event type emitted when the set id is
	// canceled.
	InvoiceSetIDCanceled

	// InvoiceSetIDSettled is the event type emitted when the set id is
	// settled.
	InvoiceSetIDSettled
)

// String returns a human-readable description of an event type.
func (i InvoiceEventType) String() string {
	switch i {
	case InvoiceCreated:
		return "invoice created"

	case InvoiceCanceled:
		return "invoice canceled"

	case InvoiceSettled:
		return "invoice settled"

	case InvoiceSetIDCreated:
		return "invoice set id created"

	case InvoiceSetIDCanceled:
		return "invoice set id canceled"

	case InvoiceSetIDSettled:
		return "invoice set id settled"

	default:
		return "unknown invoice event type"
	}
}

// NewInvoiceStore creates a new InvoiceStore instance given a open
// BatchedInvoiceQueries storage backend.
func NewInvoiceStore(db BatchedInvoiceQueries,
	clock clock.Clock) *InvoiceStore {

	return &InvoiceStore{
		db:    db,
		clock: clock,
	}
}

// AddInvoice inserts the targeted invoice into the database. If the invoice has
// *any* payment hashes which already exists within the database, then the
// insertion will be aborted and rejected due to the strict policy banning any
// duplicate payment hashes.
//
// NOTE: A side effect of this function is that it sets AddIndex on newInvoice.
func (i *InvoiceStore) AddInvoice(ctx context.Context,
	newInvoice *invpkg.Invoice, paymentHash lntypes.Hash) (uint64, error) {

	// Make sure this is a valid invoice before trying to store it in our
	// DB.
	if err := invpkg.ValidateInvoice(newInvoice, paymentHash); err != nil {
		return 0, err
	}

	var (
		writeTxOpts InvoiceQueriesTxOptions
		invoiceID   int64
	)

	err := i.db.ExecTx(ctx, &writeTxOpts, func(db InvoiceQueries) error {
		params := sqlc.InsertInvoiceParams{
			Hash:       paymentHash[:],
			Memo:       sqlStr(string(newInvoice.Memo)),
			AmountMsat: int64(newInvoice.Terms.Value),
			// Note: BOLT12 invoices don't have a final cltv delta.
			CltvDelta: sqlInt32(newInvoice.Terms.FinalCltvDelta),
			Expiry:    int32(newInvoice.Terms.Expiry),
			// Note: keysend invoices don't have a payment request.
			PaymentRequest: sqlStr(string(
				newInvoice.PaymentRequest),
			),
			State:          int16(newInvoice.State),
			AmountPaidMsat: int64(newInvoice.AmtPaid),
			IsAmp:          newInvoice.IsAMP(),
			IsHodl:         newInvoice.HodlInvoice,
			IsKeysend:      newInvoice.IsKeysend(),
			CreatedAt:      newInvoice.CreationDate,
		}

		// Some invoices may not have a preimage, like in the case of
		// HODL invoices.
		if newInvoice.Terms.PaymentPreimage != nil {
			preimage := *newInvoice.Terms.PaymentPreimage
			if preimage == invpkg.UnknownPreimage {
				return errors.New("cannot use all-zeroes " +
					"preimage")
			}
			params.Preimage = preimage[:]
		}

		// Some non MPP payments may have the defaulf (invalid) value.
		if newInvoice.Terms.PaymentAddr != invpkg.BlankPayAddr {
			params.PaymentAddr = newInvoice.Terms.PaymentAddr[:]
		}

		var err error
		invoiceID, err = db.InsertInvoice(ctx, params)
		if err != nil {
			return fmt.Errorf("unable to insert invoice: %w", err)
		}

		// TODO(positiveblue): if invocies do not have custom features
		// maybe just store the "invoice type" and populate the features
		// based on that.
		for feature := range newInvoice.Terms.Features.Features() {
			params := sqlc.InsertInvoiceFeatureParams{
				InvoiceID: invoiceID,
				Feature:   int32(feature),
			}

			err := db.InsertInvoiceFeature(ctx, params)
			if err != nil {
				return fmt.Errorf("unable to insert invoice "+
					"feature(%v): %w", feature, err)
			}
		}

		eventParams := sqlc.InsertInvoiceEventParams{
			CreatedAt: i.clock.Now(),
			InvoiceID: invoiceID,
			EventType: int32(InvoiceCreated),
		}

		err = db.InsertInvoiceEvent(ctx, eventParams)
		if err != nil {
			return fmt.Errorf("unable to insert invoice event: %w",
				err)
		}

		return nil
	})
	if err != nil {
		// Add context to unique constraint errors.
		var uniqueConstraintErr *ErrSQLUniqueConstraintViolation
		if errors.As(err, &uniqueConstraintErr) {
			return 0, invpkg.ErrDuplicateInvoice
		}

		return 0, fmt.Errorf("unable to add invoice(%v): %w",
			paymentHash, err)
	}

	newInvoice.AddIndex = uint64(invoiceID)
	return newInvoice.AddIndex, nil
}

func (i *InvoiceStore) fetchInvoice(ctx context.Context,
	db InvoiceQueries, ref invpkg.InvoiceRef) (*invpkg.Invoice, error) {

	if ref.PayHash() == nil && ref.PayAddr() == nil && ref.SetID() == nil {
		return nil, invpkg.ErrInvoiceNotFound
	}

	var (
		invoice *invpkg.Invoice
		params  sqlc.GetInvoiceParams
	)

	if ref.PayHash() != nil {
		params.Hash = ref.PayHash()[:]
	}

	if ref.PayAddr() != nil && *ref.PayAddr() != invpkg.BlankPayAddr {
		params.PaymentAddr = ref.PayAddr()[:]
	}

	if ref.SetID() != nil && ref.Modifier() != invpkg.HtlcSetOnlyModifier {
		params.SetID = ref.SetID()[:]
	}

	rows, err := db.GetInvoice(ctx, params)
	switch {
	case len(rows) == 0:
		return nil, invpkg.ErrInvoiceNotFound

	case len(rows) > 1:
		return nil, fmt.Errorf("ambiguous invoice ref: %s",
			ref.String())

	case err != nil:
		return nil, fmt.Errorf("unable to fetch invoice: %w", err)
	}

	var (
		setID         *[32]byte
		fetchAmpHtlcs bool
	)

	switch ref.Modifier() {
	case invpkg.DefaultModifier:
		// By default we'll fetch all AMP HTLCs.
		setID = nil
		fetchAmpHtlcs = true

	case invpkg.HtlcSetOnlyModifier:
		// In this case we'll fetch all AMP HTLCs for the
		// specified set id.
		if ref.SetID() == nil {
			return nil, fmt.Errorf("set id is required for htlc " +
				"set only modifier")
		}

		setID = ref.SetID()
		fetchAmpHtlcs = true

	case invpkg.HtlcSetBlankModifier:
		// No need to fetch any htlc data.
		setID = nil
		fetchAmpHtlcs = false

	default:
		return nil, fmt.Errorf("unknown invoice ref modifier: %v",
			ref.Modifier())
	}

	// Load the rest of the invoice data.
	_, invoice, err = fetchInvoiceData(
		ctx, db, rows[0], setID, fetchAmpHtlcs,
	)
	if err != nil {
		return nil, err
	}

	return invoice, nil

}

func fetchAmpState(ctx context.Context, db InvoiceQueries, invoiceID int64,
	setID *[32]byte, fetchHtlcs bool) (invpkg.AMPInvoiceState,
	invpkg.HTLCSet, error) {

	var paramSetID []byte
	if setID != nil {
		paramSetID = setID[:]
	}

	ampInvoiceRows, err := db.FetchAMPInvoices(
		ctx, sqlc.FetchAMPInvoicesParams{
			InvoiceID: invoiceID,
			SetID:     paramSetID,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	ampState := make(map[invpkg.SetID]invpkg.InvoiceStateAMP)
	for _, row := range ampInvoiceRows {
		var rowSetID [32]byte

		if len(row.SetID) != 32 {
			return nil, nil, fmt.Errorf("invalid set id length: %d",
				len(row.SetID))
		}

		copy(rowSetID[:], row.SetID)
		ampState[rowSetID] = invpkg.InvoiceStateAMP{
			State:       invpkg.HtlcState(row.State),
			SettleIndex: uint64(row.SettledIndex.Int64),
			SettleDate:  row.SettledAt.Time,
			InvoiceKeys: make(map[models.CircuitKey]struct{}),
		}
	}

	if !fetchHtlcs {
		return ampState, nil, nil
	}

	customRecordRows, err := db.GetInvoiceHTLCCustomRecords(ctx, invoiceID)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get custom records for "+
			"invoice HTLCs: %w", err)
	}

	customRecords := make(map[int64]record.CustomSet, len(customRecordRows))
	for _, row := range customRecordRows {
		if _, ok := customRecords[row.HtlcID]; !ok {
			customRecords[row.HtlcID] = make(record.CustomSet)
		}

		value := row.Value
		if value == nil {
			value = []byte{}
		}

		customRecords[row.HtlcID][uint64(row.Key)] = value
	}

	ampHtlcRows, err := db.FetchAMPInvoiceHTLCs(
		ctx, sqlc.FetchAMPInvoiceHTLCsParams{
			InvoiceID: invoiceID,
			SetID:     paramSetID,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	ampHtlcs := make(map[models.CircuitKey]*invpkg.InvoiceHTLC)
	for _, row := range ampHtlcRows {
		uint64ChanID, err := strconv.ParseUint(row.ChanID, 10, 64)
		if err != nil {
			return nil, nil, err
		}

		chanID := lnwire.NewShortChanIDFromInt(uint64ChanID)

		if row.HtlcID < 0 {
			return nil, nil, fmt.Errorf("invalid HTLC ID "+
				"value: %v", row.HtlcID)
		}

		htlcID := uint64(row.HtlcID)

		circuitKey := invpkg.CircuitKey{
			ChanID: chanID,
			HtlcID: htlcID,
		}

		htlc := &invpkg.InvoiceHTLC{
			Amt:          lnwire.MilliSatoshi(row.AmountMsat),
			AcceptHeight: uint32(row.AcceptHeight),
			AcceptTime:   row.AcceptTime,
			Expiry:       uint32(row.ExpiryHeight),
			State:        invpkg.HtlcState(row.State),
		}

		if row.TotalMppMsat.Valid {
			htlc.MppTotalAmt = lnwire.MilliSatoshi(
				row.TotalMppMsat.Int64,
			)
		}

		if row.ResolveTime.Valid {
			htlc.ResolveTime = row.ResolveTime.Time
		}

		var (
			rootShare [32]byte
			setID     [32]byte
		)

		if len(row.RootShare) != 32 {
			return nil, nil, fmt.Errorf("invalid root share "+
				"length: %d", len(row.RootShare))
		}
		copy(rootShare[:], row.RootShare)

		if len(row.SetID) != 32 {
			return nil, nil, fmt.Errorf("invalid set ID length: %d",
				len(row.SetID))
		}
		copy(setID[:], row.SetID)

		if row.ChildIndex < 0 || row.ChildIndex > math.MaxUint32 {
			return nil, nil, fmt.Errorf("invalid child index "+
				"value: %v", row.ChildIndex)
		}

		ampRecord := record.NewAMP(
			rootShare, setID, uint32(row.ChildIndex),
		)

		htlc.AMP = &invpkg.InvoiceHtlcAMPData{
			Record: *ampRecord,
		}

		if len(row.Hash) != 32 {
			return nil, nil, fmt.Errorf("invalid hash length: %d",
				len(row.Hash))
		}
		copy(htlc.AMP.Hash[:], row.Hash)

		if row.Preimage != nil {
			preimage, err := lntypes.MakePreimage(row.Preimage)
			if err != nil {
				return nil, nil, err
			}

			htlc.AMP.Preimage = &preimage
		}

		if _, ok := customRecords[row.ID]; ok {
			htlc.CustomRecords = customRecords[row.ID]
		} else {
			htlc.CustomRecords = make(record.CustomSet)
		}

		ampHtlcs[circuitKey] = htlc

	}

	return ampState, ampHtlcs, nil
}

// LookupInvoice attempts to look up an invoice according to its 32 byte
// payment hash. If an invoice which can settle the HTLC identified by the
// passed payment hash isn't found, then an error is returned.
// Otherwise, the full invoice is returned.
// Before setting the incoming HTLC, the values SHOULD be checked to ensure the
// payer meets the agreed upon contractual terms of the payment.
func (i *InvoiceStore) LookupInvoice(ctx context.Context,
	ref invpkg.InvoiceRef) (invpkg.Invoice, error) {

	var (
		invoice *invpkg.Invoice
		err     error
	)

	readTxOpt := InvoiceQueriesTxOptions{readOnly: true}
	txErr := i.db.ExecTx(ctx, &readTxOpt, func(db InvoiceQueries) error {
		invoice, err = i.fetchInvoice(ctx, db, ref)

		return err
	})
	if txErr != nil {
		return invpkg.Invoice{}, txErr
	}

	return *invoice, nil
}

func (i *InvoiceStore) FetchPendingInvoices(ctx context.Context) (
	map[lntypes.Hash]invpkg.Invoice, error) {

	invoices := make(map[lntypes.Hash]invpkg.Invoice)

	readTxOpt := InvoiceQueriesTxOptions{readOnly: true}
	err := i.db.ExecTx(ctx, &readTxOpt, func(db InvoiceQueries) error {
		allRows := make([]sqlc.Invoice, 0, 100)

		limit := int32(100)
		offset := int32(0)

		for {
			params := sqlc.FilterInvoicesParams{
				PendingOnly: true,
				NumOffset:   offset,
				NumLimit:    limit,
			}

			rows, err := db.FilterInvoices(ctx, params)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("unable to get invoices "+
					"from db: %w", err)
			}

			allRows = append(allRows, rows...)

			if len(rows) < int(limit) {
				break
			}

			offset += limit
		}

		if len(allRows) == 0 {
			return nil
		}

		// Load all the information for the invoices.
		for _, row := range allRows {
			// Load the common invoice data.
			hash, invoice, err := fetchInvoiceData(
				ctx, db, row, nil, true,
			)
			if err != nil {
				return err
			}

			invoices[*hash] = *invoice
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("unable to fetch pending invoices: %w",
			err)
	}

	return invoices, nil
}

// InvoicesSettledSince can be used by callers to catch up any settled invoices
// they missed within the settled invoice time series. We'll return all known
// settled invoice that have a settle index higher than the passed
// sinceSettleIndex.
//
// NOTE: The index starts from 1, as a result. We enforce that
// specifying a value below the starting index value is a noop.
func (i *InvoiceStore) InvoicesSettledSince(ctx context.Context,
	id uint64) ([]invpkg.Invoice, error) {

	var invoices []invpkg.Invoice

	if id == 0 {
		return invoices, nil
	}

	readTxOpt := InvoiceQueriesTxOptions{readOnly: true}
	err := i.db.ExecTx(ctx, &readTxOpt, func(db InvoiceQueries) error {
		allRows := make([]sqlc.Invoice, 0, 100)

		settleIdx := id
		limit := int32(100)
		offset := int32(0)

		for {
			params := sqlc.FilterInvoicesParams{
				// SettleIndexGreaterOrEqualThan.
				SettledIndexGet: sqlInt64(settleIdx + 1),
				NumLimit:        limit,
				NumOffset:       offset,
			}

			rows, err := db.FilterInvoices(ctx, params)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("unable to get invoices "+
					"from db: %w", err)
			}

			allRows = append(allRows, rows...)

			if len(rows) < int(limit) {
				break
			}

			settleIdx += uint64(limit)
			offset += limit
		}

		// Load all the information for the invoices.
		for _, row := range allRows {
			_, invoice, err := fetchInvoiceData(
				ctx, db, row, nil, true,
			)
			if err != nil {
				return fmt.Errorf("unable to fetch "+
					"invoice(id=%d) from db: %w",
					row.ID, err)
			}

			invoices = append(invoices, *invoice)
		}

		ampInvoices, err := i.db.FetchSettledAmpInvoices(
			ctx, sqlc.FetchSettledAmpInvoicesParams{
				SettledIndexGet: sqlInt64(id + 1),
			},
		)
		if err != nil {
			return err
		}

		for _, ampInvoice := range ampInvoices {
			sqlInvoice := sqlc.Invoice{
				ID:             ampInvoice.ID,
				Hash:           ampInvoice.Hash,
				Preimage:       ampInvoice.Preimage,
				SettledIndex:   ampInvoice.AmpSettledIndex,
				SettledAt:      ampInvoice.AmpSettledAt,
				Memo:           ampInvoice.Memo,
				AmountMsat:     ampInvoice.AmountMsat,
				CltvDelta:      ampInvoice.CltvDelta,
				Expiry:         ampInvoice.Expiry,
				PaymentAddr:    ampInvoice.PaymentAddr,
				PaymentRequest: ampInvoice.PaymentRequest,
				State:          ampInvoice.State,
				AmountPaidMsat: ampInvoice.AmountPaidMsat,
				IsAmp:          ampInvoice.IsAmp,
				IsHodl:         ampInvoice.IsHodl,
				IsKeysend:      ampInvoice.IsKeysend,
				CreatedAt:      ampInvoice.CreatedAt,
			}

			_, invoice, err := fetchInvoiceData(
				ctx, db, sqlInvoice,
				(*[32]byte)(ampInvoice.SetID), true,
			)
			if err != nil {
				return fmt.Errorf("unable to fetch "+
					"AMP invoice(id=%d) from db: %w",
					ampInvoice.ID, err)
			}

			invoices = append(invoices, *invoice)

		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get invoices settled since "+
			"index %d: %w", id, err)
	}

	return invoices, nil

}

// InvoicesAddedSince can be used by callers to seek into the event time series
// of all the invoices added in the database. The specified sinceAddIndex should
// be the highest add index that the caller knows of. This method will return
// all invoices with an add index greater than the specified sinceAddIndex.
//
// NOTE: The index starts from 1, as a result. We enforce that specifying a
// value below the starting index value is a noop.
func (i *InvoiceStore) InvoicesAddedSince(ctx context.Context,
	id uint64) ([]invpkg.Invoice, error) {

	var newInvoices []invpkg.Invoice

	if id == 0 {
		return newInvoices, nil
	}

	readTxOpt := InvoiceQueriesTxOptions{readOnly: true}
	err := i.db.ExecTx(ctx, &readTxOpt, func(db InvoiceQueries) error {
		allRows := make([]sqlc.Invoice, 0, 100)

		addIdx := id
		limit := int32(100)
		offset := int32(0)

		for {
			params := sqlc.FilterInvoicesParams{
				// AddIndexGreaterOrEqualThan.
				AddIndexGet: sqlInt64(addIdx + 1),
				NumLimit:    limit,
				NumOffset:   offset,
			}

			rows, err := db.FilterInvoices(ctx, params)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("unable to get invoices "+
					"from db: %w", err)
			}

			allRows = append(allRows, rows...)

			if len(rows) < int(limit) {
				break
			}

			addIdx += uint64(limit)
			offset += limit
		}

		if len(allRows) == 0 {
			return nil
		}

		// Load all the information for the invoices.
		for _, row := range allRows {
			// Load the common invoice data.
			_, invoice, err := fetchInvoiceData(
				ctx, db, row, nil, true,
			)
			if err != nil {
				return err
			}

			newInvoices = append(newInvoices, *invoice)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("unable to get invoices added since "+
			"index %d: %w", id, err)
	}

	return newInvoices, nil
}

// QueryInvoices allows a caller to query the invoice database for invoices
// within the specified add index range.
func (i *InvoiceStore) QueryInvoices(ctx context.Context,
	q invpkg.InvoiceQuery) (invpkg.InvoiceSlice, error) {

	var invoices []invpkg.Invoice

	if q.NumMaxInvoices == 0 {
		return invpkg.InvoiceSlice{}, fmt.Errorf("max invoices must " +
			"be non-zero")
	}

	readTxOpt := InvoiceQueriesTxOptions{readOnly: true}
	err := i.db.ExecTx(ctx, &readTxOpt, func(db InvoiceQueries) error {
		offset := int32(0)
		limit := int32(100)

		for {
			params := sqlc.FilterInvoicesParams{
				NumOffset:   offset,
				NumLimit:    limit,
				PendingOnly: q.PendingOnly,
			}

			if !q.Reversed {
				// The invoice with index offset id must not be
				// included in the results.
				params.AddIndexGet = sqlInt64(
					q.IndexOffset + uint64(offset) + 1,
				)
			}

			if q.Reversed {
				idx := int32(q.IndexOffset)

				// If the index offset was not set, we want to
				// fetch from the lastest invoice.
				if idx == 0 {
					params.AddIndexLet = sqlInt64(
						math.MaxInt64,
					)
				} else {
					// The invoice with index offset id must
					// not be included in the results.
					params.AddIndexLet = sqlInt64(
						idx - offset - 1,
					)
				}

				params.Reverse = true
			}

			if !q.CreationDateStart.IsZero() {
				params.CreatedAfter = sql.NullTime{
					Time:  q.CreationDateStart,
					Valid: true,
				}
			}

			if !q.CreationDateEnd.IsZero() {
				params.CreatedBefore = sql.NullTime{
					Time:  q.CreationDateEnd,
					Valid: true,
				}
			}

			rows, err := db.FilterInvoices(ctx, params)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("unable to get invoices "+
					"from db: %w", err)
			}

			if len(rows) == 0 {
				return nil
			}

			// Load all the information for the invoices.
			for _, row := range rows {
				// Load the common invoice data.
				_, invoice, err := fetchInvoiceData(
					ctx, db, row, nil, true,
				)
				if err != nil {
					return err
				}

				invoices = append(invoices, *invoice)

				if len(invoices) == int(q.NumMaxInvoices) {
					return nil
				}

			}

			offset += limit
		}
	})

	if err != nil {
		return invpkg.InvoiceSlice{}, fmt.Errorf("unable to query "+
			"invoices: %w", err)
	}

	if len(invoices) == 0 {
		return invpkg.InvoiceSlice{
			InvoiceQuery: q,
		}, nil
	}

	// If we iterated through the add index in reverse order, then
	// we'll need to reverse the slice of invoices to return them in
	// forward order.
	if q.Reversed {
		numInvoices := len(invoices)
		for i := 0; i < numInvoices/2; i++ {
			reverse := numInvoices - i - 1
			invoices[i], invoices[reverse] =
				invoices[reverse], invoices[i]
		}
	}

	res := invpkg.InvoiceSlice{
		InvoiceQuery:     q,
		Invoices:         invoices,
		FirstIndexOffset: invoices[0].AddIndex,
		LastIndexOffset:  invoices[len(invoices)-1].AddIndex,
	}

	return res, nil
}

type sqlInvoiceUpdater struct {
	db         InvoiceQueries
	ctx        context.Context
	invoice    *invpkg.Invoice
	updateTime time.Time
}

func (s *sqlInvoiceUpdater) AddHtlc(circuitKey models.CircuitKey,
	newHtlc *invpkg.InvoiceHTLC) error {

	htlcPrimaryKeyID, err := s.db.InsertInvoiceHTLC(
		s.ctx, sqlc.InsertInvoiceHTLCParams{
			HtlcID: int64(circuitKey.HtlcID),
			ChanID: strconv.FormatUint(
				circuitKey.ChanID.ToUint64(), 10,
			),
			AmountMsat: int64(newHtlc.Amt),
			TotalMppMsat: sql.NullInt64{
				Int64: int64(newHtlc.MppTotalAmt),
				Valid: newHtlc.MppTotalAmt != 0,
			},
			AcceptHeight: int32(newHtlc.AcceptHeight),
			AcceptTime:   newHtlc.AcceptTime,
			ExpiryHeight: int32(newHtlc.Expiry),
			State:        int16(newHtlc.State),
			InvoiceID:    int64(s.invoice.AddIndex),
		},
	)
	if err != nil {
		return err
	}

	for key, value := range newHtlc.CustomRecords {
		err = s.db.InsertInvoiceHTLCCustomRecord(
			s.ctx, sqlc.InsertInvoiceHTLCCustomRecordParams{
				// TODO(bhandras): schema might be wrong here
				// as the custom record key is an uint64.
				Key:    int64(key),
				Value:  value,
				HtlcID: htlcPrimaryKeyID,
			},
		)
	}

	if newHtlc.AMP != nil {
		setID := newHtlc.AMP.Record.SetID()

		_, err := s.db.UpsertAmpInvoice(
			s.ctx, sqlc.UpsertAmpInvoiceParams{
				SetID: setID[:],
				// TODO(bhandras): do we need need to set state?
				CreatedAt: s.updateTime,
				InvoiceID: int64(s.invoice.AddIndex),
			},
		)
		if err != nil {
			mappedSqlErr := MapSQLError(err)
			var uniqueConstraintErr *ErrSQLUniqueConstraintViolation
			if errors.As(mappedSqlErr, &uniqueConstraintErr) {
				return invpkg.ErrDuplicateSetID{
					SetID: setID,
				}
			}

			return err
		}

		rootShare := newHtlc.AMP.Record.RootShare()

		ampHtlcParams := sqlc.InsertAmpInvoiceHtlcParams{
			InvoiceID: int64(s.invoice.AddIndex),
			SetID:     setID[:],
			HtlcID:    htlcPrimaryKeyID,
			RootShare: rootShare[:],
			ChildIndex: int64(
				newHtlc.AMP.Record.ChildIndex(),
			),
			Hash: newHtlc.AMP.Hash[:],
		}

		if newHtlc.AMP.Preimage != nil {
			ampHtlcParams.Preimage = newHtlc.AMP.Preimage[:]
		}

		err = s.db.InsertAmpInvoiceHtlc(s.ctx, ampHtlcParams)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *sqlInvoiceUpdater) ResolveHtlc(circuitKey models.CircuitKey,
	state invpkg.HtlcState, resolveTime time.Time) error {

	return s.db.UpdateInvoiceHTLC(s.ctx, sqlc.UpdateInvoiceHTLCParams{
		HtlcID: int64(circuitKey.HtlcID),
		ChanID: strconv.FormatUint(
			circuitKey.ChanID.ToUint64(), 10,
		),
		InvoiceID: int64(s.invoice.AddIndex),
		State:     int16(state),
		ResolveTime: sql.NullTime{
			Time:  resolveTime,
			Valid: true,
		},
	})
}

func (s *sqlInvoiceUpdater) AddAmpHtlcPreimage(setID [32]byte,
	circuitKey models.CircuitKey, preimage lntypes.Preimage) error {

	result, err := s.db.UpdateAmpInvoiceHtlcPreimage(
		s.ctx, sqlc.UpdateAmpInvoiceHtlcPreimageParams{
			InvoiceID: int64(s.invoice.AddIndex),
			SetID:     setID[:],
			HtlcID:    int64(circuitKey.HtlcID),
			Preimage:  preimage[:],
		},
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return invpkg.ErrInvoiceNotFound
	}

	return nil
}

func (s *sqlInvoiceUpdater) UpdateInvoiceState(
	newState invpkg.ContractState, preimage *lntypes.Preimage) error {

	var (
		settledIndex sql.NullInt64
		settledAt    sql.NullTime
	)

	if newState == invpkg.ContractSettled {
		nextSettledIndex, err := s.db.NextInvoiceSettledIndex(s.ctx)
		if err != nil {
			return err
		}

		settledIndex.Int64 = int64(nextSettledIndex)
		settledIndex.Valid = true

		// If the invoice is settled, we'll also update the settle time.
		settledAt = sql.NullTime{
			Time:  s.updateTime,
			Valid: true,
		}
	}

	params := sqlc.UpdateInvoiceStateParams{
		ID:           int64(s.invoice.AddIndex),
		State:        int16(newState),
		SettledIndex: settledIndex,
		SettledAt:    settledAt,
	}

	if preimage != nil {
		params.Preimage = preimage[:]
	}

	result, err := s.db.UpdateInvoiceState(s.ctx, params)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return invpkg.ErrInvoiceNotFound
	}

	if settledIndex.Valid {
		s.invoice.SettleIndex = uint64(settledIndex.Int64)
		s.invoice.SettleDate = settledAt.Time
	}

	return nil
}

func (s *sqlInvoiceUpdater) UpdateInvoiceAmtPaid(
	amtPaid lnwire.MilliSatoshi) error {

	_, err := s.db.UpdateInvoiceAmountPaid(
		s.ctx, sqlc.UpdateInvoiceAmountPaidParams{
			ID:             int64(s.invoice.AddIndex),
			AmountPaidMsat: int64(amtPaid),
		},
	)

	return err
}

func (s *sqlInvoiceUpdater) UpdateAmpState(setID [32]byte,
	newState invpkg.InvoiceStateAMP) error {

	var (
		settledIndex sql.NullInt64
		settledAt    sql.NullTime
	)

	if newState.State == invpkg.HtlcStateSettled {
		nextSettledIndex, err := s.db.NextInvoiceSettledIndex(s.ctx)
		if err != nil {
			return err
		}

		settledIndex.Int64 = int64(nextSettledIndex)
		settledIndex.Valid = true

		// If the invoice is settled, we'll also update the settle time.
		settledAt = sql.NullTime{
			Time:  s.updateTime,
			Valid: true,
		}
	}

	err := s.db.UpdateAmpInvoiceState(
		s.ctx, sqlc.UpdateAmpInvoiceStateParams{
			SetID:        setID[:],
			State:        int16(newState.State),
			SettledIndex: settledIndex,
			SettledAt:    settledAt,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *sqlInvoiceUpdater) AcceptHtlcAmp(setID [32]byte,
	circuitKey models.CircuitKey) error {

	return nil
}

func (s *sqlInvoiceUpdater) SettleHtlcAmp(setID [32]byte,
	circuitKey models.CircuitKey) error {

	return nil
}

func (s *sqlInvoiceUpdater) CancelHtlcAmp(setID [32]byte,
	circuitKey models.CircuitKey) error {

	return nil
}

func (s *sqlInvoiceUpdater) Commit(updateType invpkg.UpdateType) error {
	return nil
}

func (i *InvoiceStore) UpdateInvoice(ctx context.Context, ref invpkg.InvoiceRef,
	setIDHint *invpkg.SetID, callback invpkg.InvoiceUpdateCallback) (
	*invpkg.Invoice, error) {

	var updatedInvoice *invpkg.Invoice

	txOpt := InvoiceQueriesTxOptions{readOnly: false}
	txErr := i.db.ExecTx(ctx, &txOpt, func(db InvoiceQueries) error {
		invoice, err := i.fetchInvoice(ctx, db, ref)
		if err != nil {
			return err
		}

		updateTime := i.clock.Now()
		updater := &sqlInvoiceUpdater{
			db:         db,
			ctx:        ctx,
			invoice:    invoice,
			updateTime: updateTime,
		}

		payHash := ref.PayHash()
		updatedInvoice, err = invpkg.UpdateInvoice(
			payHash, invoice, updateTime, callback, updater,
		)

		return err
	})
	if txErr != nil {
		return updatedInvoice, txErr
	}

	return updatedInvoice, nil
}

// DeleteInvoice attempts to delete the passed invoices and all their related
// data from the database in one transaction.
//
// NOTE: Settled invoices cannot be deleted with this method.
func (i *InvoiceStore) DeleteInvoice(ctx context.Context,
	invoicesToDelete []invpkg.InvoiceDeleteRef) error {

	// All the InvoiceDeleteRef instances include the add index of the
	// invoice. The rest was added to ensure that the invoices were deleted
	// properly in the kv database. When we have fully migrated we can
	// remove the rest of the fields.
	for _, ref := range invoicesToDelete {
		if ref.AddIndex == 0 {
			return fmt.Errorf("unable to delete invoice using a "+
				"ref without AddIndex set: %v", ref)
		}
	}

	var writeTxOpt InvoiceQueriesTxOptions
	err := i.db.ExecTx(ctx, &writeTxOpt, func(db InvoiceQueries) error {
		for _, ref := range invoicesToDelete {
			params := sqlc.DeleteInvoiceParams{
				AddIndex: sqlInt64(ref.AddIndex),
			}

			if ref.SettleIndex != 0 {
				params.SettledIndex = sqlInt64(ref.SettleIndex)
			}

			if ref.PayHash != lntypes.ZeroHash {
				params.Hash = ref.PayHash[:]
			}

			result, err := db.DeleteInvoice(ctx, params)
			if err != nil {
				return fmt.Errorf("unable to delete "+
					"invoice(%v): %w", ref.AddIndex, err)
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("unable to get rows "+
					"affected: %w", err)
			}
			if rowsAffected == 0 {
				return fmt.Errorf("%w: %v",
					invpkg.ErrInvoiceNotFound, ref.AddIndex)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("unable to delete invoices: %w", err)
	}

	return nil
}

func (i *InvoiceStore) DeleteCanceledInvoices(ctx context.Context) error {
	var writeTxOpt InvoiceQueriesTxOptions
	err := i.db.ExecTx(ctx, &writeTxOpt, func(db InvoiceQueries) error {
		_, err := db.DeleteCanceledInvoices(ctx)
		if err != nil {
			return fmt.Errorf("unable to delete canceled "+
				"invoices: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to delete invoices: %w", err)
	}

	return nil
}

// fetchInvoiceData fetches the common invoice data for the given params.
func fetchInvoiceData(ctx context.Context, db InvoiceQueries,
	row sqlc.Invoice, setID *[32]byte, fetchAmpHtlcs bool) (*lntypes.Hash,
	*invpkg.Invoice, error) {

	// Unmarshal the common data.
	hash, invoice, err := unmarshalInvoice(row)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to unmarshal "+
			"invoice(id=%d) from db: %w", row.ID, err)
	}

	// Fetch the invoice features.
	features, err := getInvoiceFeatures(ctx, db, row.ID)
	if err != nil {
		return nil, nil, err
	}

	invoice.Terms.Features = features

	// If this is an AMP invoice, we'll need fetch the AMP state along
	// with the HTLCs (if requested).
	if invoice.IsAMP() {
		invoiceID := int64(invoice.AddIndex)
		ampState, ampHtlcs, err := fetchAmpState(
			ctx, db, invoiceID, setID, fetchAmpHtlcs,
		)
		if err != nil {
			return nil, nil, err
		}

		if len(ampHtlcs) > 0 {
			for setID := range ampState {
				var amtPaid lnwire.MilliSatoshi
				invoiceKeys := make(
					map[models.CircuitKey]struct{},
				)

				for key, htlc := range ampHtlcs {
					if htlc.AMP.Record.SetID() != setID {
						continue
					}

					invoiceKeys[key] = struct{}{}

					if htlc.State != invpkg.HtlcStateCanceled { // nolint: lll
						amtPaid += htlc.Amt
					}
				}

				setState := ampState[setID]
				setState.InvoiceKeys = invoiceKeys
				setState.AmtPaid = amtPaid
				ampState[setID] = setState
			}
		}

		invoice.AMPState = ampState
		invoice.Htlcs = ampHtlcs

		return hash, invoice, nil
	}

	// Otherwise simply fetch the invoice HTLCs.
	htlcs, err := getInvoiceHtlcs(ctx, db, row.ID)
	if err != nil {
		return nil, nil, err
	}

	if len(htlcs) > 0 {
		invoice.Htlcs = htlcs
		var amountPaid lnwire.MilliSatoshi
		for _, htlc := range htlcs {
			if htlc.State == invpkg.HtlcStateSettled {
				amountPaid += htlc.Amt
			}
		}
		invoice.AmtPaid = amountPaid
	}

	return hash, invoice, nil
}

// getInvoiceFeatures fetches the invoice features for the given invoice id.
func getInvoiceFeatures(ctx context.Context, db InvoiceQueries,
	invoiceID int64) (*lnwire.FeatureVector, error) {

	rows, err := db.GetInvoiceFeatures(ctx, invoiceID)
	if err != nil {
		return nil, fmt.Errorf("unable to get invoice features: %w",
			err)
	}

	features := lnwire.EmptyFeatureVector()
	for _, feature := range rows {
		features.Set(lnwire.FeatureBit(feature.Feature))
	}

	return features, nil
}

// getInvoiceHtlcs fetches the invoice htlcs for the given invoice id.
func getInvoiceHtlcs(ctx context.Context, db InvoiceQueries,
	invoiceID int64) (map[invpkg.CircuitKey]*invpkg.InvoiceHTLC, error) {

	htlcRows, err := db.GetInvoiceHTLCs(ctx, invoiceID)
	if err != nil {
		return nil, fmt.Errorf("unable to get invoice htlcs: %w", err)
	}

	// We have no htlcs to unmarshal.
	if len(htlcRows) == 0 {
		return nil, nil
	}

	crRows, err := db.GetInvoiceHTLCCustomRecords(ctx, invoiceID)
	if err != nil {
		return nil, fmt.Errorf("unable to get custom records for "+
			"invoice htlcs: %w", err)
	}

	cr := make(map[int64]record.CustomSet, len(crRows))
	for _, row := range crRows {
		if _, ok := cr[row.HtlcID]; !ok {
			cr[row.HtlcID] = make(record.CustomSet)
		}

		value := row.Value
		if value == nil {
			value = []byte{}
		}
		cr[row.HtlcID][uint64(row.Key)] = value
	}

	htlcs := make(map[invpkg.CircuitKey]*invpkg.InvoiceHTLC, len(htlcRows))

	for _, row := range htlcRows {
		circuiteKey, htlc, err := unmarshalInvoiceHTLC(row)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal "+
				"htlc(%d): %w", row.ID, err)
		}

		if customRecords, ok := cr[row.ID]; ok {
			htlc.CustomRecords = customRecords
		} else {
			htlc.CustomRecords = make(record.CustomSet)
		}

		htlcs[circuiteKey] = htlc
	}

	return htlcs, nil
}

// unmarshalInvoice converts an InvoiceRow to an Invoice.
func unmarshalInvoice(row sqlc.Invoice) (*lntypes.Hash, *invpkg.Invoice,
	error) {

	var (
		settledIndex   int64
		settledAt      time.Time
		memo           []byte
		paymentRequest []byte
		preimage       *lntypes.Preimage
		paymentAddr    [32]byte
	)

	hash, err := lntypes.MakeHash(row.Hash)
	if err != nil {
		return nil, nil, err
	}

	if row.SettledIndex.Valid {
		settledIndex = row.SettledIndex.Int64
	}

	if row.SettledAt.Valid {
		settledAt = row.SettledAt.Time
	}

	if row.Memo.Valid {
		memo = []byte(row.Memo.String)
	}

	// Keysend payments will have this field empty.
	if row.PaymentRequest.Valid {
		paymentRequest = []byte(row.PaymentRequest.String)
	} else {
		paymentRequest = []byte{}
	}

	// We may not have the preimage if this a hodl invoice.
	if row.Preimage != nil {
		preimage = &lntypes.Preimage{}
		copy(preimage[:], row.Preimage)
	}

	copy(paymentAddr[:], row.PaymentAddr)

	var cltvDelta int32
	if row.CltvDelta.Valid {
		cltvDelta = row.CltvDelta.Int32
	}

	invoice := &invpkg.Invoice{
		SettleIndex:    uint64(settledIndex),
		SettleDate:     settledAt,
		Memo:           memo,
		PaymentRequest: paymentRequest,
		CreationDate:   row.CreatedAt,
		Terms: invpkg.ContractTerm{
			FinalCltvDelta:  cltvDelta,
			Expiry:          time.Duration(row.Expiry),
			PaymentPreimage: preimage,
			Value:           lnwire.MilliSatoshi(row.AmountMsat),
			PaymentAddr:     paymentAddr,
		},
		AddIndex:    uint64(row.ID),
		State:       invpkg.ContractState(row.State),
		AmtPaid:     lnwire.MilliSatoshi(row.AmountPaidMsat),
		Htlcs:       make(map[models.CircuitKey]*invpkg.InvoiceHTLC),
		AMPState:    invpkg.AMPInvoiceState{},
		HodlInvoice: row.IsHodl,
	}

	return &hash, invoice, nil
}

// unmarshalInvoiceHTLC converts an sqlc.InvoiceHtlc to an InvoiceHTLC.
func unmarshalInvoiceHTLC(row sqlc.InvoiceHtlc) (invpkg.CircuitKey,
	*invpkg.InvoiceHTLC, error) {

	uint64ChanID, err := strconv.ParseUint(row.ChanID, 10, 64)
	if err != nil {
		return invpkg.CircuitKey{}, nil, err
	}

	chanID := lnwire.NewShortChanIDFromInt(uint64ChanID)

	if row.HtlcID < 0 {
		return invpkg.CircuitKey{}, nil, fmt.Errorf("invalid uint64 "+
			"value: %v", row.HtlcID)
	}

	htlcID := uint64(row.HtlcID)

	circuitKey := invpkg.CircuitKey{
		ChanID: chanID,
		HtlcID: htlcID,
	}

	htlc := &invpkg.InvoiceHTLC{
		Amt:          lnwire.MilliSatoshi(row.AmountMsat),
		AcceptHeight: uint32(row.AcceptHeight),
		AcceptTime:   row.AcceptTime,
		Expiry:       uint32(row.ExpiryHeight),
		State:        invpkg.HtlcState(row.State),
	}

	if row.TotalMppMsat.Valid {
		htlc.MppTotalAmt = lnwire.MilliSatoshi(row.TotalMppMsat.Int64)
	}

	if row.ResolveTime.Valid {
		htlc.ResolveTime = row.ResolveTime.Time
	}

	return circuitKey, htlc, nil
}
