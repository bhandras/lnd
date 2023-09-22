// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.22.0
// source: amp_invoices.sql

package sqlc

import (
	"context"
	"database/sql"
	"time"
)

const deleteAMPHTLCCustomRecords = `-- name: DeleteAMPHTLCCustomRecords :exec
WITH htlc_ids AS (
    SELECT htlc_id
    FROM amp_invoice_htlcs 
    WHERE invoice_id = $1
)
DELETE
FROM invoice_htlc_custom_records
WHERE htlc_id IN (SELECT id FROM htlc_ids)
`

func (q *Queries) DeleteAMPHTLCCustomRecords(ctx context.Context, invoiceID int64) error {
	_, err := q.db.ExecContext(ctx, deleteAMPHTLCCustomRecords, invoiceID)
	return err
}

const deleteAMPHTLCs = `-- name: DeleteAMPHTLCs :exec
DELETE 
FROM amp_invoice_htlcs  
WHERE invoice_id = $1
`

func (q *Queries) DeleteAMPHTLCs(ctx context.Context, invoiceID int64) error {
	_, err := q.db.ExecContext(ctx, deleteAMPHTLCs, invoiceID)
	return err
}

const deleteAMPInvoiceHTLC = `-- name: DeleteAMPInvoiceHTLC :exec
DELETE 
FROM amp_invoice_htlcs
WHERE set_id = $1
`

func (q *Queries) DeleteAMPInvoiceHTLC(ctx context.Context, setID []byte) error {
	_, err := q.db.ExecContext(ctx, deleteAMPInvoiceHTLC, setID)
	return err
}

const getAMPInvoiceHTLCsByInvoiceID = `-- name: GetAMPInvoiceHTLCsByInvoiceID :many
SELECT set_id, htlc_id, invoice_id, root_share, child_index, hash, preimage
FROM amp_invoice_htlcs
WHERE invoice_id = $1
`

func (q *Queries) GetAMPInvoiceHTLCsByInvoiceID(ctx context.Context, invoiceID int64) ([]AmpInvoiceHtlc, error) {
	rows, err := q.db.QueryContext(ctx, getAMPInvoiceHTLCsByInvoiceID, invoiceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AmpInvoiceHtlc
	for rows.Next() {
		var i AmpInvoiceHtlc
		if err := rows.Scan(
			&i.SetID,
			&i.HtlcID,
			&i.InvoiceID,
			&i.RootShare,
			&i.ChildIndex,
			&i.Hash,
			&i.Preimage,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getAMPInvoiceHTLCsBySetID = `-- name: GetAMPInvoiceHTLCsBySetID :many
SELECT set_id, htlc_id, invoice_id, root_share, child_index, hash, preimage
FROM amp_invoice_htlcs
WHERE set_id = $1
`

func (q *Queries) GetAMPInvoiceHTLCsBySetID(ctx context.Context, setID []byte) ([]AmpInvoiceHtlc, error) {
	rows, err := q.db.QueryContext(ctx, getAMPInvoiceHTLCsBySetID, setID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AmpInvoiceHtlc
	for rows.Next() {
		var i AmpInvoiceHtlc
		if err := rows.Scan(
			&i.SetID,
			&i.HtlcID,
			&i.InvoiceID,
			&i.RootShare,
			&i.ChildIndex,
			&i.Hash,
			&i.Preimage,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getSetIDHTLCsCustomRecords = `-- name: GetSetIDHTLCsCustomRecords :many
SELECT ihcr.htlc_id, key, value
FROM amp_invoice_htlcs aih JOIN invoice_htlc_custom_records ihcr ON aih.id=ihcr.htlc_id 
WHERE aih.set_id = $1
`

type GetSetIDHTLCsCustomRecordsRow struct {
	HtlcID int64
	Key    int64
	Value  []byte
}

func (q *Queries) GetSetIDHTLCsCustomRecords(ctx context.Context, setID []byte) ([]GetSetIDHTLCsCustomRecordsRow, error) {
	rows, err := q.db.QueryContext(ctx, getSetIDHTLCsCustomRecords, setID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetSetIDHTLCsCustomRecordsRow
	for rows.Next() {
		var i GetSetIDHTLCsCustomRecordsRow
		if err := rows.Scan(&i.HtlcID, &i.Key, &i.Value); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const insertAMPInvoiceHTLC = `-- name: InsertAMPInvoiceHTLC :exec
INSERT INTO amp_invoice_htlcs (
    set_id, htlc_id, root_share, child_index, hash, preimage
) VALUES (
    $1, $2, $3, $4, $5, $6
)
`

type InsertAMPInvoiceHTLCParams struct {
	SetID      []byte
	HtlcID     int64
	RootShare  []byte
	ChildIndex int64
	Hash       []byte
	Preimage   []byte
}

func (q *Queries) InsertAMPInvoiceHTLC(ctx context.Context, arg InsertAMPInvoiceHTLCParams) error {
	_, err := q.db.ExecContext(ctx, insertAMPInvoiceHTLC,
		arg.SetID,
		arg.HtlcID,
		arg.RootShare,
		arg.ChildIndex,
		arg.Hash,
		arg.Preimage,
	)
	return err
}

const insertAMPInvoicePayment = `-- name: InsertAMPInvoicePayment :exec
INSERT INTO amp_invoice_payments (
    set_id, state, created_at, settled_index, invoice_id
) VALUES (
    $1, $2, $3, $4, $5
)
`

type InsertAMPInvoicePaymentParams struct {
	SetID        []byte
	State        int16
	CreatedAt    time.Time
	SettledIndex sql.NullInt64
	InvoiceID    int64
}

func (q *Queries) InsertAMPInvoicePayment(ctx context.Context, arg InsertAMPInvoicePaymentParams) error {
	_, err := q.db.ExecContext(ctx, insertAMPInvoicePayment,
		arg.SetID,
		arg.State,
		arg.CreatedAt,
		arg.SettledIndex,
		arg.InvoiceID,
	)
	return err
}

const selectAMPInvoicePayments = `-- name: SelectAMPInvoicePayments :many
SELECT aip.set_id, aip.state, aip.created_at, aip.settled_index, aip.invoice_id, ip.id, ip.settled_at, ip.amount_paid_msat, ip.invoice_id
FROM amp_invoice_payments aip LEFT JOIN invoice_payments ip ON aip.settled_index = ip.id
WHERE (
    set_id = $1 OR 
    $1 IS NULL
) AND (
    aip.settled_index = $2 OR 
    $2 IS NULL
) AND (
    aip.invoice_id = $3 OR 
    $3 IS NULL
)
`

type SelectAMPInvoicePaymentsParams struct {
	SetID        []byte
	SettledIndex sql.NullInt64
	InvoiceID    sql.NullInt64
}

type SelectAMPInvoicePaymentsRow struct {
	SetID          []byte
	State          int16
	CreatedAt      time.Time
	SettledIndex   sql.NullInt64
	InvoiceID      int64
	ID             sql.NullInt64
	SettledAt      sql.NullTime
	AmountPaidMsat sql.NullInt64
	InvoiceID_2    sql.NullInt64
}

func (q *Queries) SelectAMPInvoicePayments(ctx context.Context, arg SelectAMPInvoicePaymentsParams) ([]SelectAMPInvoicePaymentsRow, error) {
	rows, err := q.db.QueryContext(ctx, selectAMPInvoicePayments, arg.SetID, arg.SettledIndex, arg.InvoiceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []SelectAMPInvoicePaymentsRow
	for rows.Next() {
		var i SelectAMPInvoicePaymentsRow
		if err := rows.Scan(
			&i.SetID,
			&i.State,
			&i.CreatedAt,
			&i.SettledIndex,
			&i.InvoiceID,
			&i.ID,
			&i.SettledAt,
			&i.AmountPaidMsat,
			&i.InvoiceID_2,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const updateAMPInvoiceHTLC = `-- name: UpdateAMPInvoiceHTLC :exec
UPDATE amp_invoice_htlcs
SET preimage = $1
WHERE htlc_id = $2
`

type UpdateAMPInvoiceHTLCParams struct {
	Preimage []byte
	HtlcID   int64
}

func (q *Queries) UpdateAMPInvoiceHTLC(ctx context.Context, arg UpdateAMPInvoiceHTLCParams) error {
	_, err := q.db.ExecContext(ctx, updateAMPInvoiceHTLC, arg.Preimage, arg.HtlcID)
	return err
}

const updateAMPPayment = `-- name: UpdateAMPPayment :exec
UPDATE amp_invoice_payments
SET state = $1, settled_index = $2
WHERE state = 0 AND (
    set_id = $3 OR 
    $3 IS NULL
) AND (
    invoice_id = $4 OR 
    $4 IS NULL
)
`

type UpdateAMPPaymentParams struct {
	State        int16
	SettledIndex sql.NullInt64
	SetID        []byte
	InvoiceID    sql.NullInt64
}

func (q *Queries) UpdateAMPPayment(ctx context.Context, arg UpdateAMPPaymentParams) error {
	_, err := q.db.ExecContext(ctx, updateAMPPayment,
		arg.State,
		arg.SettledIndex,
		arg.SetID,
		arg.InvoiceID,
	)
	return err
}
