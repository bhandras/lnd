-- name: UpsertAmpInvoice :execresult
INSERT INTO amp_invoices (
    set_id, state, created_at, invoice_id
) VALUES (
    $1, $2, $3, $4
) ON CONFLICT (set_id, invoice_id) DO NOTHING;

-- name: GetAmpInvoiceID :one
SELECT invoice_id FROM amp_invoices WHERE set_id = $1;

-- name: UpdateAmpInvoiceState :exec
UPDATE amp_invoices
SET state = $2, 
    settled_index = COALESCE(settled_index, $3),
    settled_at = COALESCE(settled_at, $4)
WHERE set_id = $1; 

-- name: InsertAmpInvoiceHtlc :exec
INSERT INTO amp_invoice_htlcs (
    invoice_id, set_id, htlc_id, root_share, child_index, hash, preimage
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
);

-- name: FetchAMPInvoices :many
SELECT *
FROM amp_invoices
WHERE invoice_id = $1 
AND (
    set_id = sqlc.narg('set_id') OR 
    sqlc.narg('set_id') IS NULL
);

-- name: FetchAMPInvoiceHTLCs :many
SELECT 
    amp.set_id, amp.root_share, amp.child_index, amp.hash, amp.preimage, 
    invoice_htlcs.*
FROM amp_invoice_htlcs amp
INNER JOIN invoice_htlcs ON amp.htlc_id = invoice_htlcs.id
WHERE amp.invoice_id = $1
AND (
    set_id = sqlc.narg('set_id') OR 
    sqlc.narg('set_id') IS NULL
);

-- name: FetchSettledAmpInvoices :many
SELECT 
    a.set_id, 
    a.settled_index as amp_settled_index, 
    a.settled_at as amp_settled_at,
    i.*
FROM amp_invoices a
INNER JOIN invoices i ON a.invoice_id = i.id
WHERE (
    a.settled_index >= sqlc.narg('settled_index_get') OR
    sqlc.narg('settled_index_get') IS NULL
) AND (
    a.settled_index <= sqlc.narg('settled_index_let') OR
    sqlc.narg('settled_index_let') IS NULL
);

-- name: UpdateAmpInvoiceHtlcPreimage :execresult
UPDATE amp_invoice_htlcs AS a
SET preimage = $4
WHERE a.invoice_id = $1 AND a.set_id = $2 AND a.htlc_id = (
    SELECT id FROM invoice_htlcs AS i WHERE i.htlc_id = $3
);
