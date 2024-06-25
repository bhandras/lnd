package sqldb

import (
	"context"
	"testing"

	"github.com/lightningnetwork/lnd/sqldb/sqlc"
	"github.com/stretchr/testify/require"
)

// TestInvoiceExpiryMigration tests that the migration from version 3 to 4
// correctly sets the expiry value of all invoices to 86400 seconds.
func TestInvoiceExpiryMigration(t *testing.T) {
	ctxb := context.Background()

	// Create a new database that already has the first version of the
	// native invoice schema.
	db := NewTestDBWithVersion(t, 3)

	hash := []byte{1, 2, 3}
	_, err := db.InsertInvoice(ctxb, sqlc.InsertInvoiceParams{
		Hash:   hash,
		Expiry: -123,
	})
	require.NoError(t, err)

	// Now, we'll attempt to execute the migration that will fix the expiry
	// values by inserting 86400 seconds for all invoices.
	err = db.ExecuteMigrations(TargetVersion(4))

	invoices, err := db.GetInvoice(ctxb, sqlc.GetInvoiceParams{
		Hash: hash,
	})

	require.NoError(t, err)
	require.Len(t, invoices, 1)
	require.Equal(t, int32(86400), invoices[0].Expiry)
}
