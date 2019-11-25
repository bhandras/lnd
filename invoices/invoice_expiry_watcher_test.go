package invoices

import (
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lntypes"
)

// Tests that invoices are canceled after expiration.
func TestInvoiceExpiry(t *testing.T) {
	t.Parallel()

	watcher := NewInvoiceExpiryWatcher(&testClock{})
	defer watcher.Stop()
	cancelCalled := make(chan interface{})

	watcher.Start(func(hash lntypes.Hash) error {
		if hash != testInvoicePaymentHash {
			t.Fatalf("Expected: %v, got: %v", testInvoicePaymentHash, hash)
		}

		close(cancelCalled)
		return nil
	})

	testExpiry := time.Hour

	// Create the invoice that has already been expired.
	timestamp := time.Date(2017, time.July, 21, 10, 0, 0, 0, time.UTC)
	expiringInvoice := newTestInvoice(t, timestamp, testExpiry)

	watcher.AddInvoice(testInvoicePaymentHash, expiringInvoice)

	// We expect that the invoice is cancelled before timeout.
	select {
	case <-cancelCalled:
		// Invoice cancelled.

	case <-time.After(testTimeout):
		t.Fatalf("Invoice not cancelled in %v time", testTimeout)
	}
}
