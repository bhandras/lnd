package invoices

import (
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/channeldb"
)

// Tests that invoices are canceled after expiration.
func TestInvoiceExpiry(t *testing.T) {
	t.Parallel()

	registry, cleanup := newTestContext(t)
	defer cleanup()

	expiryWatcher := NewInvoiceExpiryWatcher(testNetParams, registry)
	expiryWatcher.Start()

	// Subscribe to the not yet existing invoice.
	subscription, err := registry.SubscribeSingleInvoice(testInvoicePaymentHash)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if subscription.hash != testInvoicePaymentHash {
		t.Fatalf("expected subscription for provided hash")
	}

	testExpiry := time.Hour

	// Create the invoice that has already been expired.
	timestamp := time.Date(2017, time.July, 21, 12, 0, 0, 0, time.UTC)
	expiringInvoice := newTestInvoice(t, timestamp, testExpiry)

	// Add the invoice and expect that AddInvoice is successful.
	addIdx, err := registry.AddInvoice(expiringInvoice, testInvoicePaymentHash)
	if err != nil {
		t.Fatal(err)
	}

	if addIdx != 1 {
		t.Fatalf("expected addIndex to start with 1, but got %v",
			addIdx)
	}

	// First we expect the open state to be sent to the single
	// invoice subscriber.
	select {
	case update := <-subscription.Updates:
		if update.State != channeldb.ContractOpen {
			t.Fatalf("expected state ContractOpen, but got %v",
				update.State)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// We expect a state update containing ContractCanceled to be sent to the
	// single invoice subscriber.
	select {
	case update := <-subscription.Updates:
		if update.State != channeldb.ContractCanceled {
			t.Fatalf(
				"expected state ContractCanceled, but got %v",
				update.State,
			)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}
}
