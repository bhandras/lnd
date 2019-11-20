package invoices

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/queue"
	"github.com/lightningnetwork/lnd/zpay32"
)

// invoiceExpiry holds and invoice's payment hash and its expiry. This
// is used to order invoices by their expiry for cancellation.
type invoiceExpiry struct {
	PaymentHash lntypes.Hash
	Expiry      time.Time
}

// Less implements PriorityQueueItem.Less such that the top item in the
// priorty queue will be the one with that expires next.
func (e invoiceExpiry) Less(other queue.PriorityQueueItem) bool {
	return e.Expiry.Before(other.(invoiceExpiry).Expiry)
}

// ExpiryWatcherClock is used to provide a time functions for ExpiryWatcher,
// which can be stubbed out in tests.
type ExpiryWatcherClock interface {
	Now() time.Time
}

// defaultClock implements ExpiryWatcherClock interface by simply calling
// the appropriate time functions.
type defaultClock struct{}

func (defaultClock) Now() time.Time { return time.Now() }

// InvoiceExpiryWatcher handles automatic invoice cancellation of expried
// invoices.
// Upon start InvoiceExpiryWatcher will subscribe to all invoice events and
// will add all active (not yet settled and not yet cancelled) invoices to its
// watcing queue.
// When a new invoice is added to the InvoiceRegistry, InvoiceExpiryWatcher
// will add that to the watching queue as well.
// If any of the watched invoices expire, they'll be removed from the watching
// queue and will be cancelled through InvoiceRegistry.CancelInvoice.
type InvoiceExpiryWatcher struct {
	started sync.Once
	stopped sync.Once
	wg      sync.WaitGroup
	quit    chan interface{}

	// clock is the clock implementation that InvoiceExpiryWatcher uses.
	// It is useful for testing.
	clock ExpiryWatcherClock

	// netParams are used for decoding payment hashes from payment requests. This
	// is required for now as invoice events received through the InvoiceRegistry
	// subscription may not hold a valid payment hash.
	netParams *chaincfg.Params

	// registry is used to subscribe to invoice events at Start() and to cancel
	// invoices upon expiration.
	registry *InvoiceRegistry

	// expiryQueue holds invoiceExpiry items and is used to find the next
	// invoice to expire.
	expiryQueue queue.PriorityQueue

	// subscription holds the invoice subscription created at Start().
	subscription *InvoiceSubscription
}

// NewInvoiceExpiryWatcher creates a new InvoiceExpiryWatcher instance.
func NewInvoiceExpiryWatcher(netParams *chaincfg.Params,
	registry *InvoiceRegistry) *InvoiceExpiryWatcher {

	return &InvoiceExpiryWatcher{
		netParams: netParams,
		registry:  registry,
	}
}

// Start starts the the subscription handler and the main loop. Start() can
// only be called once.
func (ew *InvoiceExpiryWatcher) Start() {
	ew.started.Do(func() {
		// Subscribe to all invoice notificatons and start a goroutine to
		// handle the subscription and forward all new invoices to the expiry
		// queue.
		ew.subscription = ew.registry.SubscribeNotifications(0, 0)
		ew.wg.Add(1)
		go ew.mainLoop()
	})
}

// Stop cancels the invoice subscription and stops the expiry handler loop.
func (ew *InvoiceExpiryWatcher) Stop() {
	ew.stopped.Do(func() {
		// Unsubscribe from all invoice notifications.
		ew.subscription.Cancel()

		// Signal subscriptionHandler to quit and wait for it to return.
		close(ew.quit)
		ew.wg.Wait()
	})
}

// nextExpiry returns the duraton until we must wait for the next invoice to
// expire. If there are no active invoices, then it simply returns max duration.
func (ew *InvoiceExpiryWatcher) nextExpiry() time.Duration {
	if !ew.expiryQueue.Empty() {
		top := ew.expiryQueue.Top().(*invoiceExpiry)
		return top.Expiry.Sub(ew.clock.Now())
	}

	return time.Duration(math.MaxInt64)
}

// cancelExpiredInvoices will cancel all expired invoices and removes them from
// the expiry queue.
func (ew *InvoiceExpiryWatcher) cancelExpiredInvoices() {
	for {
		if ew.expiryQueue.Empty() {
			break
		}

		top := ew.expiryQueue.Top().(*invoiceExpiry)
		if time.Since(top.Expiry) > 0 {
			log.Infof("Cancelling expired invoice '%v', expiry: %v",
				top.PaymentHash, top.Expiry)

			if err := ew.registry.CancelInvoice(top.PaymentHash); err != nil {
				log.Errorf("Unable to cancel invoice: %v", top.PaymentHash)
			}
			ew.expiryQueue.Pop()
		}
	}
}

// mainLoop receives invoice events and handles cancellaton. Only new
// invoices that are not already settled or canceled will be added to the queue.
func (ew *InvoiceExpiryWatcher) mainLoop() {
	defer ew.wg.Done()

	for {
		// Cancel any invoices that may have expired.
		ew.cancelExpiredInvoices()

		select {
		case <-time.After(ew.nextExpiry()):
			// Wait until the next invoice expires, then cancel expired invoices.
			continue

		case invoice := <-ew.subscription.NewInvoices:
			// Only care about invoices that are not canceled or settled already.
			if invoice.State != channeldb.ContractCanceled &&
				invoice.State != channeldb.ContractSettled {
				// Try to add the invoice to the expiry watcher queue.
				if err := ew.addInvoice(invoice); err != nil {
					log.Errorf("Error adding invoice to the expiry watcher queue: %v", err)
				}
			}

		case <-ew.subscription.SettledInvoices:
			// Do nothing with settled invoices. The expiry watcher will try
			// to cancel (which will be ignored).

		case <-ew.quit:
			return
		}
	}
}

// decodePaymentHash is a helper function to decode the payment hash from the
// payment request of the passed Invoice. Returns a zero hash if decoding fails.
// TODO: Remove this function, once InvoiceSubscription forwards payment hashes
// for all invoices or when channeldb.Invoice holds the payment hash.
func (ew *InvoiceExpiryWatcher) decodePaymentHash(
	invoice *channeldb.Invoice) (lntypes.Hash, error) {

	decodedInvoice, err := zpay32.Decode(
		string(invoice.PaymentRequest), ew.netParams)

	if err != nil {
		return lntypes.ZeroHash, err
	}

	return *decodedInvoice.PaymentHash, nil
}

// addInvoice acquires the payment hash for the invoice and then passes it to
// the expiry watcher queue.
func (ew *InvoiceExpiryWatcher) addInvoice(invoice *channeldb.Invoice) error {
	// The payment hash is decoded from the payment request that is stored
	// in the invoice.
	paymentHash, err := ew.decodePaymentHash(invoice)
	if err != nil {
		return fmt.Errorf("Could not decode payment hash for invoice: %v", invoice)
	}

	expiry := invoice.CreationDate.Add(invoice.Terms.Expiry)
	log.Debugf("Adding invoice '%v' to expiry watcher, expiration: %v",
		paymentHash, expiry)

	ew.expiryQueue.Push(&invoiceExpiry{
		PaymentHash: paymentHash,
		Expiry:      expiry,
	})

	return nil
}
