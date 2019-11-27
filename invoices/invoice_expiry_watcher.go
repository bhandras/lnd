package invoices

import (
	"fmt"
	"sync"
	"time"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/clock"
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
	return e.Expiry.Before(other.(*invoiceExpiry).Expiry)
}

// InvoiceExpiryWatcher handles automatic invoice cancellation of expried
// invoices. Upon start InvoiceExpiryWatcher will retrieve all pending (not yet
// settled or canceled) invoices invoices to its watcing queue. When a new
// invoice is added to the InvoiceRegistry, it'll be forarded to the
// InvoiceExpiryWatcher and will end up in the watching queue as well.
// If any of the watched invoices expire, they'll be removed from the watching
// queue and will be cancelled through InvoiceRegistry.CancelInvoice().
type InvoiceExpiryWatcher struct {
	sync.Mutex
	started bool
	wg      sync.WaitGroup

	// clock is the clock implementation that InvoiceExpiryWatcher uses.
	// It is useful for testing.
	clock clock.Clock

	// quit signals InvoiceExpiryWatcher to stop.
	quit chan interface{}

	// cancelInvoice is a template method that cancels an expired invoice.
	cancelInvoice func(lntypes.Hash) error

	// expiryQueue holds invoiceExpiry items and is used to find the next
	// invoice to expire.
	expiryQueue queue.PriorityQueue

	// newInvoices channel is used to wake up the main loop when a new invoices
	// is added.
	newInvoices chan *invoiceExpiry
}

// NewInvoiceExpiryWatcher creates a new InvoiceExpiryWatcher instance.
func NewInvoiceExpiryWatcher(clock clock.Clock) *InvoiceExpiryWatcher {
	return &InvoiceExpiryWatcher{
		clock:       clock,
		quit:        make(chan interface{}),
		newInvoices: make(chan *invoiceExpiry),
	}
}

// PrefetchInvoices fetches all active invoices and their corresponding payment
// hashes from ChannelDB and adds them to the watcher queue. This is useful to
// prepopulate InvoiceExpiryWatcher with past, but active invoices upon start.
func (ew *InvoiceExpiryWatcher) PrefetchInvoices(cdb *channeldb.DB) error {
	pendingOnly := true
	pendingInvoices, err := cdb.FetchAllInvoicesWithPaymentHash(pendingOnly)
	if err != nil && err != channeldb.ErrNoInvoicesCreated {
		log.Errorf(
			"Error while prefetching active invoices from the database: %v", err,
		)
		return err
	}

	for k := 0; k < len(pendingInvoices); k++ {
		ew.AddInvoice(pendingInvoices[k].PaymentHash, &pendingInvoices[k].Invoice)
	}

	return nil
}

// Start starts the the subscription handler and the main loop. Start() will
// return with error if InvoiceExpiryWatcher is already started. Start()
// expects a cancellation function passed that will be use to cancel expired
// invoices by their payment hash.
func (ew *InvoiceExpiryWatcher) Start(
	cancelInvoice func(lntypes.Hash) error) error {
	ew.Lock()
	defer ew.Unlock()

	if ew.started {
		return fmt.Errorf("InvoiceExpiryWatcher already started")
	}

	ew.started = true
	ew.cancelInvoice = cancelInvoice
	ew.wg.Add(1)
	go ew.mainLoop()

	return nil
}

// Stop quits the expiry handler loop and waits for InvoiceExpiryWatcher to
// fully stop.
func (ew *InvoiceExpiryWatcher) Stop() {
	ew.Lock()
	defer ew.Unlock()

	if ew.started {
		// Signal subscriptionHandler to quit and wait for it to return.
		close(ew.quit)
		ew.wg.Wait()
		ew.started = false
	}
}

// AddInvoice adds a new invoice to the InvoiceExpiryWatcher. This won't check
// if the invoice is already added and will only add invoices with ContractOpen
// state.
func (ew *InvoiceExpiryWatcher) AddInvoice(
	paymentHash lntypes.Hash, invoice *channeldb.Invoice) {

	if invoice.State != channeldb.ContractOpen {
		log.Debugf("Invoice not added to expiry watcher: %v", invoice)
		return
	}

	expiry := invoice.CreationDate.Add(
		zpay32.ToInvoiceExpiry(invoice.Terms.Expiry),
	)

	log.Debugf("Adding invoice '%v' to expiry watcher, expiration: %v",
		paymentHash, expiry)

	ew.newInvoices <- &invoiceExpiry{
		PaymentHash: paymentHash,
		Expiry:      expiry,
	}
}

// nextExpiry returns a Time chan to wait on until the next invoice expires.
// If there are no active invoices, then it'll simply wait indefinitely.
func (ew *InvoiceExpiryWatcher) nextExpiry() <-chan time.Time {
	if !ew.expiryQueue.Empty() {
		top := ew.expiryQueue.Top().(*invoiceExpiry)
		return time.After(top.Expiry.Sub(ew.clock.Now()))
	}

	return nil
}

// cancelExpiredInvoices will cancel all expired invoices and removes them from
// the expiry queue.
func (ew *InvoiceExpiryWatcher) cancelExpiredInvoices() {
	for !ew.expiryQueue.Empty() {
		top := ew.expiryQueue.Top().(*invoiceExpiry)
		if top.Expiry.Before(ew.clock.Now()) {
			log.Infof("Cancelling expired invoice '%v', expiry: %v",
				top.PaymentHash, top.Expiry)

			if err := ew.cancelInvoice(top.PaymentHash); err != nil {
				log.Errorf("Unable to cancel invoice: %v", top.PaymentHash)
			}
			ew.expiryQueue.Pop()
		} else {
			break
		}
	}
}

// mainLoop is a goroutine that receives new invoices and handles cancellation
// of expired invoices.
func (ew *InvoiceExpiryWatcher) mainLoop() {
	defer ew.wg.Done()

	for {
		// Cancel any invoices that may have expired.
		ew.cancelExpiredInvoices()

		select {
		case <-ew.nextExpiry():
			// Wait until the next invoice expires, then cancel expired invoices.
			continue

		case newInvoiceExpiry := <-ew.newInvoices:
			ew.expiryQueue.Push(newInvoiceExpiry)

		case <-ew.quit:
			return
		}
	}
}
