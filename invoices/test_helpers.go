package invoices

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/zpay32"
)

var (
	testTimeout = 5 * time.Second

	testInvoicePreimage = lntypes.Preimage{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
	}

	testInvoicePaymentHash = testInvoicePreimage.Hash()

	testHtlcExpiry = uint32(5)

	testInvoiceCltvDelta = uint32(4)

	testFinalCltvRejectDelta = int32(4)

	testCurrentHeight = int32(1)

	testPrivKeyBytes, _ = hex.DecodeString(
		"e126f68f7eafcc8b74f54d269fe206be715000f94dac067d1c04a8ca3b2db734")

	testPrivKey, _ = btcec.PrivKeyFromBytes(
		btcec.S256(), testPrivKeyBytes)

	testInvoiceDescription = "coffee"

	testInvoiceAmount = lnwire.MilliSatoshi(100000)

	testNetParams = &chaincfg.MainNetParams

	testMessageSigner = zpay32.MessageSigner{
		SignCompact: func(hash []byte) ([]byte, error) {
			sig, err := btcec.SignCompact(btcec.S256(), testPrivKey, hash, true)
			if err != nil {
				return nil, fmt.Errorf("can't sign the message: %v", err)
			}
			return sig, nil
		},
	}

	testFeatures = lnwire.NewFeatureVector(
		nil, lnwire.Features,
	)

	testNow = time.Date(2017, time.July, 21, 12, 0, 0, 0, time.UTC)

	testInvoiceCreationDate = testNow
)

type testClock struct{}

func (t *testClock) Now() time.Time {
	return testNow
}

var (
	testInvoice = &channeldb.Invoice{
		Terms: channeldb.ContractTerm{
			PaymentPreimage: testInvoicePreimage,
			Value:           lnwire.MilliSatoshi(100000),
			Expiry:          time.Hour,
			Features:        testFeatures,
		},
		CreationDate: testInvoiceCreationDate,
	}

	testHodlInvoice = &channeldb.Invoice{
		Terms: channeldb.ContractTerm{
			PaymentPreimage: channeldb.UnknownPreimage,
			Value:           lnwire.MilliSatoshi(100000),
			Expiry:          time.Hour,
			Features:        testFeatures,
		},
		CreationDate: testInvoiceCreationDate,
	}
)

func newTestChannelDB() (*channeldb.DB, func(), error) {
	// First, create a temporary directory to be used for the duration of
	// this test.
	tempDirName, err := ioutil.TempDir("", "channeldb")
	if err != nil {
		return nil, nil, err
	}

	// Next, create channeldb for the first time.
	cdb, err := channeldb.Open(tempDirName)
	if err != nil {
		os.RemoveAll(tempDirName)
		return nil, nil, err
	}

	cleanUp := func() {
		cdb.Close()
		os.RemoveAll(tempDirName)
	}

	return cdb, cleanUp, nil
}

func newTestContext(t *testing.T) (*InvoiceRegistry, func()) {
	cdb, cleanup, err := newTestChannelDB()
	if err != nil {
		t.Fatal(err)
	}

	expiryWatcher := NewInvoiceExpiryWatcher(&testClock{})

	// Instantiate and start the invoice registry.
	registry := NewRegistry(cdb, expiryWatcher, testFinalCltvRejectDelta)

	err = registry.Start()
	if err != nil {
		cleanup()
		t.Fatal(err)
	}

	return registry, func() {
		registry.Stop()
		cleanup()
	}
}

func newTestInvoice(t *testing.T,
	timestamp time.Time, expiry time.Duration) *channeldb.Invoice {

	if expiry == 0 {
		expiry = time.Hour
	}

	rawInvoice, err := zpay32.NewInvoice(
		testNetParams,
		testInvoicePaymentHash,
		timestamp,
		zpay32.Amount(testInvoiceAmount),
		zpay32.Description(testInvoiceDescription),
		zpay32.Expiry(expiry))

	if err != nil {
		t.Fatalf("Error while creating new invoice: %v", err)
	}

	paymentRequest, err := rawInvoice.Encode(testMessageSigner)

	if err != nil {
		t.Fatalf("Error while encoding payment request: %v", err)
	}

	return &channeldb.Invoice{
		Terms: channeldb.ContractTerm{
			PaymentPreimage: testInvoicePreimage,
			Value:           testInvoiceAmount,
			Expiry:          expiry,
			Features:        testFeatures,
		},
		PaymentRequest: []byte(paymentRequest),
		CreationDate:   timestamp,
	}
}
