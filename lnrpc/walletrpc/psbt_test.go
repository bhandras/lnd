//go:build walletrpc
// +build walletrpc

package walletrpc

import (
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btcwallet/wtxmgr"
	"github.com/lightningnetwork/lnd/lntest/mock"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/stretchr/testify/require"
)

// leaseOptionsWallet records the optional lease settings passed by lockInputs.
type leaseOptionsWallet struct {
	*mock.WalletController

	leaseCalls  []lnwallet.LeaseOutputOptions
	releasedIDs []wtxmgr.LockID
	failCall    int
}

// LeaseOutputWithOptions records the requested behavior and optionally fails
// one call so the partial-lock rollback path can be asserted.
func (w *leaseOptionsWallet) LeaseOutputWithOptions(_ wtxmgr.LockID,
	_ wire.OutPoint, _ time.Duration,
	opts lnwallet.LeaseOutputOptions) (time.Time, error) {

	w.leaseCalls = append(w.leaseCalls, opts)
	if w.failCall > 0 && len(w.leaseCalls) == w.failCall {
		return time.Time{}, errors.New("lease failed")
	}

	return time.Unix(123, 0), nil
}

// ReleaseOutput records the lock ID used to roll back an acquired lease.
func (w *leaseOptionsWallet) ReleaseOutput(id wtxmgr.LockID,
	_ wire.OutPoint) error {

	w.releasedIDs = append(w.releasedIDs, id)

	return nil
}

// TestLockInputsForwardsReleaseAfterSpend verifies that FundPsbt's lease helper
// passes the requested confirmation depth to every selected input.
func TestLockInputsForwardsReleaseAfterSpend(t *testing.T) {
	t.Parallel()

	wallet := &leaseOptionsWallet{
		WalletController: &mock.WalletController{},
	}
	lockID := wtxmgr.LockID{1, 2, 3}
	outpoints := []wire.OutPoint{
		{Index: 1},
		{Index: 2},
	}

	locks, err := lockInputs(
		wallet, outpoints, &lockID, time.Hour, 6,
	)
	require.NoError(t, err)
	require.Len(t, locks, 2)
	require.Len(t, wallet.leaseCalls, 2)
	for _, opts := range wallet.leaseCalls {
		require.Equal(t, uint32(6), opts.ReleaseAfterSpendConfs)
	}
}

// TestLockInputsRollbackUsesActualLockID verifies that a later lease failure
// releases earlier inputs with the ID that acquired them.
func TestLockInputsRollbackUsesActualLockID(t *testing.T) {
	t.Parallel()

	wallet := &leaseOptionsWallet{
		WalletController: &mock.WalletController{},
		failCall:         2,
	}
	lockID := wtxmgr.LockID{9, 8, 7}
	outpoints := []wire.OutPoint{
		{Index: 1},
		{Index: 2},
	}

	_, err := lockInputs(wallet, outpoints, &lockID, time.Hour, 6)
	require.ErrorContains(t, err, "lease failed")
	require.Equal(t, []wtxmgr.LockID{lockID}, wallet.releasedIDs)
}
