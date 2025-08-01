package discovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightninglabs/neutrino/cache"
	"github.com/lightninglabs/neutrino/cache/lru"
	"github.com/lightningnetwork/lnd/batch"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/graph"
	graphdb "github.com/lightningnetwork/lnd/graph/db"
	"github.com/lightningnetwork/lnd/graph/db/models"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnpeer"
	"github.com/lightningnetwork/lnd/lnutils"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/btcwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chanvalidate"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/multimutex"
	"github.com/lightningnetwork/lnd/netann"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/lightningnetwork/lnd/ticker"
	"golang.org/x/time/rate"
)

const (
	// DefaultMaxChannelUpdateBurst is the default maximum number of updates
	// for a specific channel and direction that we'll accept over an
	// interval.
	DefaultMaxChannelUpdateBurst = 10

	// DefaultChannelUpdateInterval is the default interval we'll use to
	// determine how often we should allow a new update for a specific
	// channel and direction.
	DefaultChannelUpdateInterval = time.Minute

	// maxPrematureUpdates tracks the max amount of premature channel
	// updates that we'll hold onto.
	maxPrematureUpdates = 100

	// maxFutureMessages tracks the max amount of future messages that
	// we'll hold onto.
	maxFutureMessages = 1000

	// DefaultSubBatchDelay is the default delay we'll use when
	// broadcasting the next announcement batch.
	DefaultSubBatchDelay = 5 * time.Second

	// maxRejectedUpdates tracks the max amount of rejected channel updates
	// we'll maintain. This is the global size across all peers. We'll
	// allocate ~3 MB max to the cache.
	maxRejectedUpdates = 10_000

	// DefaultProofMatureDelta specifies the default value used for
	// ProofMatureDelta, which is the number of confirmations needed before
	// processing the announcement signatures.
	DefaultProofMatureDelta = 6
)

var (
	// ErrGossiperShuttingDown is an error that is returned if the gossiper
	// is in the process of being shut down.
	ErrGossiperShuttingDown = errors.New("gossiper is shutting down")

	// ErrGossipSyncerNotFound signals that we were unable to find an active
	// gossip syncer corresponding to a gossip query message received from
	// the remote peer.
	ErrGossipSyncerNotFound = errors.New("gossip syncer not found")

	// ErrNoFundingTransaction is returned when we are unable to find the
	// funding transaction described by the short channel ID on chain.
	ErrNoFundingTransaction = errors.New(
		"unable to find the funding transaction",
	)

	// ErrInvalidFundingOutput is returned if the channel funding output
	// fails validation.
	ErrInvalidFundingOutput = errors.New(
		"channel funding output validation failed",
	)

	// ErrChannelSpent is returned when we go to validate a channel, but
	// the purported funding output has actually already been spent on
	// chain.
	ErrChannelSpent = errors.New("channel output has been spent")

	// emptyPubkey is used to compare compressed pubkeys against an empty
	// byte array.
	emptyPubkey [33]byte
)

// optionalMsgFields is a set of optional message fields that external callers
// can provide that serve useful when processing a specific network
// announcement.
type optionalMsgFields struct {
	capacity      *btcutil.Amount
	channelPoint  *wire.OutPoint
	remoteAlias   *lnwire.ShortChannelID
	tapscriptRoot fn.Option[chainhash.Hash]
}

// apply applies the optional fields within the functional options.
func (f *optionalMsgFields) apply(optionalMsgFields ...OptionalMsgField) {
	for _, optionalMsgField := range optionalMsgFields {
		optionalMsgField(f)
	}
}

// OptionalMsgField is a functional option parameter that can be used to provide
// external information that is not included within a network message but serves
// useful when processing it.
type OptionalMsgField func(*optionalMsgFields)

// ChannelCapacity is an optional field that lets the gossiper know of the
// capacity of a channel.
func ChannelCapacity(capacity btcutil.Amount) OptionalMsgField {
	return func(f *optionalMsgFields) {
		f.capacity = &capacity
	}
}

// ChannelPoint is an optional field that lets the gossiper know of the outpoint
// of a channel.
func ChannelPoint(op wire.OutPoint) OptionalMsgField {
	return func(f *optionalMsgFields) {
		f.channelPoint = &op
	}
}

// TapscriptRoot is an optional field that lets the gossiper know of the root of
// the tapscript tree for a custom channel.
func TapscriptRoot(root fn.Option[chainhash.Hash]) OptionalMsgField {
	return func(f *optionalMsgFields) {
		f.tapscriptRoot = root
	}
}

// RemoteAlias is an optional field that lets the gossiper know that a locally
// sent channel update is actually an update for the peer that should replace
// the ShortChannelID field with the remote's alias. This is only used for
// channels with peers where the option-scid-alias feature bit was negotiated.
// The channel update will be added to the graph under the original SCID, but
// will be modified and re-signed with this alias.
func RemoteAlias(alias *lnwire.ShortChannelID) OptionalMsgField {
	return func(f *optionalMsgFields) {
		f.remoteAlias = alias
	}
}

// networkMsg couples a routing related wire message with the peer that
// originally sent it.
type networkMsg struct {
	peer              lnpeer.Peer
	source            *btcec.PublicKey
	msg               lnwire.Message
	optionalMsgFields *optionalMsgFields

	isRemote bool

	err chan error
}

// chanPolicyUpdateRequest is a request that is sent to the server when a caller
// wishes to update a particular set of channels. New ChannelUpdate messages
// will be crafted to be sent out during the next broadcast epoch and the fee
// updates committed to the lower layer.
type chanPolicyUpdateRequest struct {
	edgesToUpdate []EdgeWithInfo
	errChan       chan error
}

// PinnedSyncers is a set of node pubkeys for which we will maintain an active
// syncer at all times.
type PinnedSyncers map[route.Vertex]struct{}

// Config defines the configuration for the service. ALL elements within the
// configuration MUST be non-nil for the service to carry out its duties.
type Config struct {
	// ChainHash is a hash that indicates which resident chain of the
	// AuthenticatedGossiper. Any announcements that don't match this
	// chain hash will be ignored.
	//
	// TODO(roasbeef): eventually make into map so can de-multiplex
	// incoming announcements
	//   * also need to do same for Notifier
	ChainHash chainhash.Hash

	// Graph is the subsystem which is responsible for managing the
	// topology of lightning network. After incoming channel, node, channel
	// updates announcements are validated they are sent to the router in
	// order to be included in the LN graph.
	Graph graph.ChannelGraphSource

	// ChainIO represents an abstraction over a source that can query the
	// blockchain.
	ChainIO lnwallet.BlockChainIO

	// ChanSeries is an interfaces that provides access to a time series
	// view of the current known channel graph. Each GossipSyncer enabled
	// peer will utilize this in order to create and respond to channel
	// graph time series queries.
	ChanSeries ChannelGraphTimeSeries

	// Notifier is used for receiving notifications of incoming blocks.
	// With each new incoming block found we process previously premature
	// announcements.
	//
	// TODO(roasbeef): could possibly just replace this with an epoch
	// channel.
	Notifier chainntnfs.ChainNotifier

	// Broadcast broadcasts a particular set of announcements to all peers
	// that the daemon is connected to. If supplied, the exclude parameter
	// indicates that the target peer should be excluded from the
	// broadcast.
	Broadcast func(skips map[route.Vertex]struct{},
		msg ...lnwire.Message) error

	// NotifyWhenOnline is a function that allows the gossiper to be
	// notified when a certain peer comes online, allowing it to
	// retry sending a peer message.
	//
	// NOTE: The peerChan channel must be buffered.
	NotifyWhenOnline func(peerPubKey [33]byte, peerChan chan<- lnpeer.Peer)

	// NotifyWhenOffline is a function that allows the gossiper to be
	// notified when a certain peer disconnects, allowing it to request a
	// notification for when it reconnects.
	NotifyWhenOffline func(peerPubKey [33]byte) <-chan struct{}

	// FetchSelfAnnouncement retrieves our current node announcement, for
	// use when determining whether we should update our peers about our
	// presence in the network.
	FetchSelfAnnouncement func() lnwire.NodeAnnouncement

	// UpdateSelfAnnouncement produces a new announcement for our node with
	// an updated timestamp which can be broadcast to our peers.
	UpdateSelfAnnouncement func() (lnwire.NodeAnnouncement, error)

	// ProofMatureDelta the number of confirmations which is needed before
	// exchange the channel announcement proofs.
	ProofMatureDelta uint32

	// TrickleDelay the period of trickle timer which flushes to the
	// network the pending batch of new announcements we've received since
	// the last trickle tick.
	TrickleDelay time.Duration

	// RetransmitTicker is a ticker that ticks with a period which
	// indicates that we should check if we need re-broadcast any of our
	// personal channels.
	RetransmitTicker ticker.Ticker

	// RebroadcastInterval is the maximum time we wait between sending out
	// channel updates for our active channels and our own node
	// announcement. We do this to ensure our active presence on the
	// network is known, and we are not being considered a zombie node or
	// having zombie channels.
	RebroadcastInterval time.Duration

	// WaitingProofStore is a persistent storage of partial channel proof
	// announcement messages. We use it to buffer half of the material
	// needed to reconstruct a full authenticated channel announcement.
	// Once we receive the other half the channel proof, we'll be able to
	// properly validate it and re-broadcast it out to the network.
	//
	// TODO(wilmer): make interface to prevent channeldb dependency.
	WaitingProofStore *channeldb.WaitingProofStore

	// MessageStore is a persistent storage of gossip messages which we will
	// use to determine which messages need to be resent for a given peer.
	MessageStore GossipMessageStore

	// AnnSigner is an instance of the MessageSigner interface which will
	// be used to manually sign any outgoing channel updates. The signer
	// implementation should be backed by the public key of the backing
	// Lightning node.
	//
	// TODO(roasbeef): extract ann crafting + sign from fundingMgr into
	// here?
	AnnSigner lnwallet.MessageSigner

	// ScidCloser is an instance of ClosedChannelTracker that helps the
	// gossiper cut down on spam channel announcements for already closed
	// channels.
	ScidCloser ClosedChannelTracker

	// NumActiveSyncers is the number of peers for which we should have
	// active syncers with. After reaching NumActiveSyncers, any future
	// gossip syncers will be passive.
	NumActiveSyncers int

	// NoTimestampQueries will prevent the GossipSyncer from querying
	// timestamps of announcement messages from the peer and from replying
	// to timestamp queries.
	NoTimestampQueries bool

	// RotateTicker is a ticker responsible for notifying the SyncManager
	// when it should rotate its active syncers. A single active syncer with
	// a chansSynced state will be exchanged for a passive syncer in order
	// to ensure we don't keep syncing with the same peers.
	RotateTicker ticker.Ticker

	// HistoricalSyncTicker is a ticker responsible for notifying the
	// syncManager when it should attempt a historical sync with a gossip
	// sync peer.
	HistoricalSyncTicker ticker.Ticker

	// ActiveSyncerTimeoutTicker is a ticker responsible for notifying the
	// syncManager when it should attempt to start the next pending
	// activeSyncer due to the current one not completing its state machine
	// within the timeout.
	ActiveSyncerTimeoutTicker ticker.Ticker

	// MinimumBatchSize is minimum size of a sub batch of announcement
	// messages.
	MinimumBatchSize int

	// SubBatchDelay is the delay between sending sub batches of
	// gossip messages.
	SubBatchDelay time.Duration

	// IgnoreHistoricalFilters will prevent syncers from replying with
	// historical data when the remote peer sets a gossip_timestamp_range.
	// This prevents ranges with old start times from causing us to dump the
	// graph on connect.
	IgnoreHistoricalFilters bool

	// PinnedSyncers is a set of peers that will always transition to
	// ActiveSync upon connection. These peers will never transition to
	// PassiveSync.
	PinnedSyncers PinnedSyncers

	// MaxChannelUpdateBurst specifies the maximum number of updates for a
	// specific channel and direction that we'll accept over an interval.
	MaxChannelUpdateBurst int

	// ChannelUpdateInterval specifies the interval we'll use to determine
	// how often we should allow a new update for a specific channel and
	// direction.
	ChannelUpdateInterval time.Duration

	// IsAlias returns true if a given ShortChannelID is an alias for
	// option_scid_alias channels.
	IsAlias func(scid lnwire.ShortChannelID) bool

	// SignAliasUpdate is used to re-sign a channel update using the
	// remote's alias if the option-scid-alias feature bit was negotiated.
	SignAliasUpdate func(u *lnwire.ChannelUpdate1) (*ecdsa.Signature,
		error)

	// FindBaseByAlias finds the SCID stored in the graph by an alias SCID.
	// This is used for channels that have negotiated the option-scid-alias
	// feature bit.
	FindBaseByAlias func(alias lnwire.ShortChannelID) (
		lnwire.ShortChannelID, error)

	// GetAlias allows the gossiper to look up the peer's alias for a given
	// ChannelID. This is used to sign updates for them if the channel has
	// no AuthProof and the option-scid-alias feature bit was negotiated.
	GetAlias func(lnwire.ChannelID) (lnwire.ShortChannelID, error)

	// FindChannel allows the gossiper to find a channel that we're party
	// to without iterating over the entire set of open channels.
	FindChannel func(node *btcec.PublicKey, chanID lnwire.ChannelID) (
		*channeldb.OpenChannel, error)

	// IsStillZombieChannel takes the timestamps of the latest channel
	// updates for a channel and returns true if the channel should be
	// considered a zombie based on these timestamps.
	IsStillZombieChannel func(time.Time, time.Time) bool

	// AssumeChannelValid toggles whether the gossiper will check for
	// spent-ness of channel outpoints. For neutrino, this saves long
	// rescans from blocking initial usage of the daemon.
	AssumeChannelValid bool

	// MsgRateBytes is the rate limit for the number of bytes per second
	// that we'll allocate to outbound gossip messages.
	MsgRateBytes uint64

	// MsgBurstBytes is the allotted burst amount in bytes. This is the
	// number of starting tokens in our token bucket algorithm.
	MsgBurstBytes uint64
}

// processedNetworkMsg is a wrapper around networkMsg and a boolean. It is
// used to let the caller of the lru.Cache know if a message has already been
// processed or not.
type processedNetworkMsg struct {
	processed bool
	msg       *networkMsg
}

// cachedNetworkMsg is a wrapper around a network message that can be used with
// *lru.Cache.
//
// NOTE: This struct is not thread safe which means you need to assure no
// concurrent read write access to it and all its contents which are pointers
// as well.
type cachedNetworkMsg struct {
	msgs []*processedNetworkMsg
}

// Size returns the "size" of an entry. We return the number of items as we
// just want to limit the total amount of entries rather than do accurate size
// accounting.
func (c *cachedNetworkMsg) Size() (uint64, error) {
	return uint64(len(c.msgs)), nil
}

// rejectCacheKey is the cache key that we'll use to track announcements we've
// recently rejected.
type rejectCacheKey struct {
	pubkey [33]byte
	chanID uint64
}

// newRejectCacheKey returns a new cache key for the reject cache.
func newRejectCacheKey(cid uint64, pub [33]byte) rejectCacheKey {
	k := rejectCacheKey{
		chanID: cid,
		pubkey: pub,
	}

	return k
}

// sourceToPub returns a serialized-compressed public key for use in the reject
// cache.
func sourceToPub(pk *btcec.PublicKey) [33]byte {
	var pub [33]byte
	copy(pub[:], pk.SerializeCompressed())
	return pub
}

// cachedReject is the empty value used to track the value for rejects.
type cachedReject struct {
}

// Size returns the "size" of an entry. We return 1 as we just want to limit
// the total size.
func (c *cachedReject) Size() (uint64, error) {
	return 1, nil
}

// AuthenticatedGossiper is a subsystem which is responsible for receiving
// announcements, validating them and applying the changes to router, syncing
// lightning network with newly connected nodes, broadcasting announcements
// after validation, negotiating the channel announcement proofs exchange and
// handling the premature announcements. All outgoing announcements are
// expected to be properly signed as dictated in BOLT#7, additionally, all
// incoming message are expected to be well formed and signed. Invalid messages
// will be rejected by this struct.
type AuthenticatedGossiper struct {
	// Parameters which are needed to properly handle the start and stop of
	// the service.
	started sync.Once
	stopped sync.Once

	// bestHeight is the height of the block at the tip of the main chain
	// as we know it. Accesses *MUST* be done with the gossiper's lock
	// held.
	bestHeight uint32

	// cfg is a copy of the configuration struct that the gossiper service
	// was initialized with.
	cfg *Config

	// blockEpochs encapsulates a stream of block epochs that are sent at
	// every new block height.
	blockEpochs *chainntnfs.BlockEpochEvent

	// prematureChannelUpdates is a map of ChannelUpdates we have received
	// that wasn't associated with any channel we know about.  We store
	// them temporarily, such that we can reprocess them when a
	// ChannelAnnouncement for the channel is received.
	prematureChannelUpdates *lru.Cache[uint64, *cachedNetworkMsg]

	// banman tracks our peer's ban status.
	banman *banman

	// networkMsgs is a channel that carries new network broadcasted
	// message from outside the gossiper service to be processed by the
	// networkHandler.
	networkMsgs chan *networkMsg

	// futureMsgs is a list of premature network messages that have a block
	// height specified in the future. We will save them and resend it to
	// the chan networkMsgs once the block height has reached. The cached
	// map format is,
	//   {msgID1: msg1, msgID2: msg2, ...}
	futureMsgs *futureMsgCache

	// chanPolicyUpdates is a channel that requests to update the
	// forwarding policy of a set of channels is sent over.
	chanPolicyUpdates chan *chanPolicyUpdateRequest

	// selfKey is the identity public key of the backing Lightning node.
	selfKey *btcec.PublicKey

	// selfKeyLoc is the locator for the identity public key of the backing
	// Lightning node.
	selfKeyLoc keychain.KeyLocator

	// channelMtx is used to restrict the database access to one
	// goroutine per channel ID. This is done to ensure that when
	// the gossiper is handling an announcement, the db state stays
	// consistent between when the DB is first read until it's written.
	channelMtx *multimutex.Mutex[uint64]

	recentRejects *lru.Cache[rejectCacheKey, *cachedReject]

	// syncMgr is a subsystem responsible for managing the gossip syncers
	// for peers currently connected. When a new peer is connected, the
	// manager will create its accompanying gossip syncer and determine
	// whether it should have an activeSync or passiveSync sync type based
	// on how many other gossip syncers are currently active. Any activeSync
	// gossip syncers are started in a round-robin manner to ensure we're
	// not syncing with multiple peers at the same time.
	syncMgr *SyncManager

	// reliableSender is a subsystem responsible for handling reliable
	// message send requests to peers. This should only be used for channels
	// that are unadvertised at the time of handling the message since if it
	// is advertised, then peers should be able to get the message from the
	// network.
	reliableSender *reliableSender

	// chanUpdateRateLimiter contains rate limiters for each direction of
	// a channel update we've processed. We'll use these to determine
	// whether we should accept a new update for a specific channel and
	// direction.
	//
	// NOTE: This map must be synchronized with the main
	// AuthenticatedGossiper lock.
	chanUpdateRateLimiter map[uint64][2]*rate.Limiter

	// vb is used to enforce job dependency ordering of gossip messages.
	vb *ValidationBarrier

	sync.Mutex

	cancel fn.Option[context.CancelFunc]
	quit   chan struct{}
	wg     sync.WaitGroup
}

// New creates a new AuthenticatedGossiper instance, initialized with the
// passed configuration parameters.
func New(cfg Config, selfKeyDesc *keychain.KeyDescriptor) *AuthenticatedGossiper {
	gossiper := &AuthenticatedGossiper{
		selfKey:           selfKeyDesc.PubKey,
		selfKeyLoc:        selfKeyDesc.KeyLocator,
		cfg:               &cfg,
		networkMsgs:       make(chan *networkMsg),
		futureMsgs:        newFutureMsgCache(maxFutureMessages),
		quit:              make(chan struct{}),
		chanPolicyUpdates: make(chan *chanPolicyUpdateRequest),
		prematureChannelUpdates: lru.NewCache[uint64, *cachedNetworkMsg]( //nolint: ll
			maxPrematureUpdates,
		),
		channelMtx: multimutex.NewMutex[uint64](),
		recentRejects: lru.NewCache[rejectCacheKey, *cachedReject](
			maxRejectedUpdates,
		),
		chanUpdateRateLimiter: make(map[uint64][2]*rate.Limiter),
		banman:                newBanman(),
	}

	gossiper.vb = NewValidationBarrier(1000, gossiper.quit)

	gossiper.syncMgr = newSyncManager(&SyncManagerCfg{
		ChainHash:                cfg.ChainHash,
		ChanSeries:               cfg.ChanSeries,
		RotateTicker:             cfg.RotateTicker,
		HistoricalSyncTicker:     cfg.HistoricalSyncTicker,
		NumActiveSyncers:         cfg.NumActiveSyncers,
		NoTimestampQueries:       cfg.NoTimestampQueries,
		IgnoreHistoricalFilters:  cfg.IgnoreHistoricalFilters,
		BestHeight:               gossiper.latestHeight,
		PinnedSyncers:            cfg.PinnedSyncers,
		IsStillZombieChannel:     cfg.IsStillZombieChannel,
		AllotedMsgBytesPerSecond: cfg.MsgRateBytes,
		AllotedMsgBytesBurst:     cfg.MsgBurstBytes,
	})

	gossiper.reliableSender = newReliableSender(&reliableSenderCfg{
		NotifyWhenOnline:  cfg.NotifyWhenOnline,
		NotifyWhenOffline: cfg.NotifyWhenOffline,
		MessageStore:      cfg.MessageStore,
		IsMsgStale:        gossiper.isMsgStale,
	})

	return gossiper
}

// EdgeWithInfo contains the information that is required to update an edge.
type EdgeWithInfo struct {
	// Info describes the channel.
	Info *models.ChannelEdgeInfo

	// Edge describes the policy in one direction of the channel.
	Edge *models.ChannelEdgePolicy
}

// PropagateChanPolicyUpdate signals the AuthenticatedGossiper to perform the
// specified edge updates. Updates are done in two stages: first, the
// AuthenticatedGossiper ensures the update has been committed by dependent
// sub-systems, then it signs and broadcasts new updates to the network. A
// mapping between outpoints and updated channel policies is returned, which is
// used to update the forwarding policies of the underlying links.
func (d *AuthenticatedGossiper) PropagateChanPolicyUpdate(
	edgesToUpdate []EdgeWithInfo) error {

	errChan := make(chan error, 1)
	policyUpdate := &chanPolicyUpdateRequest{
		edgesToUpdate: edgesToUpdate,
		errChan:       errChan,
	}

	select {
	case d.chanPolicyUpdates <- policyUpdate:
		err := <-errChan
		return err
	case <-d.quit:
		return fmt.Errorf("AuthenticatedGossiper shutting down")
	}
}

// Start spawns network messages handler goroutine and registers on new block
// notifications in order to properly handle the premature announcements.
func (d *AuthenticatedGossiper) Start() error {
	var err error
	d.started.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		d.cancel = fn.Some(cancel)

		log.Info("Authenticated Gossiper starting")
		err = d.start(ctx)
	})
	return err
}

func (d *AuthenticatedGossiper) start(ctx context.Context) error {
	// First we register for new notifications of newly discovered blocks.
	// We do this immediately so we'll later be able to consume any/all
	// blocks which were discovered.
	blockEpochs, err := d.cfg.Notifier.RegisterBlockEpochNtfn(nil)
	if err != nil {
		return err
	}
	d.blockEpochs = blockEpochs

	height, err := d.cfg.Graph.CurrentBlockHeight()
	if err != nil {
		return err
	}
	d.bestHeight = height

	// Start the reliable sender. In case we had any pending messages ready
	// to be sent when the gossiper was last shut down, we must continue on
	// our quest to deliver them to their respective peers.
	if err := d.reliableSender.Start(); err != nil {
		return err
	}

	d.syncMgr.Start()

	d.banman.start()

	// Start receiving blocks in its dedicated goroutine.
	d.wg.Add(2)
	go d.syncBlockHeight()
	go d.networkHandler(ctx)

	return nil
}

// syncBlockHeight syncs the best block height for the gossiper by reading
// blockEpochs.
//
// NOTE: must be run as a goroutine.
func (d *AuthenticatedGossiper) syncBlockHeight() {
	defer d.wg.Done()

	for {
		select {
		// A new block has arrived, so we can re-process the previously
		// premature announcements.
		case newBlock, ok := <-d.blockEpochs.Epochs:
			// If the channel has been closed, then this indicates
			// the daemon is shutting down, so we exit ourselves.
			if !ok {
				return
			}

			// Once a new block arrives, we update our running
			// track of the height of the chain tip.
			d.Lock()
			blockHeight := uint32(newBlock.Height)
			d.bestHeight = blockHeight
			d.Unlock()

			log.Debugf("New block: height=%d, hash=%s", blockHeight,
				newBlock.Hash)

			// Resend future messages, if any.
			d.resendFutureMessages(blockHeight)

		case <-d.quit:
			return
		}
	}
}

// futureMsgCache embeds a `lru.Cache` with a message counter that's served as
// the unique ID when saving the message.
type futureMsgCache struct {
	*lru.Cache[uint64, *cachedFutureMsg]

	// msgID is a monotonically increased integer.
	msgID atomic.Uint64
}

// nextMsgID returns a unique message ID.
func (f *futureMsgCache) nextMsgID() uint64 {
	return f.msgID.Add(1)
}

// newFutureMsgCache creates a new future message cache with the underlying lru
// cache being initialized with the specified capacity.
func newFutureMsgCache(capacity uint64) *futureMsgCache {
	// Create a new cache.
	cache := lru.NewCache[uint64, *cachedFutureMsg](capacity)

	return &futureMsgCache{
		Cache: cache,
	}
}

// cachedFutureMsg is a future message that's saved to the `futureMsgCache`.
type cachedFutureMsg struct {
	// msg is the network message.
	msg *networkMsg

	// height is the block height.
	height uint32
}

// Size returns the size of the message.
func (c *cachedFutureMsg) Size() (uint64, error) {
	// Return a constant 1.
	return 1, nil
}

// resendFutureMessages takes a block height, resends all the future messages
// found below and equal to that height and deletes those messages found in the
// gossiper's futureMsgs.
func (d *AuthenticatedGossiper) resendFutureMessages(height uint32) {
	var (
		// msgs are the target messages.
		msgs []*networkMsg

		// keys are the target messages' caching keys.
		keys []uint64
	)

	// filterMsgs is the visitor used when iterating the future cache.
	filterMsgs := func(k uint64, cmsg *cachedFutureMsg) bool {
		if cmsg.height <= height {
			msgs = append(msgs, cmsg.msg)
			keys = append(keys, k)
		}

		return true
	}

	// Filter out the target messages.
	d.futureMsgs.Range(filterMsgs)

	// Return early if no messages found.
	if len(msgs) == 0 {
		return
	}

	// Remove the filtered messages.
	for _, key := range keys {
		d.futureMsgs.Delete(key)
	}

	log.Debugf("Resending %d network messages at height %d",
		len(msgs), height)

	for _, msg := range msgs {
		select {
		case d.networkMsgs <- msg:
		case <-d.quit:
			msg.err <- ErrGossiperShuttingDown
		}
	}
}

// Stop signals any active goroutines for a graceful closure.
func (d *AuthenticatedGossiper) Stop() error {
	d.stopped.Do(func() {
		log.Info("Authenticated gossiper shutting down...")
		defer log.Debug("Authenticated gossiper shutdown complete")

		d.stop()
	})
	return nil
}

func (d *AuthenticatedGossiper) stop() {
	log.Debug("Authenticated Gossiper is stopping")
	defer log.Debug("Authenticated Gossiper stopped")

	// `blockEpochs` is only initialized in the start routine so we make
	// sure we don't panic here in the case where the `Stop` method is
	// called when the `Start` method does not complete.
	if d.blockEpochs != nil {
		d.blockEpochs.Cancel()
	}

	d.syncMgr.Stop()

	d.banman.stop()

	d.cancel.WhenSome(func(fn context.CancelFunc) { fn() })
	close(d.quit)
	d.wg.Wait()

	// We'll stop our reliable sender after all of the gossiper's goroutines
	// have exited to ensure nothing can cause it to continue executing.
	d.reliableSender.Stop()
}

// TODO(roasbeef): need method to get current gossip timestamp?
//  * using mtx, check time rotate forward is needed?

// ProcessRemoteAnnouncement sends a new remote announcement message along with
// the peer that sent the routing message. The announcement will be processed
// then added to a queue for batched trickled announcement to all connected
// peers.  Remote channel announcements should contain the announcement proof
// and be fully validated.
func (d *AuthenticatedGossiper) ProcessRemoteAnnouncement(ctx context.Context,
	msg lnwire.Message, peer lnpeer.Peer) chan error {

	errChan := make(chan error, 1)

	// For messages in the known set of channel series queries, we'll
	// dispatch the message directly to the GossipSyncer, and skip the main
	// processing loop.
	switch m := msg.(type) {
	case *lnwire.QueryShortChanIDs,
		*lnwire.QueryChannelRange,
		*lnwire.ReplyChannelRange,
		*lnwire.ReplyShortChanIDsEnd:

		syncer, ok := d.syncMgr.GossipSyncer(peer.PubKey())
		if !ok {
			log.Warnf("Gossip syncer for peer=%x not found",
				peer.PubKey())

			errChan <- ErrGossipSyncerNotFound
			return errChan
		}

		// If we've found the message target, then we'll dispatch the
		// message directly to it.
		err := syncer.ProcessQueryMsg(m, peer.QuitSignal())
		if err != nil {
			log.Errorf("Process query msg from peer %x got %v",
				peer.PubKey(), err)
		}

		errChan <- err
		return errChan

	// If a peer is updating its current update horizon, then we'll dispatch
	// that directly to the proper GossipSyncer.
	case *lnwire.GossipTimestampRange:
		syncer, ok := d.syncMgr.GossipSyncer(peer.PubKey())
		if !ok {
			log.Warnf("Gossip syncer for peer=%x not found",
				peer.PubKey())

			errChan <- ErrGossipSyncerNotFound
			return errChan
		}

		// If we've found the message target, then we'll dispatch the
		// message directly to it.
		if err := syncer.ApplyGossipFilter(ctx, m); err != nil {
			log.Warnf("Unable to apply gossip filter for peer=%x: "+
				"%v", peer.PubKey(), err)

			errChan <- err
			return errChan
		}

		errChan <- nil
		return errChan

	// To avoid inserting edges in the graph for our own channels that we
	// have already closed, we ignore such channel announcements coming
	// from the remote.
	case *lnwire.ChannelAnnouncement1:
		ownKey := d.selfKey.SerializeCompressed()
		ownErr := fmt.Errorf("ignoring remote ChannelAnnouncement1 " +
			"for own channel")

		if bytes.Equal(m.NodeID1[:], ownKey) ||
			bytes.Equal(m.NodeID2[:], ownKey) {

			log.Warn(ownErr)
			errChan <- ownErr
			return errChan
		}
	}

	nMsg := &networkMsg{
		msg:      msg,
		isRemote: true,
		peer:     peer,
		source:   peer.IdentityKey(),
		err:      errChan,
	}

	select {
	case d.networkMsgs <- nMsg:

	// If the peer that sent us this error is quitting, then we don't need
	// to send back an error and can return immediately.
	// TODO(elle): the peer should now just rely on canceling the passed
	//  context.
	case <-peer.QuitSignal():
		return nil
	case <-ctx.Done():
		return nil
	case <-d.quit:
		nMsg.err <- ErrGossiperShuttingDown
	}

	return nMsg.err
}

// ProcessLocalAnnouncement sends a new remote announcement message along with
// the peer that sent the routing message. The announcement will be processed
// then added to a queue for batched trickled announcement to all connected
// peers.  Local channel announcements don't contain the announcement proof and
// will not be fully validated. Once the channel proofs are received, the
// entire channel announcement and update messages will be re-constructed and
// broadcast to the rest of the network.
func (d *AuthenticatedGossiper) ProcessLocalAnnouncement(msg lnwire.Message,
	optionalFields ...OptionalMsgField) chan error {

	optionalMsgFields := &optionalMsgFields{}
	optionalMsgFields.apply(optionalFields...)

	nMsg := &networkMsg{
		msg:               msg,
		optionalMsgFields: optionalMsgFields,
		isRemote:          false,
		source:            d.selfKey,
		err:               make(chan error, 1),
	}

	select {
	case d.networkMsgs <- nMsg:
	case <-d.quit:
		nMsg.err <- ErrGossiperShuttingDown
	}

	return nMsg.err
}

// channelUpdateID is a unique identifier for ChannelUpdate messages, as
// channel updates can be identified by the (ShortChannelID, ChannelFlags)
// tuple.
type channelUpdateID struct {
	// channelID represents the set of data which is needed to
	// retrieve all necessary data to validate the channel existence.
	channelID lnwire.ShortChannelID

	// Flags least-significant bit must be set to 0 if the creating node
	// corresponds to the first node in the previously sent channel
	// announcement and 1 otherwise.
	flags lnwire.ChanUpdateChanFlags
}

// msgWithSenders is a wrapper struct around a message, and the set of peers
// that originally sent us this message. Using this struct, we can ensure that
// we don't re-send a message to the peer that sent it to us in the first
// place.
type msgWithSenders struct {
	// msg is the wire message itself.
	msg lnwire.Message

	// isLocal is true if this was a message that originated locally. We'll
	// use this to bypass our normal checks to ensure we prioritize sending
	// out our own updates.
	isLocal bool

	// sender is the set of peers that sent us this message.
	senders map[route.Vertex]struct{}
}

// mergeSyncerMap is used to merge the set of senders of a particular message
// with peers that we have an active GossipSyncer with. We do this to ensure
// that we don't broadcast messages to any peers that we have active gossip
// syncers for.
func (m *msgWithSenders) mergeSyncerMap(syncers map[route.Vertex]*GossipSyncer) {
	for peerPub := range syncers {
		m.senders[peerPub] = struct{}{}
	}
}

// deDupedAnnouncements de-duplicates announcements that have been added to the
// batch. Internally, announcements are stored in three maps
// (one each for channel announcements, channel updates, and node
// announcements). These maps keep track of unique announcements and ensure no
// announcements are duplicated. We keep the three message types separate, such
// that we can send channel announcements first, then channel updates, and
// finally node announcements when it's time to broadcast them.
type deDupedAnnouncements struct {
	// channelAnnouncements are identified by the short channel id field.
	channelAnnouncements map[lnwire.ShortChannelID]msgWithSenders

	// channelUpdates are identified by the channel update id field.
	channelUpdates map[channelUpdateID]msgWithSenders

	// nodeAnnouncements are identified by the Vertex field.
	nodeAnnouncements map[route.Vertex]msgWithSenders

	sync.Mutex
}

// Reset operates on deDupedAnnouncements to reset the storage of
// announcements.
func (d *deDupedAnnouncements) Reset() {
	d.Lock()
	defer d.Unlock()

	d.reset()
}

// reset is the private version of the Reset method. We have this so we can
// call this method within method that are already holding the lock.
func (d *deDupedAnnouncements) reset() {
	// Storage of each type of announcement (channel announcements, channel
	// updates, node announcements) is set to an empty map where the
	// appropriate key points to the corresponding lnwire.Message.
	d.channelAnnouncements = make(map[lnwire.ShortChannelID]msgWithSenders)
	d.channelUpdates = make(map[channelUpdateID]msgWithSenders)
	d.nodeAnnouncements = make(map[route.Vertex]msgWithSenders)
}

// addMsg adds a new message to the current batch. If the message is already
// present in the current batch, then this new instance replaces the latter,
// and the set of senders is updated to reflect which node sent us this
// message.
func (d *deDupedAnnouncements) addMsg(message networkMsg) {
	log.Tracef("Adding network message: %v to batch", message.msg.MsgType())

	// Depending on the message type (channel announcement, channel update,
	// or node announcement), the message is added to the corresponding map
	// in deDupedAnnouncements. Because each identifying key can have at
	// most one value, the announcements are de-duplicated, with newer ones
	// replacing older ones.
	switch msg := message.msg.(type) {

	// Channel announcements are identified by the short channel id field.
	case *lnwire.ChannelAnnouncement1:
		deDupKey := msg.ShortChannelID
		sender := route.NewVertex(message.source)

		mws, ok := d.channelAnnouncements[deDupKey]
		if !ok {
			mws = msgWithSenders{
				msg:     msg,
				isLocal: !message.isRemote,
				senders: make(map[route.Vertex]struct{}),
			}
			mws.senders[sender] = struct{}{}

			d.channelAnnouncements[deDupKey] = mws

			return
		}

		mws.msg = msg
		mws.senders[sender] = struct{}{}
		d.channelAnnouncements[deDupKey] = mws

	// Channel updates are identified by the (short channel id,
	// channelflags) tuple.
	case *lnwire.ChannelUpdate1:
		sender := route.NewVertex(message.source)
		deDupKey := channelUpdateID{
			msg.ShortChannelID,
			msg.ChannelFlags,
		}

		oldTimestamp := uint32(0)
		mws, ok := d.channelUpdates[deDupKey]
		if ok {
			// If we already have seen this message, record its
			// timestamp.
			update, ok := mws.msg.(*lnwire.ChannelUpdate1)
			if !ok {
				log.Errorf("Expected *lnwire.ChannelUpdate1, "+
					"got: %T", mws.msg)

				return
			}

			oldTimestamp = update.Timestamp
		}

		// If we already had this message with a strictly newer
		// timestamp, then we'll just discard the message we got.
		if oldTimestamp > msg.Timestamp {
			log.Debugf("Ignored outdated network message: "+
				"peer=%v, msg=%s", message.peer, msg.MsgType())
			return
		}

		// If the message we just got is newer than what we previously
		// have seen, or this is the first time we see it, then we'll
		// add it to our map of announcements.
		if oldTimestamp < msg.Timestamp {
			mws = msgWithSenders{
				msg:     msg,
				isLocal: !message.isRemote,
				senders: make(map[route.Vertex]struct{}),
			}

			// We'll mark the sender of the message in the
			// senders map.
			mws.senders[sender] = struct{}{}

			d.channelUpdates[deDupKey] = mws

			return
		}

		// Lastly, if we had seen this exact message from before, with
		// the same timestamp, we'll add the sender to the map of
		// senders, such that we can skip sending this message back in
		// the next batch.
		mws.msg = msg
		mws.senders[sender] = struct{}{}
		d.channelUpdates[deDupKey] = mws

	// Node announcements are identified by the Vertex field.  Use the
	// NodeID to create the corresponding Vertex.
	case *lnwire.NodeAnnouncement:
		sender := route.NewVertex(message.source)
		deDupKey := route.Vertex(msg.NodeID)

		// We do the same for node announcements as we did for channel
		// updates, as they also carry a timestamp.
		oldTimestamp := uint32(0)
		mws, ok := d.nodeAnnouncements[deDupKey]
		if ok {
			oldTimestamp = mws.msg.(*lnwire.NodeAnnouncement).Timestamp
		}

		// Discard the message if it's old.
		if oldTimestamp > msg.Timestamp {
			return
		}

		// Replace if it's newer.
		if oldTimestamp < msg.Timestamp {
			mws = msgWithSenders{
				msg:     msg,
				isLocal: !message.isRemote,
				senders: make(map[route.Vertex]struct{}),
			}

			mws.senders[sender] = struct{}{}

			d.nodeAnnouncements[deDupKey] = mws

			return
		}

		// Add to senders map if it's the same as we had.
		mws.msg = msg
		mws.senders[sender] = struct{}{}
		d.nodeAnnouncements[deDupKey] = mws
	}
}

// AddMsgs is a helper method to add multiple messages to the announcement
// batch.
func (d *deDupedAnnouncements) AddMsgs(msgs ...networkMsg) {
	d.Lock()
	defer d.Unlock()

	for _, msg := range msgs {
		d.addMsg(msg)
	}
}

// msgsToBroadcast is returned by Emit() and partitions the messages we'd like
// to broadcast next into messages that are locally sourced and those that are
// sourced remotely.
type msgsToBroadcast struct {
	// localMsgs is the set of messages we created locally.
	localMsgs []msgWithSenders

	// remoteMsgs is the set of messages that we received from a remote
	// party.
	remoteMsgs []msgWithSenders
}

// addMsg adds a new message to the appropriate sub-slice.
func (m *msgsToBroadcast) addMsg(msg msgWithSenders) {
	if msg.isLocal {
		m.localMsgs = append(m.localMsgs, msg)
	} else {
		m.remoteMsgs = append(m.remoteMsgs, msg)
	}
}

// isEmpty returns true if the batch is empty.
func (m *msgsToBroadcast) isEmpty() bool {
	return len(m.localMsgs) == 0 && len(m.remoteMsgs) == 0
}

// length returns the length of the combined message set.
func (m *msgsToBroadcast) length() int {
	return len(m.localMsgs) + len(m.remoteMsgs)
}

// Emit returns the set of de-duplicated announcements to be sent out during
// the next announcement epoch, in the order of channel announcements, channel
// updates, and node announcements. Each message emitted, contains the set of
// peers that sent us the message. This way, we can ensure that we don't waste
// bandwidth by re-sending a message to the peer that sent it to us in the
// first place. Additionally, the set of stored messages are reset.
func (d *deDupedAnnouncements) Emit() msgsToBroadcast {
	d.Lock()
	defer d.Unlock()

	// Get the total number of announcements.
	numAnnouncements := len(d.channelAnnouncements) + len(d.channelUpdates) +
		len(d.nodeAnnouncements)

	// Create an empty array of lnwire.Messages with a length equal to
	// the total number of announcements.
	msgs := msgsToBroadcast{
		localMsgs:  make([]msgWithSenders, 0, numAnnouncements),
		remoteMsgs: make([]msgWithSenders, 0, numAnnouncements),
	}

	// Add the channel announcements to the array first.
	for _, message := range d.channelAnnouncements {
		msgs.addMsg(message)
	}

	// Then add the channel updates.
	for _, message := range d.channelUpdates {
		msgs.addMsg(message)
	}

	// Finally add the node announcements.
	for _, message := range d.nodeAnnouncements {
		msgs.addMsg(message)
	}

	d.reset()

	// Return the array of lnwire.messages.
	return msgs
}

// calculateSubBatchSize is a helper function that calculates the size to break
// down the batchSize into.
func calculateSubBatchSize(totalDelay, subBatchDelay time.Duration,
	minimumBatchSize, batchSize int) int {
	if subBatchDelay > totalDelay {
		return batchSize
	}

	subBatchSize := (batchSize*int(subBatchDelay) +
		int(totalDelay) - 1) / int(totalDelay)

	if subBatchSize < minimumBatchSize {
		return minimumBatchSize
	}

	return subBatchSize
}

// batchSizeCalculator maps to the function `calculateSubBatchSize`. We create
// this variable so the function can be mocked in our test.
var batchSizeCalculator = calculateSubBatchSize

// splitAnnouncementBatches takes an exiting list of announcements and
// decomposes it into sub batches controlled by the `subBatchSize`.
func (d *AuthenticatedGossiper) splitAnnouncementBatches(
	announcementBatch []msgWithSenders) [][]msgWithSenders {

	subBatchSize := batchSizeCalculator(
		d.cfg.TrickleDelay, d.cfg.SubBatchDelay,
		d.cfg.MinimumBatchSize, len(announcementBatch),
	)

	var splitAnnouncementBatch [][]msgWithSenders

	for subBatchSize < len(announcementBatch) {
		// For slicing with minimal allocation
		// https://github.com/golang/go/wiki/SliceTricks
		announcementBatch, splitAnnouncementBatch =
			announcementBatch[subBatchSize:],
			append(splitAnnouncementBatch,
				announcementBatch[0:subBatchSize:subBatchSize])
	}
	splitAnnouncementBatch = append(
		splitAnnouncementBatch, announcementBatch,
	)

	return splitAnnouncementBatch
}

// splitAndSendAnnBatch takes a batch of messages, computes the proper batch
// split size, and then sends out all items to the set of target peers. Locally
// generated announcements are always sent before remotely generated
// announcements.
func (d *AuthenticatedGossiper) splitAndSendAnnBatch(ctx context.Context,
	annBatch msgsToBroadcast) {

	// delayNextBatch is a helper closure that blocks for `SubBatchDelay`
	// duration to delay the sending of next announcement batch.
	delayNextBatch := func() {
		select {
		case <-time.After(d.cfg.SubBatchDelay):
		case <-d.quit:
			return
		}
	}

	// Fetch the local and remote announcements.
	localBatches := d.splitAnnouncementBatches(annBatch.localMsgs)
	remoteBatches := d.splitAnnouncementBatches(annBatch.remoteMsgs)

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		log.Debugf("Broadcasting %v new local announcements in %d "+
			"sub batches", len(annBatch.localMsgs),
			len(localBatches))

		// Send out the local announcements first.
		for _, annBatch := range localBatches {
			d.sendLocalBatch(annBatch)
			delayNextBatch()
		}

		log.Debugf("Broadcasting %v new remote announcements in %d "+
			"sub batches", len(annBatch.remoteMsgs),
			len(remoteBatches))

		// Now send the remote announcements.
		for _, annBatch := range remoteBatches {
			d.sendRemoteBatch(ctx, annBatch)
			delayNextBatch()
		}
	}()
}

// sendLocalBatch broadcasts a list of locally generated announcements to our
// peers. For local announcements, we skip the filter and dedup logic and just
// send the announcements out to all our coonnected peers.
func (d *AuthenticatedGossiper) sendLocalBatch(annBatch []msgWithSenders) {
	msgsToSend := lnutils.Map(
		annBatch, func(m msgWithSenders) lnwire.Message {
			return m.msg
		},
	)

	err := d.cfg.Broadcast(nil, msgsToSend...)
	if err != nil {
		log.Errorf("Unable to send local batch announcements: %v", err)
	}
}

// sendRemoteBatch broadcasts a list of remotely generated announcements to our
// peers.
func (d *AuthenticatedGossiper) sendRemoteBatch(ctx context.Context,
	annBatch []msgWithSenders) {

	syncerPeers := d.syncMgr.GossipSyncers()

	// We'll first attempt to filter out this new message for all peers
	// that have active gossip syncers active.
	for pub, syncer := range syncerPeers {
		log.Tracef("Sending messages batch to GossipSyncer(%s)", pub)
		syncer.FilterGossipMsgs(ctx, annBatch...)
	}

	for _, msgChunk := range annBatch {
		msgChunk := msgChunk

		// With the syncers taken care of, we'll merge the sender map
		// with the set of syncers, so we don't send out duplicate
		// messages.
		msgChunk.mergeSyncerMap(syncerPeers)

		err := d.cfg.Broadcast(msgChunk.senders, msgChunk.msg)
		if err != nil {
			log.Errorf("Unable to send batch "+
				"announcements: %v", err)
			continue
		}
	}
}

// networkHandler is the primary goroutine that drives this service. The roles
// of this goroutine includes answering queries related to the state of the
// network, syncing up newly connected peers, and also periodically
// broadcasting our latest topology state to all connected peers.
//
// NOTE: This MUST be run as a goroutine.
func (d *AuthenticatedGossiper) networkHandler(ctx context.Context) {
	defer d.wg.Done()

	// Initialize empty deDupedAnnouncements to store announcement batch.
	announcements := deDupedAnnouncements{}
	announcements.Reset()

	d.cfg.RetransmitTicker.Resume()
	defer d.cfg.RetransmitTicker.Stop()

	trickleTimer := time.NewTicker(d.cfg.TrickleDelay)
	defer trickleTimer.Stop()

	// To start, we'll first check to see if there are any stale channel or
	// node announcements that we need to re-transmit.
	if err := d.retransmitStaleAnns(ctx, time.Now()); err != nil {
		log.Errorf("Unable to rebroadcast stale announcements: %v", err)
	}

	for {
		select {
		// A new policy update has arrived. We'll commit it to the
		// sub-systems below us, then craft, sign, and broadcast a new
		// ChannelUpdate for the set of affected clients.
		case policyUpdate := <-d.chanPolicyUpdates:
			log.Tracef("Received channel %d policy update requests",
				len(policyUpdate.edgesToUpdate))

			// First, we'll now create new fully signed updates for
			// the affected channels and also update the underlying
			// graph with the new state.
			newChanUpdates, err := d.processChanPolicyUpdate(
				ctx, policyUpdate.edgesToUpdate,
			)
			policyUpdate.errChan <- err
			if err != nil {
				log.Errorf("Unable to craft policy updates: %v",
					err)
				continue
			}

			// Finally, with the updates committed, we'll now add
			// them to the announcement batch to be flushed at the
			// start of the next epoch.
			announcements.AddMsgs(newChanUpdates...)

		case announcement := <-d.networkMsgs:
			log.Tracef("Received network message: "+
				"peer=%v, msg=%s, is_remote=%v",
				announcement.peer, announcement.msg.MsgType(),
				announcement.isRemote)

			switch announcement.msg.(type) {
			// Channel announcement signatures are amongst the only
			// messages that we'll process serially.
			case *lnwire.AnnounceSignatures1:
				emittedAnnouncements, _ := d.processNetworkAnnouncement(
					ctx, announcement,
				)
				log.Debugf("Processed network message %s, "+
					"returned len(announcements)=%v",
					announcement.msg.MsgType(),
					len(emittedAnnouncements))

				if emittedAnnouncements != nil {
					announcements.AddMsgs(
						emittedAnnouncements...,
					)
				}
				continue
			}

			// If this message was recently rejected, then we won't
			// attempt to re-process it.
			if announcement.isRemote && d.isRecentlyRejectedMsg(
				announcement.msg,
				sourceToPub(announcement.source),
			) {

				announcement.err <- fmt.Errorf("recently " +
					"rejected")
				continue
			}

			// We'll set up any dependent, and wait until a free
			// slot for this job opens up, this allow us to not
			// have thousands of goroutines active.
			annJobID, err := d.vb.InitJobDependencies(
				announcement.msg,
			)
			if err != nil {
				announcement.err <- err
				continue
			}

			d.wg.Add(1)
			go d.handleNetworkMessages(
				ctx, announcement, &announcements, annJobID,
			)

		// The trickle timer has ticked, which indicates we should
		// flush to the network the pending batch of new announcements
		// we've received since the last trickle tick.
		case <-trickleTimer.C:
			// Emit the current batch of announcements from
			// deDupedAnnouncements.
			announcementBatch := announcements.Emit()

			// If the current announcements batch is nil, then we
			// have no further work here.
			if announcementBatch.isEmpty() {
				continue
			}

			// At this point, we have the set of local and remote
			// announcements we want to send out. We'll do the
			// batching as normal for both, but for local
			// announcements, we'll blast them out w/o regard for
			// our peer's policies so we ensure they propagate
			// properly.
			d.splitAndSendAnnBatch(ctx, announcementBatch)

		// The retransmission timer has ticked which indicates that we
		// should check if we need to prune or re-broadcast any of our
		// personal channels or node announcement. This addresses the
		// case of "zombie" channels and channel advertisements that
		// have been dropped, or not properly propagated through the
		// network.
		case tick := <-d.cfg.RetransmitTicker.Ticks():
			if err := d.retransmitStaleAnns(ctx, tick); err != nil {
				log.Errorf("unable to rebroadcast stale "+
					"announcements: %v", err)
			}

		// The gossiper has been signalled to exit, to we exit our
		// main loop so the wait group can be decremented.
		case <-d.quit:
			return
		}
	}
}

// handleNetworkMessages is responsible for waiting for dependencies for a
// given network message and processing the message. Once processed, it will
// signal its dependants and add the new announcements to the announce batch.
//
// NOTE: must be run as a goroutine.
func (d *AuthenticatedGossiper) handleNetworkMessages(ctx context.Context,
	nMsg *networkMsg, deDuped *deDupedAnnouncements, jobID JobID) {

	defer d.wg.Done()
	defer d.vb.CompleteJob()

	// We should only broadcast this message forward if it originated from
	// us or it wasn't received as part of our initial historical sync.
	shouldBroadcast := !nMsg.isRemote || d.syncMgr.IsGraphSynced()

	// If this message has an existing dependency, then we'll wait until
	// that has been fully validated before we proceed.
	err := d.vb.WaitForParents(jobID, nMsg.msg)
	if err != nil {
		log.Debugf("Validating network message %s got err: %v",
			nMsg.msg.MsgType(), err)

		if errors.Is(err, ErrVBarrierShuttingDown) {
			log.Warnf("unexpected error during validation "+
				"barrier shutdown: %v", err)
		}
		nMsg.err <- err

		return
	}

	// Process the network announcement to determine if this is either a
	// new announcement from our PoV or an edges to a prior vertex/edge we
	// previously proceeded.
	newAnns, allow := d.processNetworkAnnouncement(ctx, nMsg)

	log.Tracef("Processed network message %s, returned "+
		"len(announcements)=%v, allowDependents=%v",
		nMsg.msg.MsgType(), len(newAnns), allow)

	// If this message had any dependencies, then we can now signal them to
	// continue.
	err = d.vb.SignalDependents(nMsg.msg, jobID)
	if err != nil {
		// Something is wrong if SignalDependents returns an error.
		log.Errorf("SignalDependents returned error for msg=%v with "+
			"JobID=%v", spew.Sdump(nMsg.msg), jobID)

		nMsg.err <- err

		return
	}

	// If the announcement was accepted, then add the emitted announcements
	// to our announce batch to be broadcast once the trickle timer ticks
	// gain.
	if newAnns != nil && shouldBroadcast {
		// TODO(roasbeef): exclude peer that sent.
		deDuped.AddMsgs(newAnns...)
	} else if newAnns != nil {
		log.Trace("Skipping broadcast of announcements received " +
			"during initial graph sync")
	}
}

// TODO(roasbeef): d/c peers that send updates not on our chain

// InitSyncState is called by outside sub-systems when a connection is
// established to a new peer that understands how to perform channel range
// queries. We'll allocate a new gossip syncer for it, and start any goroutines
// needed to handle new queries.
func (d *AuthenticatedGossiper) InitSyncState(syncPeer lnpeer.Peer) {
	d.syncMgr.InitSyncState(syncPeer)
}

// PruneSyncState is called by outside sub-systems once a peer that we were
// previously connected to has been disconnected. In this case we can stop the
// existing GossipSyncer assigned to the peer and free up resources.
func (d *AuthenticatedGossiper) PruneSyncState(peer route.Vertex) {
	d.syncMgr.PruneSyncState(peer)
}

// isRecentlyRejectedMsg returns true if we recently rejected a message, and
// false otherwise, This avoids expensive reprocessing of the message.
func (d *AuthenticatedGossiper) isRecentlyRejectedMsg(msg lnwire.Message,
	peerPub [33]byte) bool {

	var scid uint64
	switch m := msg.(type) {
	case *lnwire.ChannelUpdate1:
		scid = m.ShortChannelID.ToUint64()

	case *lnwire.ChannelAnnouncement1:
		scid = m.ShortChannelID.ToUint64()

	default:
		return false
	}

	_, err := d.recentRejects.Get(newRejectCacheKey(scid, peerPub))
	return err != cache.ErrElementNotFound
}

// retransmitStaleAnns examines all outgoing channels that the source node is
// known to maintain to check to see if any of them are "stale". A channel is
// stale iff, the last timestamp of its rebroadcast is older than the
// RebroadcastInterval. We also check if a refreshed node announcement should
// be resent.
func (d *AuthenticatedGossiper) retransmitStaleAnns(ctx context.Context,
	now time.Time) error {

	// Iterate over all of our channels and check if any of them fall
	// within the prune interval or re-broadcast interval.
	type updateTuple struct {
		info *models.ChannelEdgeInfo
		edge *models.ChannelEdgePolicy
	}

	var (
		havePublicChannels bool
		edgesToUpdate      []updateTuple
	)
	err := d.cfg.Graph.ForAllOutgoingChannels(ctx, func(
		info *models.ChannelEdgeInfo,
		edge *models.ChannelEdgePolicy) error {

		// If there's no auth proof attached to this edge, it means
		// that it is a private channel not meant to be announced to
		// the greater network, so avoid sending channel updates for
		// this channel to not leak its
		// existence.
		if info.AuthProof == nil {
			log.Debugf("Skipping retransmission of channel "+
				"without AuthProof: %v", info.ChannelID)
			return nil
		}

		// We make a note that we have at least one public channel. We
		// use this to determine whether we should send a node
		// announcement below.
		havePublicChannels = true

		// If this edge has a ChannelUpdate that was created before the
		// introduction of the MaxHTLC field, then we'll update this
		// edge to propagate this information in the network.
		if !edge.MessageFlags.HasMaxHtlc() {
			// We'll make sure we support the new max_htlc field if
			// not already present.
			edge.MessageFlags |= lnwire.ChanUpdateRequiredMaxHtlc
			edge.MaxHTLC = lnwire.NewMSatFromSatoshis(info.Capacity)

			edgesToUpdate = append(edgesToUpdate, updateTuple{
				info: info,
				edge: edge,
			})
			return nil
		}

		timeElapsed := now.Sub(edge.LastUpdate)

		// If it's been longer than RebroadcastInterval since we've
		// re-broadcasted the channel, add the channel to the set of
		// edges we need to update.
		if timeElapsed >= d.cfg.RebroadcastInterval {
			edgesToUpdate = append(edgesToUpdate, updateTuple{
				info: info,
				edge: edge,
			})
		}

		return nil
	}, func() {
		havePublicChannels = false
		edgesToUpdate = nil
	})
	if err != nil && !errors.Is(err, graphdb.ErrGraphNoEdgesFound) {
		return fmt.Errorf("unable to retrieve outgoing channels: %w",
			err)
	}

	var signedUpdates []lnwire.Message
	for _, chanToUpdate := range edgesToUpdate {
		// Re-sign and update the channel on disk and retrieve our
		// ChannelUpdate to broadcast.
		chanAnn, chanUpdate, err := d.updateChannel(
			ctx, chanToUpdate.info, chanToUpdate.edge,
		)
		if err != nil {
			return fmt.Errorf("unable to update channel: %w", err)
		}

		// If we have a valid announcement to transmit, then we'll send
		// that along with the update.
		if chanAnn != nil {
			signedUpdates = append(signedUpdates, chanAnn)
		}

		signedUpdates = append(signedUpdates, chanUpdate)
	}

	// If we don't have any public channels, we return as we don't want to
	// broadcast anything that would reveal our existence.
	if !havePublicChannels {
		return nil
	}

	// We'll also check that our NodeAnnouncement is not too old.
	currentNodeAnn := d.cfg.FetchSelfAnnouncement()
	timestamp := time.Unix(int64(currentNodeAnn.Timestamp), 0)
	timeElapsed := now.Sub(timestamp)

	// If it's been a full day since we've re-broadcasted the
	// node announcement, refresh it and resend it.
	nodeAnnStr := ""
	if timeElapsed >= d.cfg.RebroadcastInterval {
		newNodeAnn, err := d.cfg.UpdateSelfAnnouncement()
		if err != nil {
			return fmt.Errorf("unable to get refreshed node "+
				"announcement: %v", err)
		}

		signedUpdates = append(signedUpdates, &newNodeAnn)
		nodeAnnStr = " and our refreshed node announcement"

		// Before broadcasting the refreshed node announcement, add it
		// to our own graph.
		if err := d.addNode(ctx, &newNodeAnn); err != nil {
			log.Errorf("Unable to add refreshed node announcement "+
				"to graph: %v", err)
		}
	}

	// If we don't have any updates to re-broadcast, then we'll exit
	// early.
	if len(signedUpdates) == 0 {
		return nil
	}

	log.Infof("Retransmitting %v outgoing channels%v",
		len(edgesToUpdate), nodeAnnStr)

	// With all the wire announcements properly crafted, we'll broadcast
	// our known outgoing channels to all our immediate peers.
	if err := d.cfg.Broadcast(nil, signedUpdates...); err != nil {
		return fmt.Errorf("unable to re-broadcast channels: %w", err)
	}

	return nil
}

// processChanPolicyUpdate generates a new set of channel updates for the
// provided list of edges and updates the backing ChannelGraphSource.
func (d *AuthenticatedGossiper) processChanPolicyUpdate(ctx context.Context,
	edgesToUpdate []EdgeWithInfo) ([]networkMsg, error) {

	var chanUpdates []networkMsg
	for _, edgeInfo := range edgesToUpdate {
		// Now that we've collected all the channels we need to update,
		// we'll re-sign and update the backing ChannelGraphSource, and
		// retrieve our ChannelUpdate to broadcast.
		_, chanUpdate, err := d.updateChannel(
			ctx, edgeInfo.Info, edgeInfo.Edge,
		)
		if err != nil {
			return nil, err
		}

		// We'll avoid broadcasting any updates for private channels to
		// avoid directly giving away their existence. Instead, we'll
		// send the update directly to the remote party.
		if edgeInfo.Info.AuthProof == nil {
			// If AuthProof is nil and an alias was found for this
			// ChannelID (meaning the option-scid-alias feature was
			// negotiated), we'll replace the ShortChannelID in the
			// update with the peer's alias. We do this after
			// updateChannel so that the alias isn't persisted to
			// the database.
			chanID := lnwire.NewChanIDFromOutPoint(
				edgeInfo.Info.ChannelPoint,
			)

			var defaultAlias lnwire.ShortChannelID
			foundAlias, _ := d.cfg.GetAlias(chanID)
			if foundAlias != defaultAlias {
				chanUpdate.ShortChannelID = foundAlias

				sig, err := d.cfg.SignAliasUpdate(chanUpdate)
				if err != nil {
					log.Errorf("Unable to sign alias "+
						"update: %v", err)
					continue
				}

				lnSig, err := lnwire.NewSigFromSignature(sig)
				if err != nil {
					log.Errorf("Unable to create sig: %v",
						err)
					continue
				}

				chanUpdate.Signature = lnSig
			}

			remotePubKey := remotePubFromChanInfo(
				edgeInfo.Info, chanUpdate.ChannelFlags,
			)
			err := d.reliableSender.sendMessage(
				ctx, chanUpdate, remotePubKey,
			)
			if err != nil {
				log.Errorf("Unable to reliably send %v for "+
					"channel=%v to peer=%x: %v",
					chanUpdate.MsgType(),
					chanUpdate.ShortChannelID,
					remotePubKey, err)
			}
			continue
		}

		// We set ourselves as the source of this message to indicate
		// that we shouldn't skip any peers when sending this message.
		chanUpdates = append(chanUpdates, networkMsg{
			source:   d.selfKey,
			isRemote: false,
			msg:      chanUpdate,
		})
	}

	return chanUpdates, nil
}

// remotePubFromChanInfo returns the public key of the remote peer given a
// ChannelEdgeInfo that describe a channel we have with them.
func remotePubFromChanInfo(chanInfo *models.ChannelEdgeInfo,
	chanFlags lnwire.ChanUpdateChanFlags) [33]byte {

	var remotePubKey [33]byte
	switch {
	case chanFlags&lnwire.ChanUpdateDirection == 0:
		remotePubKey = chanInfo.NodeKey2Bytes
	case chanFlags&lnwire.ChanUpdateDirection == 1:
		remotePubKey = chanInfo.NodeKey1Bytes
	}

	return remotePubKey
}

// processRejectedEdge examines a rejected edge to see if we can extract any
// new announcements from it.  An edge will get rejected if we already added
// the same edge without AuthProof to the graph. If the received announcement
// contains a proof, we can add this proof to our edge.  We can end up in this
// situation in the case where we create a channel, but for some reason fail
// to receive the remote peer's proof, while the remote peer is able to fully
// assemble the proof and craft the ChannelAnnouncement.
func (d *AuthenticatedGossiper) processRejectedEdge(_ context.Context,
	chanAnnMsg *lnwire.ChannelAnnouncement1,
	proof *models.ChannelAuthProof) ([]networkMsg, error) {

	// First, we'll fetch the state of the channel as we know if from the
	// database.
	chanInfo, e1, e2, err := d.cfg.Graph.GetChannelByID(
		chanAnnMsg.ShortChannelID,
	)
	if err != nil {
		return nil, err
	}

	// The edge is in the graph, and has a proof attached, then we'll just
	// reject it as normal.
	if chanInfo.AuthProof != nil {
		return nil, nil
	}

	// Otherwise, this means that the edge is within the graph, but it
	// doesn't yet have a proper proof attached. If we did not receive
	// the proof such that we now can add it, there's nothing more we
	// can do.
	if proof == nil {
		return nil, nil
	}

	// We'll then create then validate the new fully assembled
	// announcement.
	chanAnn, e1Ann, e2Ann, err := netann.CreateChanAnnouncement(
		proof, chanInfo, e1, e2,
	)
	if err != nil {
		return nil, err
	}
	err = netann.ValidateChannelAnn(chanAnn, d.fetchPKScript)
	if err != nil {
		err := fmt.Errorf("assembled channel announcement proof "+
			"for shortChanID=%v isn't valid: %v",
			chanAnnMsg.ShortChannelID, err)
		log.Error(err)
		return nil, err
	}

	// If everything checks out, then we'll add the fully assembled proof
	// to the database.
	err = d.cfg.Graph.AddProof(chanAnnMsg.ShortChannelID, proof)
	if err != nil {
		err := fmt.Errorf("unable add proof to shortChanID=%v: %w",
			chanAnnMsg.ShortChannelID, err)
		log.Error(err)
		return nil, err
	}

	// As we now have a complete channel announcement for this channel,
	// we'll construct the announcement so they can be broadcast out to all
	// our peers.
	announcements := make([]networkMsg, 0, 3)
	announcements = append(announcements, networkMsg{
		source: d.selfKey,
		msg:    chanAnn,
	})
	if e1Ann != nil {
		announcements = append(announcements, networkMsg{
			source: d.selfKey,
			msg:    e1Ann,
		})
	}
	if e2Ann != nil {
		announcements = append(announcements, networkMsg{
			source: d.selfKey,
			msg:    e2Ann,
		})

	}

	return announcements, nil
}

// fetchPKScript fetches the output script for the given SCID.
func (d *AuthenticatedGossiper) fetchPKScript(chanID *lnwire.ShortChannelID) (
	[]byte, error) {

	return lnwallet.FetchPKScriptWithQuit(d.cfg.ChainIO, chanID, d.quit)
}

// addNode processes the given node announcement, and adds it to our channel
// graph.
func (d *AuthenticatedGossiper) addNode(ctx context.Context,
	msg *lnwire.NodeAnnouncement, op ...batch.SchedulerOption) error {

	if err := netann.ValidateNodeAnn(msg); err != nil {
		return fmt.Errorf("unable to validate node announcement: %w",
			err)
	}

	return d.cfg.Graph.AddNode(
		ctx, models.NodeFromWireAnnouncement(msg), op...,
	)
}

// isPremature decides whether a given network message has a block height+delta
// value specified in the future. If so, the message will be added to the
// future message map and be processed when the block height as reached.
//
// NOTE: must be used inside a lock.
func (d *AuthenticatedGossiper) isPremature(chanID lnwire.ShortChannelID,
	delta uint32, msg *networkMsg) bool {

	// The channel is already confirmed at chanID.BlockHeight so we minus
	// one block. For instance, if the required confirmation for this
	// channel announcement is 6, we then only need to wait for 5 more
	// blocks once the funding tx is confirmed.
	if delta > 0 {
		delta--
	}

	msgHeight := chanID.BlockHeight + delta

	// The message height is smaller or equal to our best known height,
	// thus the message is mature.
	if msgHeight <= d.bestHeight {
		return false
	}

	// Add the premature message to our future messages which will be
	// resent once the block height has reached.
	//
	// Copy the networkMsgs since the old message's err chan will be
	// consumed.
	copied := &networkMsg{
		peer:              msg.peer,
		source:            msg.source,
		msg:               msg.msg,
		optionalMsgFields: msg.optionalMsgFields,
		isRemote:          msg.isRemote,
		err:               make(chan error, 1),
	}

	// Create the cached message.
	cachedMsg := &cachedFutureMsg{
		msg:    copied,
		height: msgHeight,
	}

	// Increment the msg ID and add it to the cache.
	nextMsgID := d.futureMsgs.nextMsgID()
	_, err := d.futureMsgs.Put(nextMsgID, cachedMsg)
	if err != nil {
		log.Errorf("Adding future message got error: %v", err)
	}

	log.Debugf("Network message: %v added to future messages for "+
		"msgHeight=%d, bestHeight=%d", msg.msg.MsgType(),
		msgHeight, d.bestHeight)

	return true
}

// processNetworkAnnouncement processes a new network relate authenticated
// channel or node announcement or announcements proofs. If the announcement
// didn't affect the internal state due to either being out of date, invalid,
// or redundant, then nil is returned. Otherwise, the set of announcements will
// be returned which should be broadcasted to the rest of the network. The
// boolean returned indicates whether any dependents of the announcement should
// attempt to be processed as well.
func (d *AuthenticatedGossiper) processNetworkAnnouncement(ctx context.Context,
	nMsg *networkMsg) ([]networkMsg, bool) {

	// If this is a remote update, we set the scheduler option to lazily
	// add it to the graph.
	var schedulerOp []batch.SchedulerOption
	if nMsg.isRemote {
		schedulerOp = append(schedulerOp, batch.LazyAdd())
	}

	switch msg := nMsg.msg.(type) {
	// A new node announcement has arrived which either presents new
	// information about a node in one of the channels we know about, or a
	// updating previously advertised information.
	case *lnwire.NodeAnnouncement:
		return d.handleNodeAnnouncement(ctx, nMsg, msg, schedulerOp)

	// A new channel announcement has arrived, this indicates the
	// *creation* of a new channel within the network. This only advertises
	// the existence of a channel and not yet the routing policies in
	// either direction of the channel.
	case *lnwire.ChannelAnnouncement1:
		return d.handleChanAnnouncement(ctx, nMsg, msg, schedulerOp...)

	// A new authenticated channel edge update has arrived. This indicates
	// that the directional information for an already known channel has
	// been updated.
	case *lnwire.ChannelUpdate1:
		return d.handleChanUpdate(ctx, nMsg, msg, schedulerOp)

	// A new signature announcement has been received. This indicates
	// willingness of nodes involved in the funding of a channel to
	// announce this new channel to the rest of the world.
	case *lnwire.AnnounceSignatures1:
		return d.handleAnnSig(ctx, nMsg, msg)

	default:
		err := errors.New("wrong type of the announcement")
		nMsg.err <- err
		return nil, false
	}
}

// processZombieUpdate determines whether the provided channel update should
// resurrect a given zombie edge.
//
// NOTE: only the NodeKey1Bytes and NodeKey2Bytes members of the ChannelEdgeInfo
// should be inspected.
func (d *AuthenticatedGossiper) processZombieUpdate(_ context.Context,
	chanInfo *models.ChannelEdgeInfo, scid lnwire.ShortChannelID,
	msg *lnwire.ChannelUpdate1) error {

	// The least-significant bit in the flag on the channel update tells us
	// which edge is being updated.
	isNode1 := msg.ChannelFlags&lnwire.ChanUpdateDirection == 0

	// Since we've deemed the update as not stale above, before marking it
	// live, we'll make sure it has been signed by the correct party. If we
	// have both pubkeys, either party can resurrect the channel. If we've
	// already marked this with the stricter, single-sided resurrection we
	// will only have the pubkey of the node with the oldest timestamp.
	var pubKey *btcec.PublicKey
	switch {
	case isNode1 && chanInfo.NodeKey1Bytes != emptyPubkey:
		pubKey, _ = chanInfo.NodeKey1()
	case !isNode1 && chanInfo.NodeKey2Bytes != emptyPubkey:
		pubKey, _ = chanInfo.NodeKey2()
	}
	if pubKey == nil {
		return fmt.Errorf("incorrect pubkey to resurrect zombie "+
			"with chan_id=%v", msg.ShortChannelID)
	}

	err := netann.VerifyChannelUpdateSignature(msg, pubKey)
	if err != nil {
		return fmt.Errorf("unable to verify channel "+
			"update signature: %v", err)
	}

	// With the signature valid, we'll proceed to mark the
	// edge as live and wait for the channel announcement to
	// come through again.
	err = d.cfg.Graph.MarkEdgeLive(scid)
	switch {
	case errors.Is(err, graphdb.ErrZombieEdgeNotFound):
		log.Errorf("edge with chan_id=%v was not found in the "+
			"zombie index: %v", err)

		return nil

	case err != nil:
		return fmt.Errorf("unable to remove edge with "+
			"chan_id=%v from zombie index: %v",
			msg.ShortChannelID, err)

	default:
	}

	log.Debugf("Removed edge with chan_id=%v from zombie "+
		"index", msg.ShortChannelID)

	return nil
}

// fetchNodeAnn fetches the latest signed node announcement from our point of
// view for the node with the given public key.
func (d *AuthenticatedGossiper) fetchNodeAnn(ctx context.Context,
	pubKey [33]byte) (*lnwire.NodeAnnouncement, error) {

	node, err := d.cfg.Graph.FetchLightningNode(ctx, pubKey)
	if err != nil {
		return nil, err
	}

	return node.NodeAnnouncement(true)
}

// isMsgStale determines whether a message retrieved from the backing
// MessageStore is seen as stale by the current graph.
func (d *AuthenticatedGossiper) isMsgStale(_ context.Context,
	msg lnwire.Message) bool {

	switch msg := msg.(type) {
	case *lnwire.AnnounceSignatures1:
		chanInfo, _, _, err := d.cfg.Graph.GetChannelByID(
			msg.ShortChannelID,
		)

		// If the channel cannot be found, it is most likely a leftover
		// message for a channel that was closed, so we can consider it
		// stale.
		if errors.Is(err, graphdb.ErrEdgeNotFound) {
			return true
		}
		if err != nil {
			log.Debugf("Unable to retrieve channel=%v from graph: "+
				"%v", msg.ShortChannelID, err)
			return false
		}

		// If the proof exists in the graph, then we have successfully
		// received the remote proof and assembled the full proof, so we
		// can safely delete the local proof from the database.
		return chanInfo.AuthProof != nil

	case *lnwire.ChannelUpdate1:
		_, p1, p2, err := d.cfg.Graph.GetChannelByID(msg.ShortChannelID)

		// If the channel cannot be found, it is most likely a leftover
		// message for a channel that was closed, so we can consider it
		// stale.
		if errors.Is(err, graphdb.ErrEdgeNotFound) {
			return true
		}
		if err != nil {
			log.Debugf("Unable to retrieve channel=%v from graph: "+
				"%v", msg.ShortChannelID, err)
			return false
		}

		// Otherwise, we'll retrieve the correct policy that we
		// currently have stored within our graph to check if this
		// message is stale by comparing its timestamp.
		var p *models.ChannelEdgePolicy
		if msg.ChannelFlags&lnwire.ChanUpdateDirection == 0 {
			p = p1
		} else {
			p = p2
		}

		// If the policy is still unknown, then we can consider this
		// policy fresh.
		if p == nil {
			return false
		}

		timestamp := time.Unix(int64(msg.Timestamp), 0)
		return p.LastUpdate.After(timestamp)

	default:
		// We'll make sure to not mark any unsupported messages as stale
		// to ensure they are not removed.
		return false
	}
}

// updateChannel creates a new fully signed update for the channel, and updates
// the underlying graph with the new state.
func (d *AuthenticatedGossiper) updateChannel(ctx context.Context,
	info *models.ChannelEdgeInfo,
	edge *models.ChannelEdgePolicy) (*lnwire.ChannelAnnouncement1,
	*lnwire.ChannelUpdate1, error) {

	// Parse the unsigned edge into a channel update.
	chanUpdate := netann.UnsignedChannelUpdateFromEdge(info, edge)

	// We'll generate a new signature over a digest of the channel
	// announcement itself and update the timestamp to ensure it propagate.
	err := netann.SignChannelUpdate(
		d.cfg.AnnSigner, d.selfKeyLoc, chanUpdate,
		netann.ChanUpdSetTimestamp,
	)
	if err != nil {
		return nil, nil, err
	}

	// Next, we'll set the new signature in place, and update the reference
	// in the backing slice.
	edge.LastUpdate = time.Unix(int64(chanUpdate.Timestamp), 0)
	edge.SigBytes = chanUpdate.Signature.ToSignatureBytes()

	// To ensure that our signature is valid, we'll verify it ourself
	// before committing it to the slice returned.
	err = netann.ValidateChannelUpdateAnn(
		d.selfKey, info.Capacity, chanUpdate,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("generated invalid channel "+
			"update sig: %v", err)
	}

	// Finally, we'll write the new edge policy to disk.
	if err := d.cfg.Graph.UpdateEdge(ctx, edge); err != nil {
		return nil, nil, err
	}

	// We'll also create the original channel announcement so the two can
	// be broadcast along side each other (if necessary), but only if we
	// have a full channel announcement for this channel.
	var chanAnn *lnwire.ChannelAnnouncement1
	if info.AuthProof != nil {
		chanID := lnwire.NewShortChanIDFromInt(info.ChannelID)
		chanAnn = &lnwire.ChannelAnnouncement1{
			ShortChannelID:  chanID,
			NodeID1:         info.NodeKey1Bytes,
			NodeID2:         info.NodeKey2Bytes,
			ChainHash:       info.ChainHash,
			BitcoinKey1:     info.BitcoinKey1Bytes,
			Features:        lnwire.NewRawFeatureVector(),
			BitcoinKey2:     info.BitcoinKey2Bytes,
			ExtraOpaqueData: info.ExtraOpaqueData,
		}
		chanAnn.NodeSig1, err = lnwire.NewSigFromECDSARawSignature(
			info.AuthProof.NodeSig1Bytes,
		)
		if err != nil {
			return nil, nil, err
		}
		chanAnn.NodeSig2, err = lnwire.NewSigFromECDSARawSignature(
			info.AuthProof.NodeSig2Bytes,
		)
		if err != nil {
			return nil, nil, err
		}
		chanAnn.BitcoinSig1, err = lnwire.NewSigFromECDSARawSignature(
			info.AuthProof.BitcoinSig1Bytes,
		)
		if err != nil {
			return nil, nil, err
		}
		chanAnn.BitcoinSig2, err = lnwire.NewSigFromECDSARawSignature(
			info.AuthProof.BitcoinSig2Bytes,
		)
		if err != nil {
			return nil, nil, err
		}
	}

	return chanAnn, chanUpdate, err
}

// SyncManager returns the gossiper's SyncManager instance.
func (d *AuthenticatedGossiper) SyncManager() *SyncManager {
	return d.syncMgr
}

// IsKeepAliveUpdate determines whether this channel update is considered a
// keep-alive update based on the previous channel update processed for the same
// direction.
func IsKeepAliveUpdate(update *lnwire.ChannelUpdate1,
	prev *models.ChannelEdgePolicy) bool {

	// Both updates should be from the same direction.
	if update.ChannelFlags&lnwire.ChanUpdateDirection !=
		prev.ChannelFlags&lnwire.ChanUpdateDirection {

		return false
	}

	// The timestamp should always increase for a keep-alive update.
	timestamp := time.Unix(int64(update.Timestamp), 0)
	if !timestamp.After(prev.LastUpdate) {
		return false
	}

	// None of the remaining fields should change for a keep-alive update.
	if update.ChannelFlags.IsDisabled() != prev.ChannelFlags.IsDisabled() {
		return false
	}
	if lnwire.MilliSatoshi(update.BaseFee) != prev.FeeBaseMSat {
		return false
	}
	if lnwire.MilliSatoshi(update.FeeRate) != prev.FeeProportionalMillionths {
		return false
	}
	if update.TimeLockDelta != prev.TimeLockDelta {
		return false
	}
	if update.HtlcMinimumMsat != prev.MinHTLC {
		return false
	}
	if update.MessageFlags.HasMaxHtlc() && !prev.MessageFlags.HasMaxHtlc() {
		return false
	}
	if update.HtlcMaximumMsat != prev.MaxHTLC {
		return false
	}
	if !bytes.Equal(update.ExtraOpaqueData, prev.ExtraOpaqueData) {
		return false
	}
	return true
}

// latestHeight returns the gossiper's latest height known of the chain.
func (d *AuthenticatedGossiper) latestHeight() uint32 {
	d.Lock()
	defer d.Unlock()
	return d.bestHeight
}

// handleNodeAnnouncement processes a new node announcement.
func (d *AuthenticatedGossiper) handleNodeAnnouncement(ctx context.Context,
	nMsg *networkMsg, nodeAnn *lnwire.NodeAnnouncement,
	ops []batch.SchedulerOption) ([]networkMsg, bool) {

	timestamp := time.Unix(int64(nodeAnn.Timestamp), 0)

	log.Debugf("Processing NodeAnnouncement: peer=%v, timestamp=%v, "+
		"node=%x, source=%x", nMsg.peer, timestamp, nodeAnn.NodeID,
		nMsg.source.SerializeCompressed())

	// We'll quickly ask the router if it already has a newer update for
	// this node so we can skip validating signatures if not required.
	if d.cfg.Graph.IsStaleNode(ctx, nodeAnn.NodeID, timestamp) {
		log.Debugf("Skipped processing stale node: %x", nodeAnn.NodeID)
		nMsg.err <- nil
		return nil, true
	}

	if err := d.addNode(ctx, nodeAnn, ops...); err != nil {
		log.Debugf("Adding node: %x got error: %v", nodeAnn.NodeID,
			err)

		if !graph.IsError(
			err,
			graph.ErrOutdated,
			graph.ErrIgnored,
		) {

			log.Error(err)
		}

		nMsg.err <- err
		return nil, false
	}

	// In order to ensure we don't leak unadvertised nodes, we'll make a
	// quick check to ensure this node intends to publicly advertise itself
	// to the network.
	isPublic, err := d.cfg.Graph.IsPublicNode(nodeAnn.NodeID)
	if err != nil {
		log.Errorf("Unable to determine if node %x is advertised: %v",
			nodeAnn.NodeID, err)
		nMsg.err <- err
		return nil, false
	}

	var announcements []networkMsg

	// If it does, we'll add their announcement to our batch so that it can
	// be broadcast to the rest of our peers.
	if isPublic {
		announcements = append(announcements, networkMsg{
			peer:     nMsg.peer,
			isRemote: nMsg.isRemote,
			source:   nMsg.source,
			msg:      nodeAnn,
		})
	} else {
		log.Tracef("Skipping broadcasting node announcement for %x "+
			"due to being unadvertised", nodeAnn.NodeID)
	}

	nMsg.err <- nil
	// TODO(roasbeef): get rid of the above

	log.Debugf("Processed NodeAnnouncement: peer=%v, timestamp=%v, "+
		"node=%x, source=%x", nMsg.peer, timestamp, nodeAnn.NodeID,
		nMsg.source.SerializeCompressed())

	return announcements, true
}

// handleChanAnnouncement processes a new channel announcement.
//
//nolint:funlen
func (d *AuthenticatedGossiper) handleChanAnnouncement(ctx context.Context,
	nMsg *networkMsg, ann *lnwire.ChannelAnnouncement1,
	ops ...batch.SchedulerOption) ([]networkMsg, bool) {

	scid := ann.ShortChannelID

	log.Debugf("Processing ChannelAnnouncement1: peer=%v, short_chan_id=%v",
		nMsg.peer, scid.ToUint64())

	// We'll ignore any channel announcements that target any chain other
	// than the set of chains we know of.
	if !bytes.Equal(ann.ChainHash[:], d.cfg.ChainHash[:]) {
		err := fmt.Errorf("ignoring ChannelAnnouncement1 from chain=%v"+
			", gossiper on chain=%v", ann.ChainHash,
			d.cfg.ChainHash)
		log.Errorf(err.Error())

		key := newRejectCacheKey(
			scid.ToUint64(),
			sourceToPub(nMsg.source),
		)
		_, _ = d.recentRejects.Put(key, &cachedReject{})

		nMsg.err <- err
		return nil, false
	}

	// If this is a remote ChannelAnnouncement with an alias SCID, we'll
	// reject the announcement. Since the router accepts alias SCIDs,
	// not erroring out would be a DoS vector.
	if nMsg.isRemote && d.cfg.IsAlias(scid) {
		err := fmt.Errorf("ignoring remote alias channel=%v", scid)
		log.Errorf(err.Error())

		key := newRejectCacheKey(
			scid.ToUint64(),
			sourceToPub(nMsg.source),
		)
		_, _ = d.recentRejects.Put(key, &cachedReject{})

		nMsg.err <- err
		return nil, false
	}

	// If the advertised inclusionary block is beyond our knowledge of the
	// chain tip, then we'll ignore it for now.
	d.Lock()
	if nMsg.isRemote && d.isPremature(scid, 0, nMsg) {
		log.Warnf("Announcement for chan_id=(%v), is premature: "+
			"advertises height %v, only height %v is known",
			scid.ToUint64(), scid.BlockHeight, d.bestHeight)
		d.Unlock()
		nMsg.err <- nil
		return nil, false
	}
	d.Unlock()

	// At this point, we'll now ask the router if this is a zombie/known
	// edge. If so we can skip all the processing below.
	if d.cfg.Graph.IsKnownEdge(scid) {
		nMsg.err <- nil
		return nil, true
	}

	// Check if the channel is already closed in which case we can ignore
	// it.
	closed, err := d.cfg.ScidCloser.IsClosedScid(scid)
	if err != nil {
		log.Errorf("failed to check if scid %v is closed: %v", scid,
			err)
		nMsg.err <- err

		return nil, false
	}

	if closed {
		err = fmt.Errorf("ignoring closed channel %v", scid)
		log.Error(err)

		// If this is an announcement from us, we'll just ignore it.
		if !nMsg.isRemote {
			nMsg.err <- err
			return nil, false
		}

		// Increment the peer's ban score if they are sending closed
		// channel announcements.
		d.banman.incrementBanScore(nMsg.peer.PubKey())

		// If the peer is banned and not a channel peer, we'll
		// disconnect them.
		shouldDc, dcErr := d.ShouldDisconnect(nMsg.peer.IdentityKey())
		if dcErr != nil {
			log.Errorf("failed to check if we should disconnect "+
				"peer: %v", dcErr)
			nMsg.err <- dcErr

			return nil, false
		}

		if shouldDc {
			nMsg.peer.Disconnect(ErrPeerBanned)
		}

		nMsg.err <- err

		return nil, false
	}

	// If this is a remote channel announcement, then we'll validate all
	// the signatures within the proof as it should be well formed.
	var proof *models.ChannelAuthProof
	if nMsg.isRemote {
		err := netann.ValidateChannelAnn(ann, d.fetchPKScript)
		if err != nil {
			err := fmt.Errorf("unable to validate announcement: "+
				"%v", err)

			key := newRejectCacheKey(
				scid.ToUint64(),
				sourceToPub(nMsg.source),
			)
			_, _ = d.recentRejects.Put(key, &cachedReject{})

			log.Error(err)
			nMsg.err <- err
			return nil, false
		}

		// If the proof checks out, then we'll save the proof itself to
		// the database so we can fetch it later when gossiping with
		// other nodes.
		proof = &models.ChannelAuthProof{
			NodeSig1Bytes:    ann.NodeSig1.ToSignatureBytes(),
			NodeSig2Bytes:    ann.NodeSig2.ToSignatureBytes(),
			BitcoinSig1Bytes: ann.BitcoinSig1.ToSignatureBytes(),
			BitcoinSig2Bytes: ann.BitcoinSig2.ToSignatureBytes(),
		}
	}

	// With the proof validated (if necessary), we can now store it within
	// the database for our path finding and syncing needs.
	edge := &models.ChannelEdgeInfo{
		ChannelID:        scid.ToUint64(),
		ChainHash:        ann.ChainHash,
		NodeKey1Bytes:    ann.NodeID1,
		NodeKey2Bytes:    ann.NodeID2,
		BitcoinKey1Bytes: ann.BitcoinKey1,
		BitcoinKey2Bytes: ann.BitcoinKey2,
		AuthProof:        proof,
		Features: lnwire.NewFeatureVector(
			ann.Features, lnwire.Features,
		),
		ExtraOpaqueData: ann.ExtraOpaqueData,
	}

	// If there were any optional message fields provided, we'll include
	// them in its serialized disk representation now.
	var tapscriptRoot fn.Option[chainhash.Hash]
	if nMsg.optionalMsgFields != nil {
		if nMsg.optionalMsgFields.capacity != nil {
			edge.Capacity = *nMsg.optionalMsgFields.capacity
		}
		if nMsg.optionalMsgFields.channelPoint != nil {
			cp := *nMsg.optionalMsgFields.channelPoint
			edge.ChannelPoint = cp
		}

		// Optional tapscript root for custom channels.
		tapscriptRoot = nMsg.optionalMsgFields.tapscriptRoot
	}

	// Before we start validation or add the edge to the database, we obtain
	// the mutex for this channel ID. We do this to ensure no other
	// goroutine has read the database and is now making decisions based on
	// this DB state, before it writes to the DB. It also ensures that we
	// don't perform the expensive validation check on the same channel
	// announcement at the same time.
	d.channelMtx.Lock(scid.ToUint64())

	// If AssumeChannelValid is present, then we are unable to perform any
	// of the expensive checks below, so we'll short-circuit our path
	// straight to adding the edge to our graph. If the passed
	// ShortChannelID is an alias, then we'll skip validation as it will
	// not map to a legitimate tx. This is not a DoS vector as only we can
	// add an alias ChannelAnnouncement from the gossiper.
	if !(d.cfg.AssumeChannelValid || d.cfg.IsAlias(scid)) { //nolint:nestif
		op, capacity, script, err := d.validateFundingTransaction(
			ctx, ann, tapscriptRoot,
		)
		if err != nil {
			defer d.channelMtx.Unlock(scid.ToUint64())

			switch {
			case errors.Is(err, ErrNoFundingTransaction),
				errors.Is(err, ErrInvalidFundingOutput):

				key := newRejectCacheKey(
					scid.ToUint64(),
					sourceToPub(nMsg.source),
				)
				_, _ = d.recentRejects.Put(
					key, &cachedReject{},
				)

				// Increment the peer's ban score. We check
				// isRemote so we don't actually ban the peer in
				// case of a local bug.
				if nMsg.isRemote {
					d.banman.incrementBanScore(
						nMsg.peer.PubKey(),
					)
				}

			case errors.Is(err, ErrChannelSpent):
				key := newRejectCacheKey(
					scid.ToUint64(),
					sourceToPub(nMsg.source),
				)
				_, _ = d.recentRejects.Put(key, &cachedReject{})

				// Since this channel has already been closed,
				// we'll add it to the graph's closed channel
				// index such that we won't attempt to do
				// expensive validation checks on it again.
				// TODO: Populate the ScidCloser by using closed
				// channel notifications.
				dbErr := d.cfg.ScidCloser.PutClosedScid(scid)
				if dbErr != nil {
					log.Errorf("failed to mark scid(%v) "+
						"as closed: %v", scid, dbErr)

					nMsg.err <- dbErr

					return nil, false
				}

				// Increment the peer's ban score. We check
				// isRemote so we don't accidentally ban
				// ourselves in case of a bug.
				if nMsg.isRemote {
					d.banman.incrementBanScore(
						nMsg.peer.PubKey(),
					)
				}

			default:
				// Otherwise, this is just a regular rejected
				// edge.
				key := newRejectCacheKey(
					scid.ToUint64(),
					sourceToPub(nMsg.source),
				)
				_, _ = d.recentRejects.Put(key, &cachedReject{})
			}

			if !nMsg.isRemote {
				log.Errorf("failed to add edge for local "+
					"channel: %v", err)
				nMsg.err <- err

				return nil, false
			}

			shouldDc, dcErr := d.ShouldDisconnect(
				nMsg.peer.IdentityKey(),
			)
			if dcErr != nil {
				log.Errorf("failed to check if we should "+
					"disconnect peer: %v", dcErr)
				nMsg.err <- dcErr

				return nil, false
			}

			if shouldDc {
				nMsg.peer.Disconnect(ErrPeerBanned)
			}

			nMsg.err <- err

			return nil, false
		}

		edge.FundingScript = fn.Some(script)

		// TODO(roasbeef): this is a hack, needs to be removed after
		//  commitment fees are dynamic.
		edge.Capacity = capacity
		edge.ChannelPoint = op
	}

	log.Debugf("Adding edge for short_chan_id: %v", scid.ToUint64())

	// We will add the edge to the channel router. If the nodes present in
	// this channel are not present in the database, a partial node will be
	// added to represent each node while we wait for a node announcement.
	err = d.cfg.Graph.AddEdge(ctx, edge, ops...)
	if err != nil {
		log.Debugf("Graph rejected edge for short_chan_id(%v): %v",
			scid.ToUint64(), err)

		defer d.channelMtx.Unlock(scid.ToUint64())

		// If the edge was rejected due to already being known, then it
		// may be the case that this new message has a fresh channel
		// proof, so we'll check.
		if graph.IsError(err, graph.ErrIgnored) {
			// Attempt to process the rejected message to see if we
			// get any new announcements.
			anns, rErr := d.processRejectedEdge(ctx, ann, proof)
			if rErr != nil {
				key := newRejectCacheKey(
					scid.ToUint64(),
					sourceToPub(nMsg.source),
				)
				cr := &cachedReject{}
				_, _ = d.recentRejects.Put(key, cr)

				nMsg.err <- rErr

				return nil, false
			}

			log.Debugf("Extracted %v announcements from rejected "+
				"msgs", len(anns))

			// If while processing this rejected edge, we realized
			// there's a set of announcements we could extract,
			// then we'll return those directly.
			//
			// NOTE: since this is an ErrIgnored, we can return
			// true here to signal "allow" to its dependants.
			nMsg.err <- nil

			return anns, true
		}

		// Otherwise, this is just a regular rejected edge.
		key := newRejectCacheKey(
			scid.ToUint64(),
			sourceToPub(nMsg.source),
		)
		_, _ = d.recentRejects.Put(key, &cachedReject{})

		if !nMsg.isRemote {
			log.Errorf("failed to add edge for local channel: %v",
				err)
			nMsg.err <- err

			return nil, false
		}

		shouldDc, dcErr := d.ShouldDisconnect(nMsg.peer.IdentityKey())
		if dcErr != nil {
			log.Errorf("failed to check if we should disconnect "+
				"peer: %v", dcErr)
			nMsg.err <- dcErr

			return nil, false
		}

		if shouldDc {
			nMsg.peer.Disconnect(ErrPeerBanned)
		}

		nMsg.err <- err

		return nil, false
	}

	// If err is nil, release the lock immediately.
	d.channelMtx.Unlock(scid.ToUint64())

	log.Debugf("Finish adding edge for short_chan_id: %v", scid.ToUint64())

	// If we earlier received any ChannelUpdates for this channel, we can
	// now process them, as the channel is added to the graph.
	var channelUpdates []*processedNetworkMsg

	earlyChanUpdates, err := d.prematureChannelUpdates.Get(scid.ToUint64())
	if err == nil {
		// There was actually an entry in the map, so we'll accumulate
		// it. We don't worry about deletion, since it'll eventually
		// fall out anyway.
		chanMsgs := earlyChanUpdates
		channelUpdates = append(channelUpdates, chanMsgs.msgs...)
	}

	// Launch a new goroutine to handle each ChannelUpdate, this is to
	// ensure we don't block here, as we can handle only one announcement
	// at a time.
	for _, cu := range channelUpdates {
		// Skip if already processed.
		if cu.processed {
			continue
		}

		// Mark the ChannelUpdate as processed. This ensures that a
		// subsequent announcement in the option-scid-alias case does
		// not re-use an old ChannelUpdate.
		cu.processed = true

		d.wg.Add(1)
		go func(updMsg *networkMsg) {
			defer d.wg.Done()

			switch msg := updMsg.msg.(type) {
			// Reprocess the message, making sure we return an
			// error to the original caller in case the gossiper
			// shuts down.
			case *lnwire.ChannelUpdate1:
				log.Debugf("Reprocessing ChannelUpdate for "+
					"shortChanID=%v", scid.ToUint64())

				select {
				case d.networkMsgs <- updMsg:
				case <-d.quit:
					updMsg.err <- ErrGossiperShuttingDown
				}

			// We don't expect any other message type than
			// ChannelUpdate to be in this cache.
			default:
				log.Errorf("Unsupported message type found "+
					"among ChannelUpdates: %T", msg)
			}
		}(cu.msg)
	}

	// Channel announcement was successfully processed and now it might be
	// broadcast to other connected nodes if it was an announcement with
	// proof (remote).
	var announcements []networkMsg

	if proof != nil {
		announcements = append(announcements, networkMsg{
			peer:     nMsg.peer,
			isRemote: nMsg.isRemote,
			source:   nMsg.source,
			msg:      ann,
		})
	}

	nMsg.err <- nil

	log.Debugf("Processed ChannelAnnouncement1: peer=%v, short_chan_id=%v",
		nMsg.peer, scid.ToUint64())

	return announcements, true
}

// handleChanUpdate processes a new channel update.
//
//nolint:funlen
func (d *AuthenticatedGossiper) handleChanUpdate(ctx context.Context,
	nMsg *networkMsg, upd *lnwire.ChannelUpdate1,
	ops []batch.SchedulerOption) ([]networkMsg, bool) {

	log.Debugf("Processing ChannelUpdate: peer=%v, short_chan_id=%v, ",
		nMsg.peer, upd.ShortChannelID.ToUint64())

	// We'll ignore any channel updates that target any chain other than
	// the set of chains we know of.
	if !bytes.Equal(upd.ChainHash[:], d.cfg.ChainHash[:]) {
		err := fmt.Errorf("ignoring ChannelUpdate from chain=%v, "+
			"gossiper on chain=%v", upd.ChainHash, d.cfg.ChainHash)
		log.Errorf(err.Error())

		key := newRejectCacheKey(
			upd.ShortChannelID.ToUint64(),
			sourceToPub(nMsg.source),
		)
		_, _ = d.recentRejects.Put(key, &cachedReject{})

		nMsg.err <- err
		return nil, false
	}

	blockHeight := upd.ShortChannelID.BlockHeight
	shortChanID := upd.ShortChannelID.ToUint64()

	// If the advertised inclusionary block is beyond our knowledge of the
	// chain tip, then we'll put the announcement in limbo to be fully
	// verified once we advance forward in the chain. If the update has an
	// alias SCID, we'll skip the isPremature check. This is necessary
	// since aliases start at block height 16_000_000.
	d.Lock()
	if nMsg.isRemote && !d.cfg.IsAlias(upd.ShortChannelID) &&
		d.isPremature(upd.ShortChannelID, 0, nMsg) {

		log.Warnf("Update announcement for short_chan_id(%v), is "+
			"premature: advertises height %v, only height %v is "+
			"known", shortChanID, blockHeight, d.bestHeight)
		d.Unlock()
		nMsg.err <- nil
		return nil, false
	}
	d.Unlock()

	// Before we perform any of the expensive checks below, we'll check
	// whether this update is stale or is for a zombie channel in order to
	// quickly reject it.
	timestamp := time.Unix(int64(upd.Timestamp), 0)

	// Fetch the SCID we should be using to lock the channelMtx and make
	// graph queries with.
	graphScid, err := d.cfg.FindBaseByAlias(upd.ShortChannelID)
	if err != nil {
		// Fallback and set the graphScid to the peer-provided SCID.
		// This will occur for non-option-scid-alias channels and for
		// public option-scid-alias channels after 6 confirmations.
		// Once public option-scid-alias channels have 6 confs, we'll
		// ignore ChannelUpdates with one of their aliases.
		graphScid = upd.ShortChannelID
	}

	// We make sure to obtain the mutex for this channel ID before we access
	// the database. This ensures the state we read from the database has
	// not changed between this point and when we call UpdateEdge() later.
	d.channelMtx.Lock(graphScid.ToUint64())
	defer d.channelMtx.Unlock(graphScid.ToUint64())

	if d.cfg.Graph.IsStaleEdgePolicy(
		graphScid, timestamp, upd.ChannelFlags,
	) {

		log.Debugf("Ignored stale edge policy for short_chan_id(%v): "+
			"peer=%v, msg=%s, is_remote=%v", shortChanID,
			nMsg.peer, nMsg.msg.MsgType(), nMsg.isRemote,
		)

		nMsg.err <- nil
		return nil, true
	}

	// Check that the ChanUpdate is not too far into the future, this could
	// reveal some faulty implementation therefore we log an error.
	if time.Until(timestamp) > graph.DefaultChannelPruneExpiry {
		log.Errorf("Skewed timestamp (%v) for edge policy of "+
			"short_chan_id(%v), timestamp too far in the future: "+
			"peer=%v, msg=%s, is_remote=%v", timestamp.Unix(),
			shortChanID, nMsg.peer, nMsg.msg.MsgType(),
			nMsg.isRemote,
		)

		nMsg.err <- fmt.Errorf("skewed timestamp of edge policy, "+
			"timestamp too far in the future: %v", timestamp.Unix())

		return nil, false
	}

	// Get the node pub key as far since we don't have it in the channel
	// update announcement message. We'll need this to properly verify the
	// message's signature.
	chanInfo, e1, e2, err := d.cfg.Graph.GetChannelByID(graphScid)
	switch {
	// No error, break.
	case err == nil:
		break

	case errors.Is(err, graphdb.ErrZombieEdge):
		err = d.processZombieUpdate(ctx, chanInfo, graphScid, upd)
		if err != nil {
			log.Debug(err)
			nMsg.err <- err
			return nil, false
		}

		// We'll fallthrough to ensure we stash the update until we
		// receive its corresponding ChannelAnnouncement. This is
		// needed to ensure the edge exists in the graph before
		// applying the update.
		fallthrough
	case errors.Is(err, graphdb.ErrGraphNotFound):
		fallthrough
	case errors.Is(err, graphdb.ErrGraphNoEdgesFound):
		fallthrough
	case errors.Is(err, graphdb.ErrEdgeNotFound):
		// If the edge corresponding to this ChannelUpdate was not
		// found in the graph, this might be a channel in the process
		// of being opened, and we haven't processed our own
		// ChannelAnnouncement yet, hence it is not not found in the
		// graph. This usually gets resolved after the channel proofs
		// are exchanged and the channel is broadcasted to the rest of
		// the network, but in case this is a private channel this
		// won't ever happen. This can also happen in the case of a
		// zombie channel with a fresh update for which we don't have a
		// ChannelAnnouncement for since we reject them. Because of
		// this, we temporarily add it to a map, and reprocess it after
		// our own ChannelAnnouncement has been processed.
		//
		// The shortChanID may be an alias, but it is fine to use here
		// since we don't have an edge in the graph and if the peer is
		// not buggy, we should be able to use it once the gossiper
		// receives the local announcement.
		pMsg := &processedNetworkMsg{msg: nMsg}

		earlyMsgs, err := d.prematureChannelUpdates.Get(shortChanID)
		switch {
		// Nothing in the cache yet, we can just directly insert this
		// element.
		case err == cache.ErrElementNotFound:
			_, _ = d.prematureChannelUpdates.Put(
				shortChanID, &cachedNetworkMsg{
					msgs: []*processedNetworkMsg{pMsg},
				})

		// There's already something in the cache, so we'll combine the
		// set of messages into a single value.
		default:
			msgs := earlyMsgs.msgs
			msgs = append(msgs, pMsg)
			_, _ = d.prematureChannelUpdates.Put(
				shortChanID, &cachedNetworkMsg{
					msgs: msgs,
				})
		}

		log.Debugf("Got ChannelUpdate for edge not found in graph"+
			"(shortChanID=%v), saving for reprocessing later",
			shortChanID)

		// NOTE: We don't return anything on the error channel for this
		// message, as we expect that will be done when this
		// ChannelUpdate is later reprocessed. This might never happen
		// if the corresponding ChannelAnnouncement is never received
		// or the LRU cache is filled up and the entry is evicted.
		return nil, false

	default:
		err := fmt.Errorf("unable to validate channel update "+
			"short_chan_id=%v: %v", shortChanID, err)
		log.Error(err)
		nMsg.err <- err

		key := newRejectCacheKey(
			upd.ShortChannelID.ToUint64(),
			sourceToPub(nMsg.source),
		)
		_, _ = d.recentRejects.Put(key, &cachedReject{})

		return nil, false
	}

	// The least-significant bit in the flag on the channel update
	// announcement tells us "which" side of the channels directed edge is
	// being updated.
	var (
		pubKey       *btcec.PublicKey
		edgeToUpdate *models.ChannelEdgePolicy
	)
	direction := upd.ChannelFlags & lnwire.ChanUpdateDirection
	switch direction {
	case 0:
		pubKey, _ = chanInfo.NodeKey1()
		edgeToUpdate = e1
	case 1:
		pubKey, _ = chanInfo.NodeKey2()
		edgeToUpdate = e2
	}

	log.Debugf("Validating ChannelUpdate: channel=%v, for node=%x, has "+
		"edge policy=%v", chanInfo.ChannelID,
		pubKey.SerializeCompressed(), edgeToUpdate != nil)

	// Validate the channel announcement with the expected public key and
	// channel capacity. In the case of an invalid channel update, we'll
	// return an error to the caller and exit early.
	err = netann.ValidateChannelUpdateAnn(pubKey, chanInfo.Capacity, upd)
	if err != nil {
		rErr := fmt.Errorf("unable to validate channel update "+
			"announcement for short_chan_id=%v: %v",
			spew.Sdump(upd.ShortChannelID), err)

		log.Error(rErr)
		nMsg.err <- rErr
		return nil, false
	}

	// If we have a previous version of the edge being updated, we'll want
	// to rate limit its updates to prevent spam throughout the network.
	if nMsg.isRemote && edgeToUpdate != nil {
		// If it's a keep-alive update, we'll only propagate one if
		// it's been a day since the previous. This follows our own
		// heuristic of sending keep-alive updates after the same
		// duration (see retransmitStaleAnns).
		timeSinceLastUpdate := timestamp.Sub(edgeToUpdate.LastUpdate)
		if IsKeepAliveUpdate(upd, edgeToUpdate) {
			if timeSinceLastUpdate < d.cfg.RebroadcastInterval {
				log.Debugf("Ignoring keep alive update not "+
					"within %v period for channel %v",
					d.cfg.RebroadcastInterval, shortChanID)
				nMsg.err <- nil
				return nil, false
			}
		} else {
			// If it's not, we'll allow an update per minute with a
			// maximum burst of 10. If we haven't seen an update
			// for this channel before, we'll need to initialize a
			// rate limiter for each direction.
			//
			// Since the edge exists in the graph, we'll create a
			// rate limiter for chanInfo.ChannelID rather then the
			// SCID the peer sent. This is because there may be
			// multiple aliases for a channel and we may otherwise
			// rate-limit only a single alias of the channel,
			// instead of the whole channel.
			baseScid := chanInfo.ChannelID
			d.Lock()
			rls, ok := d.chanUpdateRateLimiter[baseScid]
			if !ok {
				r := rate.Every(d.cfg.ChannelUpdateInterval)
				b := d.cfg.MaxChannelUpdateBurst
				rls = [2]*rate.Limiter{
					rate.NewLimiter(r, b),
					rate.NewLimiter(r, b),
				}
				d.chanUpdateRateLimiter[baseScid] = rls
			}
			d.Unlock()

			if !rls[direction].Allow() {
				log.Debugf("Rate limiting update for channel "+
					"%v from direction %x", shortChanID,
					pubKey.SerializeCompressed())
				nMsg.err <- nil
				return nil, false
			}
		}
	}

	// We'll use chanInfo.ChannelID rather than the peer-supplied
	// ShortChannelID in the ChannelUpdate to avoid the router having to
	// lookup the stored SCID. If we're sending the update, we'll always
	// use the SCID stored in the database rather than a potentially
	// different alias. This might mean that SigBytes is incorrect as it
	// signs a different SCID than the database SCID, but since there will
	// only be a difference if AuthProof == nil, this is fine.
	update := &models.ChannelEdgePolicy{
		SigBytes:                  upd.Signature.ToSignatureBytes(),
		ChannelID:                 chanInfo.ChannelID,
		LastUpdate:                timestamp,
		MessageFlags:              upd.MessageFlags,
		ChannelFlags:              upd.ChannelFlags,
		TimeLockDelta:             upd.TimeLockDelta,
		MinHTLC:                   upd.HtlcMinimumMsat,
		MaxHTLC:                   upd.HtlcMaximumMsat,
		FeeBaseMSat:               lnwire.MilliSatoshi(upd.BaseFee),
		FeeProportionalMillionths: lnwire.MilliSatoshi(upd.FeeRate),
		InboundFee:                upd.InboundFee.ValOpt(),
		ExtraOpaqueData:           upd.ExtraOpaqueData,
	}

	if err := d.cfg.Graph.UpdateEdge(ctx, update, ops...); err != nil {
		if graph.IsError(
			err, graph.ErrOutdated,
			graph.ErrIgnored,
		) {

			log.Debugf("Update edge for short_chan_id(%v) got: %v",
				shortChanID, err)
		} else {
			// Since we know the stored SCID in the graph, we'll
			// cache that SCID.
			key := newRejectCacheKey(
				chanInfo.ChannelID,
				sourceToPub(nMsg.source),
			)
			_, _ = d.recentRejects.Put(key, &cachedReject{})

			log.Errorf("Update edge for short_chan_id(%v) got: %v",
				shortChanID, err)
		}

		nMsg.err <- err
		return nil, false
	}

	// If this is a local ChannelUpdate without an AuthProof, it means it
	// is an update to a channel that is not (yet) supposed to be announced
	// to the greater network. However, our channel counter party will need
	// to be given the update, so we'll try sending the update directly to
	// the remote peer.
	if !nMsg.isRemote && chanInfo.AuthProof == nil {
		if nMsg.optionalMsgFields != nil {
			remoteAlias := nMsg.optionalMsgFields.remoteAlias
			if remoteAlias != nil {
				// The remoteAlias field was specified, meaning
				// that we should replace the SCID in the
				// update with the remote's alias. We'll also
				// need to re-sign the channel update. This is
				// required for option-scid-alias feature-bit
				// negotiated channels.
				upd.ShortChannelID = *remoteAlias

				sig, err := d.cfg.SignAliasUpdate(upd)
				if err != nil {
					log.Error(err)
					nMsg.err <- err
					return nil, false
				}

				lnSig, err := lnwire.NewSigFromSignature(sig)
				if err != nil {
					log.Error(err)
					nMsg.err <- err
					return nil, false
				}

				upd.Signature = lnSig
			}
		}

		// Get our peer's public key.
		remotePubKey := remotePubFromChanInfo(
			chanInfo, upd.ChannelFlags,
		)

		log.Debugf("The message %v has no AuthProof, sending the "+
			"update to remote peer %x", upd.MsgType(), remotePubKey)

		// Now we'll attempt to send the channel update message
		// reliably to the remote peer in the background, so that we
		// don't block if the peer happens to be offline at the moment.
		err := d.reliableSender.sendMessage(ctx, upd, remotePubKey)
		if err != nil {
			err := fmt.Errorf("unable to reliably send %v for "+
				"channel=%v to peer=%x: %v", upd.MsgType(),
				upd.ShortChannelID, remotePubKey, err)
			nMsg.err <- err
			return nil, false
		}
	}

	// Channel update announcement was successfully processed and now it
	// can be broadcast to the rest of the network. However, we'll only
	// broadcast the channel update announcement if it has an attached
	// authentication proof. We also won't broadcast the update if it
	// contains an alias because the network would reject this.
	var announcements []networkMsg
	if chanInfo.AuthProof != nil && !d.cfg.IsAlias(upd.ShortChannelID) {
		announcements = append(announcements, networkMsg{
			peer:     nMsg.peer,
			source:   nMsg.source,
			isRemote: nMsg.isRemote,
			msg:      upd,
		})
	}

	nMsg.err <- nil

	log.Debugf("Processed ChannelUpdate: peer=%v, short_chan_id=%v, "+
		"timestamp=%v", nMsg.peer, upd.ShortChannelID.ToUint64(),
		timestamp)
	return announcements, true
}

// handleAnnSig processes a new announcement signatures message.
//
//nolint:funlen
func (d *AuthenticatedGossiper) handleAnnSig(ctx context.Context,
	nMsg *networkMsg, ann *lnwire.AnnounceSignatures1) ([]networkMsg,
	bool) {

	needBlockHeight := ann.ShortChannelID.BlockHeight +
		d.cfg.ProofMatureDelta
	shortChanID := ann.ShortChannelID.ToUint64()

	prefix := "local"
	if nMsg.isRemote {
		prefix = "remote"
	}

	log.Infof("Received new %v announcement signature for %v", prefix,
		ann.ShortChannelID)

	// By the specification, channel announcement proofs should be sent
	// after some number of confirmations after channel was registered in
	// bitcoin blockchain. Therefore, we check if the proof is mature.
	d.Lock()
	premature := d.isPremature(
		ann.ShortChannelID, d.cfg.ProofMatureDelta, nMsg,
	)
	if premature {
		log.Warnf("Premature proof announcement, current block height"+
			"lower than needed: %v < %v", d.bestHeight,
			needBlockHeight)
		d.Unlock()
		nMsg.err <- nil
		return nil, false
	}
	d.Unlock()

	// Ensure that we know of a channel with the target channel ID before
	// proceeding further.
	//
	// We must acquire the mutex for this channel ID before getting the
	// channel from the database, to ensure what we read does not change
	// before we call AddProof() later.
	d.channelMtx.Lock(ann.ShortChannelID.ToUint64())
	defer d.channelMtx.Unlock(ann.ShortChannelID.ToUint64())

	chanInfo, e1, e2, err := d.cfg.Graph.GetChannelByID(
		ann.ShortChannelID,
	)
	if err != nil {
		_, err = d.cfg.FindChannel(nMsg.source, ann.ChannelID)
		if err != nil {
			err := fmt.Errorf("unable to store the proof for "+
				"short_chan_id=%v: %v", shortChanID, err)
			log.Error(err)
			nMsg.err <- err

			return nil, false
		}

		proof := channeldb.NewWaitingProof(nMsg.isRemote, ann)
		err := d.cfg.WaitingProofStore.Add(proof)
		if err != nil {
			err := fmt.Errorf("unable to store the proof for "+
				"short_chan_id=%v: %v", shortChanID, err)
			log.Error(err)
			nMsg.err <- err
			return nil, false
		}

		log.Infof("Orphan %v proof announcement with short_chan_id=%v"+
			", adding to waiting batch", prefix, shortChanID)
		nMsg.err <- nil
		return nil, false
	}

	nodeID := nMsg.source.SerializeCompressed()
	isFirstNode := bytes.Equal(nodeID, chanInfo.NodeKey1Bytes[:])
	isSecondNode := bytes.Equal(nodeID, chanInfo.NodeKey2Bytes[:])

	// Ensure that channel that was retrieved belongs to the peer which
	// sent the proof announcement.
	if !(isFirstNode || isSecondNode) {
		err := fmt.Errorf("channel that was received doesn't belong "+
			"to the peer which sent the proof, short_chan_id=%v",
			shortChanID)
		log.Error(err)
		nMsg.err <- err
		return nil, false
	}

	// If proof was sent by a local sub-system, then we'll send the
	// announcement signature to the remote node so they can also
	// reconstruct the full channel announcement.
	if !nMsg.isRemote {
		var remotePubKey [33]byte
		if isFirstNode {
			remotePubKey = chanInfo.NodeKey2Bytes
		} else {
			remotePubKey = chanInfo.NodeKey1Bytes
		}

		// Since the remote peer might not be online we'll call a
		// method that will attempt to deliver the proof when it comes
		// online.
		err := d.reliableSender.sendMessage(ctx, ann, remotePubKey)
		if err != nil {
			err := fmt.Errorf("unable to reliably send %v for "+
				"channel=%v to peer=%x: %v", ann.MsgType(),
				ann.ShortChannelID, remotePubKey, err)
			nMsg.err <- err
			return nil, false
		}
	}

	// Check if we already have the full proof for this channel.
	if chanInfo.AuthProof != nil {
		// If we already have the fully assembled proof, then the peer
		// sending us their proof has probably not received our local
		// proof yet. So be kind and send them the full proof.
		if nMsg.isRemote {
			peerID := nMsg.source.SerializeCompressed()
			log.Debugf("Got AnnounceSignatures for channel with " +
				"full proof.")

			d.wg.Add(1)
			go func() {
				defer d.wg.Done()

				log.Debugf("Received half proof for channel "+
					"%v with existing full proof. Sending"+
					" full proof to peer=%x",
					ann.ChannelID, peerID)

				ca, _, _, err := netann.CreateChanAnnouncement(
					chanInfo.AuthProof, chanInfo, e1, e2,
				)
				if err != nil {
					log.Errorf("unable to gen ann: %v",
						err)
					return
				}

				err = nMsg.peer.SendMessage(false, ca)
				if err != nil {
					log.Errorf("Failed sending full proof"+
						" to peer=%x: %v", peerID, err)
					return
				}

				log.Debugf("Full proof sent to peer=%x for "+
					"chanID=%v", peerID, ann.ChannelID)
			}()
		}

		log.Debugf("Already have proof for channel with chanID=%v",
			ann.ChannelID)
		nMsg.err <- nil
		return nil, true
	}

	// Check that we received the opposite proof. If so, then we're now
	// able to construct the full proof, and create the channel
	// announcement. If we didn't receive the opposite half of the proof
	// then we should store this one, and wait for the opposite to be
	// received.
	proof := channeldb.NewWaitingProof(nMsg.isRemote, ann)
	oppProof, err := d.cfg.WaitingProofStore.Get(proof.OppositeKey())
	if err != nil && err != channeldb.ErrWaitingProofNotFound {
		err := fmt.Errorf("unable to get the opposite proof for "+
			"short_chan_id=%v: %v", shortChanID, err)
		log.Error(err)
		nMsg.err <- err
		return nil, false
	}

	if err == channeldb.ErrWaitingProofNotFound {
		err := d.cfg.WaitingProofStore.Add(proof)
		if err != nil {
			err := fmt.Errorf("unable to store the proof for "+
				"short_chan_id=%v: %v", shortChanID, err)
			log.Error(err)
			nMsg.err <- err
			return nil, false
		}

		log.Infof("1/2 of channel ann proof received for "+
			"short_chan_id=%v, waiting for other half",
			shortChanID)

		nMsg.err <- nil
		return nil, false
	}

	// We now have both halves of the channel announcement proof, then
	// we'll reconstruct the initial announcement so we can validate it
	// shortly below.
	var dbProof models.ChannelAuthProof
	if isFirstNode {
		dbProof.NodeSig1Bytes = ann.NodeSignature.ToSignatureBytes()
		dbProof.NodeSig2Bytes = oppProof.NodeSignature.ToSignatureBytes()
		dbProof.BitcoinSig1Bytes = ann.BitcoinSignature.ToSignatureBytes()
		dbProof.BitcoinSig2Bytes = oppProof.BitcoinSignature.ToSignatureBytes()
	} else {
		dbProof.NodeSig1Bytes = oppProof.NodeSignature.ToSignatureBytes()
		dbProof.NodeSig2Bytes = ann.NodeSignature.ToSignatureBytes()
		dbProof.BitcoinSig1Bytes = oppProof.BitcoinSignature.ToSignatureBytes()
		dbProof.BitcoinSig2Bytes = ann.BitcoinSignature.ToSignatureBytes()
	}

	chanAnn, e1Ann, e2Ann, err := netann.CreateChanAnnouncement(
		&dbProof, chanInfo, e1, e2,
	)
	if err != nil {
		log.Error(err)
		nMsg.err <- err
		return nil, false
	}

	// With all the necessary components assembled validate the full
	// channel announcement proof.
	err = netann.ValidateChannelAnn(chanAnn, d.fetchPKScript)
	if err != nil {
		err := fmt.Errorf("channel announcement proof for "+
			"short_chan_id=%v isn't valid: %v", shortChanID, err)

		log.Error(err)
		nMsg.err <- err
		return nil, false
	}

	// If the channel was returned by the router it means that existence of
	// funding point and inclusion of nodes bitcoin keys in it already
	// checked by the router. In this stage we should check that node keys
	// attest to the bitcoin keys by validating the signatures of
	// announcement. If proof is valid then we'll populate the channel edge
	// with it, so we can announce it on peer connect.
	err = d.cfg.Graph.AddProof(ann.ShortChannelID, &dbProof)
	if err != nil {
		err := fmt.Errorf("unable add proof to the channel chanID=%v:"+
			" %v", ann.ChannelID, err)
		log.Error(err)
		nMsg.err <- err
		return nil, false
	}

	err = d.cfg.WaitingProofStore.Remove(proof.OppositeKey())
	if err != nil {
		err := fmt.Errorf("unable to remove opposite proof for the "+
			"channel with chanID=%v: %v", ann.ChannelID, err)
		log.Error(err)
		nMsg.err <- err
		return nil, false
	}

	// Proof was successfully created and now can announce the channel to
	// the remain network.
	log.Infof("Fully valid channel proof for short_chan_id=%v constructed"+
		", adding to next ann batch", shortChanID)

	// Assemble the necessary announcements to add to the next broadcasting
	// batch.
	var announcements []networkMsg
	announcements = append(announcements, networkMsg{
		peer:   nMsg.peer,
		source: nMsg.source,
		msg:    chanAnn,
	})
	if src, err := chanInfo.NodeKey1(); err == nil && e1Ann != nil {
		announcements = append(announcements, networkMsg{
			peer:   nMsg.peer,
			source: src,
			msg:    e1Ann,
		})
	}
	if src, err := chanInfo.NodeKey2(); err == nil && e2Ann != nil {
		announcements = append(announcements, networkMsg{
			peer:   nMsg.peer,
			source: src,
			msg:    e2Ann,
		})
	}

	// We'll also send along the node announcements for each channel
	// participant if we know of them. To ensure our node announcement
	// propagates to our channel counterparty, we'll set the source for
	// each announcement to the node it belongs to, otherwise we won't send
	// it since the source gets skipped. This isn't necessary for channel
	// updates and announcement signatures since we send those directly to
	// our channel counterparty through the gossiper's reliable sender.
	node1Ann, err := d.fetchNodeAnn(ctx, chanInfo.NodeKey1Bytes)
	if err != nil {
		log.Debugf("Unable to fetch node announcement for %x: %v",
			chanInfo.NodeKey1Bytes, err)
	} else {
		if nodeKey1, err := chanInfo.NodeKey1(); err == nil {
			announcements = append(announcements, networkMsg{
				peer:   nMsg.peer,
				source: nodeKey1,
				msg:    node1Ann,
			})
		}
	}

	node2Ann, err := d.fetchNodeAnn(ctx, chanInfo.NodeKey2Bytes)
	if err != nil {
		log.Debugf("Unable to fetch node announcement for %x: %v",
			chanInfo.NodeKey2Bytes, err)
	} else {
		if nodeKey2, err := chanInfo.NodeKey2(); err == nil {
			announcements = append(announcements, networkMsg{
				peer:   nMsg.peer,
				source: nodeKey2,
				msg:    node2Ann,
			})
		}
	}

	nMsg.err <- nil
	return announcements, true
}

// isBanned returns true if the peer identified by pubkey is banned for sending
// invalid channel announcements.
func (d *AuthenticatedGossiper) isBanned(pubkey [33]byte) bool {
	return d.banman.isBanned(pubkey)
}

// ShouldDisconnect returns true if we should disconnect the peer identified by
// pubkey.
func (d *AuthenticatedGossiper) ShouldDisconnect(pubkey *btcec.PublicKey) (
	bool, error) {

	pubkeySer := pubkey.SerializeCompressed()

	var pubkeyBytes [33]byte
	copy(pubkeyBytes[:], pubkeySer)

	// If the public key is banned, check whether or not this is a channel
	// peer.
	if d.isBanned(pubkeyBytes) {
		isChanPeer, err := d.cfg.ScidCloser.IsChannelPeer(pubkey)
		if err != nil {
			return false, err
		}

		// We should only disconnect non-channel peers.
		if !isChanPeer {
			return true, nil
		}
	}

	return false, nil
}

// validateFundingTransaction fetches the channel announcements claimed funding
// transaction from chain to ensure that it exists, is not spent and matches
// the channel announcement proof. The transaction's outpoint and value are
// returned if we can glean them from the work done in this method.
func (d *AuthenticatedGossiper) validateFundingTransaction(_ context.Context,
	ann *lnwire.ChannelAnnouncement1,
	tapscriptRoot fn.Option[chainhash.Hash]) (wire.OutPoint, btcutil.Amount,
	[]byte, error) {

	scid := ann.ShortChannelID

	// Before we can add the channel to the channel graph, we need to obtain
	// the full funding outpoint that's encoded within the channel ID.
	fundingTx, err := lnwallet.FetchFundingTxWrapper(
		d.cfg.ChainIO, &scid, d.quit,
	)
	if err != nil {
		//nolint:ll
		//
		// In order to ensure we don't erroneously mark a channel as a
		// zombie due to an RPC failure, we'll attempt to string match
		// for the relevant errors.
		//
		// * btcd:
		//    * https://github.com/btcsuite/btcd/blob/master/rpcserver.go#L1316
		//    * https://github.com/btcsuite/btcd/blob/master/rpcserver.go#L1086
		// * bitcoind:
		//    * https://github.com/bitcoin/bitcoin/blob/7fcf53f7b4524572d1d0c9a5fdc388e87eb02416/src/rpc/blockchain.cpp#L770
		//     * https://github.com/bitcoin/bitcoin/blob/7fcf53f7b4524572d1d0c9a5fdc388e87eb02416/src/rpc/blockchain.cpp#L954
		switch {
		case strings.Contains(err.Error(), "not found"):
			fallthrough

		case strings.Contains(err.Error(), "out of range"):
			// If the funding transaction isn't found at all, then
			// we'll mark the edge itself as a zombie so we don't
			// continue to request it. We use the "zero key" for
			// both node pubkeys so this edge can't be resurrected.
			zErr := d.cfg.Graph.MarkZombieEdge(scid.ToUint64())
			if zErr != nil {
				return wire.OutPoint{}, 0, nil, zErr
			}

		default:
		}

		return wire.OutPoint{}, 0, nil, fmt.Errorf("%w: %w",
			ErrNoFundingTransaction, err)
	}

	// Recreate witness output to be sure that declared in channel edge
	// bitcoin keys and channel value corresponds to the reality.
	fundingPkScript, err := makeFundingScript(
		ann.BitcoinKey1[:], ann.BitcoinKey2[:], ann.Features,
		tapscriptRoot,
	)
	if err != nil {
		return wire.OutPoint{}, 0, nil, err
	}

	// Next we'll validate that this channel is actually well formed. If
	// this check fails, then this channel either doesn't exist, or isn't
	// the one that was meant to be created according to the passed channel
	// proofs.
	fundingPoint, err := chanvalidate.Validate(
		&chanvalidate.Context{
			Locator: &chanvalidate.ShortChanIDChanLocator{
				ID: scid,
			},
			MultiSigPkScript: fundingPkScript,
			FundingTx:        fundingTx,
		},
	)
	if err != nil {
		// Mark the edge as a zombie so we won't try to re-validate it
		// on start up.
		zErr := d.cfg.Graph.MarkZombieEdge(scid.ToUint64())
		if zErr != nil {
			return wire.OutPoint{}, 0, nil, zErr
		}

		return wire.OutPoint{}, 0, nil, fmt.Errorf("%w: %w",
			ErrInvalidFundingOutput, err)
	}

	// Now that we have the funding outpoint of the channel, ensure
	// that it hasn't yet been spent. If so, then this channel has
	// been closed so we'll ignore it.
	chanUtxo, err := d.cfg.ChainIO.GetUtxo(
		fundingPoint, fundingPkScript, scid.BlockHeight, d.quit,
	)
	if err != nil {
		if errors.Is(err, btcwallet.ErrOutputSpent) {
			zErr := d.cfg.Graph.MarkZombieEdge(scid.ToUint64())
			if zErr != nil {
				return wire.OutPoint{}, 0, nil, zErr
			}
		}

		return wire.OutPoint{}, 0, nil, fmt.Errorf("%w: unable to "+
			"fetch utxo for chan_id=%v, chan_point=%v: %w",
			ErrChannelSpent, scid.ToUint64(), fundingPoint, err)
	}

	return *fundingPoint, btcutil.Amount(chanUtxo.Value), fundingPkScript,
		nil
}

// makeFundingScript is used to make the funding script for both segwit v0 and
// segwit v1 (taproot) channels.
func makeFundingScript(bitcoinKey1, bitcoinKey2 []byte,
	features *lnwire.RawFeatureVector,
	tapscriptRoot fn.Option[chainhash.Hash]) ([]byte, error) {

	legacyFundingScript := func() ([]byte, error) {
		witnessScript, err := input.GenMultiSigScript(
			bitcoinKey1, bitcoinKey2,
		)
		if err != nil {
			return nil, err
		}
		pkScript, err := input.WitnessScriptHash(witnessScript)
		if err != nil {
			return nil, err
		}

		return pkScript, nil
	}

	if features.IsEmpty() {
		return legacyFundingScript()
	}

	chanFeatureBits := lnwire.NewFeatureVector(features, lnwire.Features)
	if chanFeatureBits.HasFeature(
		lnwire.SimpleTaprootChannelsOptionalStaging,
	) {

		pubKey1, err := btcec.ParsePubKey(bitcoinKey1)
		if err != nil {
			return nil, err
		}
		pubKey2, err := btcec.ParsePubKey(bitcoinKey2)
		if err != nil {
			return nil, err
		}

		fundingScript, _, err := input.GenTaprootFundingScript(
			pubKey1, pubKey2, 0, tapscriptRoot,
		)
		if err != nil {
			return nil, err
		}

		// TODO(roasbeef): add tapscript root to gossip v1.5

		return fundingScript, nil
	}

	return legacyFundingScript()
}
