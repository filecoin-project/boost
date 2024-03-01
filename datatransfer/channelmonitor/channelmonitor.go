package channelmonitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bep/debounce"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/channels"
)

var log = logging.Logger("dt-chanmon")

type monitorAPI interface {
	SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe
	RestartDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error
	CloseDataTransferChannelWithError(ctx context.Context, chid datatransfer.ChannelID, cherr error) error
	ConnectTo(context.Context, peer.ID) error
	PeerID() peer.ID
}

// Monitor watches the events for data transfer channels, and restarts
// a channel if there are timeouts / errors
type Monitor struct {
	ctx  context.Context
	stop context.CancelFunc
	mgr  monitorAPI
	cfg  *Config

	lk       sync.RWMutex
	channels map[datatransfer.ChannelID]*monitoredChannel
}

type Config struct {
	// Max time to wait for other side to accept open channel request before attempting restart.
	// Set to 0 to disable timeout.
	AcceptTimeout time.Duration
	// Debounce when restart is triggered by multiple errors
	RestartDebounce time.Duration
	// Backoff after restarting
	RestartBackoff time.Duration
	// Number of times to try to restart before failing
	MaxConsecutiveRestarts uint32
	// Max time to wait for the responder to send a Complete message once all
	// data has been sent.
	// Set to 0 to disable timeout.
	CompleteTimeout time.Duration
	// Called when a restart completes successfully
	OnRestartComplete func(id datatransfer.ChannelID)
}

func NewMonitor(mgr monitorAPI, cfg *Config) *Monitor {
	checkConfig(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	return &Monitor{
		ctx:      ctx,
		stop:     cancel,
		mgr:      mgr,
		cfg:      cfg,
		channels: make(map[datatransfer.ChannelID]*monitoredChannel),
	}
}

func checkConfig(cfg *Config) {
	if cfg == nil {
		return
	}

	prefix := "data-transfer channel monitor config "
	if cfg.AcceptTimeout < 0 {
		panic(fmt.Sprintf(prefix+"AcceptTimeout is %s but must be >= 0", cfg.AcceptTimeout))
	}
	if cfg.MaxConsecutiveRestarts == 0 {
		panic(fmt.Sprintf(prefix+"MaxConsecutiveRestarts is %d but must be > 0", cfg.MaxConsecutiveRestarts))
	}
	if cfg.CompleteTimeout < 0 {
		panic(fmt.Sprintf(prefix+"CompleteTimeout is %s but must be >= 0", cfg.CompleteTimeout))
	}
}

// AddPushChannel adds a push channel to the channel monitor
func (m *Monitor) AddPushChannel(chid datatransfer.ChannelID) *monitoredChannel {
	return m.addChannel(chid, true)
}

// AddPullChannel adds a pull channel to the channel monitor
func (m *Monitor) AddPullChannel(chid datatransfer.ChannelID) *monitoredChannel {
	return m.addChannel(chid, false)
}

// addChannel adds a channel to the channel monitor
func (m *Monitor) addChannel(chid datatransfer.ChannelID, isPush bool) *monitoredChannel {
	if !m.enabled() {
		return nil
	}

	m.lk.Lock()
	defer m.lk.Unlock()

	// Check if there is already a monitor for this channel
	if _, ok := m.channels[chid]; ok {
		tp := "push"
		if !isPush {
			tp = "pull"
		}
		log.Warnf("ignoring add %s channel %s: %s channel with that id already exists",
			tp, chid, tp)
		return nil
	}

	mpc := newMonitoredChannel(m.ctx, m.mgr, chid, m.cfg, m.onMonitoredChannelShutdown)
	m.channels[chid] = mpc
	return mpc
}

func (m *Monitor) Shutdown() {
	// Cancel the context for the Monitor
	m.stop()
}

// onMonitoredChannelShutdown is called when a monitored channel shuts down
func (m *Monitor) onMonitoredChannelShutdown(chid datatransfer.ChannelID) {
	m.lk.Lock()
	defer m.lk.Unlock()

	delete(m.channels, chid)
}

// enabled indicates whether the channel monitor is running
func (m *Monitor) enabled() bool {
	return m.cfg != nil
}

// monitoredChannel keeps track of events for a channel, and
// restarts the channel if there are connection issues
type monitoredChannel struct {
	// The parentCtx is used when sending a close message for a channel, so
	// that operation can continue even after the monitoredChannel is shutdown
	parentCtx               context.Context
	ctx                     context.Context
	cancel                  context.CancelFunc
	mgr                     monitorAPI
	chid                    datatransfer.ChannelID
	cfg                     *Config
	unsub                   datatransfer.Unsubscribe
	restartChannelDebounced func(error)
	onShutdown              func(datatransfer.ChannelID)
	shutdownLk              sync.Mutex

	restartLk           sync.RWMutex
	restartedAt         time.Time
	restartQueued       bool
	consecutiveRestarts int
}

func newMonitoredChannel(
	parentCtx context.Context,
	mgr monitorAPI,
	chid datatransfer.ChannelID,
	cfg *Config,
	onShutdown func(datatransfer.ChannelID),
) *monitoredChannel {
	ctx, cancel := context.WithCancel(context.Background())
	mpc := &monitoredChannel{
		parentCtx:  parentCtx,
		ctx:        ctx,
		cancel:     cancel,
		mgr:        mgr,
		chid:       chid,
		cfg:        cfg,
		onShutdown: onShutdown,
	}

	// "debounce" calls to restart channel, ie if there are multiple calls in a
	// short space of time, only send a message to restart the channel once
	var lk sync.Mutex
	var lastErr error
	debouncer := debounce.New(cfg.RestartDebounce)
	mpc.restartChannelDebounced = func(err error) {
		// Log the error at debug level
		log.Debug(err.Error())

		// Save the last error passed to restartChannelDebounced
		lk.Lock()
		lastErr = err
		lk.Unlock()

		debouncer(func() {
			// Log only the last error passed to restartChannelDebounced at warning level
			lk.Lock()
			log.Warnf("%s", lastErr)
			lk.Unlock()

			// Restart the channel
			mpc.restartChannel()
		})
	}

	// Start monitoring the channel
	mpc.start()
	return mpc
}

// Cancel the context and unsubscribe from events.
// Returns true if channel has not already been shutdown.
func (mc *monitoredChannel) Shutdown() bool {
	mc.shutdownLk.Lock()
	defer mc.shutdownLk.Unlock()

	// Check if the channel was already shut down
	if mc.cancel == nil {
		return false
	}
	mc.cancel() // cancel context so all go-routines exit
	mc.cancel = nil

	// unsubscribe from data transfer events
	mc.unsub()

	// Inform the Manager that this channel has shut down
	go mc.onShutdown(mc.chid)

	return true
}

func (mc *monitoredChannel) start() {
	// Prevent shutdown until after startup
	mc.shutdownLk.Lock()
	defer mc.shutdownLk.Unlock()

	log.Debugf("%s: starting data-transfer channel monitoring", mc.chid)

	// Watch to make sure the responder accepts the channel in time
	cancelAcceptTimer := mc.watchForResponderAccept()

	// Watch for data-transfer channel events
	mc.unsub = mc.mgr.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if channelState.ChannelID() != mc.chid {
			return
		}

		// Once the channel completes, shut down the monitor
		state := channelState.Status()
		if channels.IsChannelCleaningUp(state) || channels.IsChannelTerminated(state) {
			log.Debugf("%s: stopping data-transfer channel monitoring (event: %s / state: %s)",
				mc.chid, datatransfer.Events[event.Code], datatransfer.Statuses[channelState.Status()])
			go mc.Shutdown()
			return
		}

		switch event.Code {
		case datatransfer.Accept:
			// The Accept event is fired when we receive an Accept message from the responder
			cancelAcceptTimer()
		case datatransfer.SendDataError:
			// If the transport layer reports an error sending data over the wire,
			// attempt to restart the channel
			err := xerrors.Errorf("%s: data transfer transport send error, restarting data transfer", mc.chid)
			go mc.restartChannelDebounced(err)
		case datatransfer.ReceiveDataError:
			// If the transport layer reports an error receiving data over the wire,
			// attempt to restart the channel
			err := xerrors.Errorf("%s: data transfer transport receive error, restarting data transfer", mc.chid)
			go mc.restartChannelDebounced(err)
		case datatransfer.FinishTransfer:
			// The channel initiator has finished sending / receiving all data.
			// Watch to make sure that the responder sends a message to acknowledge
			// that the transfer is complete
			go mc.watchForResponderComplete()
		case datatransfer.DataSent, datatransfer.DataReceived:
			// Some data was sent / received so reset the consecutive restart
			// counter
			mc.resetConsecutiveRestarts()
		}
	})
}

// watchForResponderAccept watches to make sure that the responder sends
// an Accept to our open channel request before the accept timeout.
// Returns a function that can be used to cancel the timer.
func (mc *monitoredChannel) watchForResponderAccept() func() {
	// Check if the accept timeout is disabled
	if mc.cfg.AcceptTimeout == 0 {
		return func() {}
	}

	// Start a timer for the accept timeout
	timer := time.NewTimer(mc.cfg.AcceptTimeout)

	go func() {
		defer timer.Stop()

		select {
		case <-mc.ctx.Done():
		case <-timer.C:
			// Timer expired before we received an Accept from the responder,
			// fail the data transfer
			err := xerrors.Errorf("%s: timed out waiting %s for Accept message from remote peer",
				mc.chid, mc.cfg.AcceptTimeout)
			mc.closeChannelAndShutdown(err)
		}
	}()

	return func() { timer.Stop() }
}

// Wait up to the configured timeout for the responder to send a Complete message
func (mc *monitoredChannel) watchForResponderComplete() {
	// Check if the complete timeout is disabled
	if mc.cfg.CompleteTimeout == 0 {
		return
	}

	// Start a timer for the complete timeout
	timer := time.NewTimer(mc.cfg.CompleteTimeout)
	defer timer.Stop()

	select {
	case <-mc.ctx.Done():
		// When the Complete message is received, the channel shuts down and
		// its context is cancelled
	case <-timer.C:
		// Timer expired before we received a Complete message from the responder
		err := xerrors.Errorf("%s: timed out waiting %s for Complete message from remote peer",
			mc.chid, mc.cfg.CompleteTimeout)
		mc.closeChannelAndShutdown(err)
	}
}

// clear the consecutive restart count (we do this when data is sent or
// received)
func (mc *monitoredChannel) resetConsecutiveRestarts() {
	mc.restartLk.Lock()
	defer mc.restartLk.Unlock()

	mc.consecutiveRestarts = 0
}

// Send a restart message for the channel
func (mc *monitoredChannel) restartChannel() {
	var restartedAt time.Time
	mc.restartLk.Lock()
	{
		restartedAt = mc.restartedAt
		if mc.restartedAt.IsZero() {
			// If there is not already a restart in progress, we'll restart now
			mc.restartedAt = time.Now()
		} else {
			// There is already a restart in progress, so queue up a restart
			// for after the current one has completed
			mc.restartQueued = true
		}
	}
	mc.restartLk.Unlock()

	// Check if channel is already being restarted
	if !restartedAt.IsZero() {
		log.Debugf("%s: restart called but already restarting channel, "+
			"waiting to restart again (since %s; restart backoff is %s)",
			mc.chid, time.Since(restartedAt), mc.cfg.RestartBackoff)
		return
	}

	for {
		// Send the restart message
		err := mc.doRestartChannel()
		if err != nil {
			// If there was an error restarting, close the channel and shutdown
			// the monitor
			mc.closeChannelAndShutdown(err)
			return
		}

		// Restart has completed, check if there's another restart queued up
		restartAgain := false
		mc.restartLk.Lock()
		{
			if mc.restartQueued {
				// There's another restart queued up, so reset the restart time
				// to now
				mc.restartedAt = time.Now()
				restartAgain = true
				mc.restartQueued = false
			} else {
				// No other restarts queued up, so clear the restart time
				mc.restartedAt = time.Time{}
			}
		}
		mc.restartLk.Unlock()

		if !restartAgain {
			// No restart queued, we're done
			if mc.cfg.OnRestartComplete != nil {
				mc.cfg.OnRestartComplete(mc.chid)
			}
			return
		}

		// There was a restart queued, restart again
		log.Debugf("%s: restart was queued - restarting again", mc.chid)
	}
}

func (mc *monitoredChannel) doRestartChannel() error {
	// Keep track of the number of consecutive restarts with no data
	// transferred
	mc.restartLk.Lock()
	mc.consecutiveRestarts++
	restartCount := mc.consecutiveRestarts
	mc.restartLk.Unlock()

	if uint32(restartCount) > mc.cfg.MaxConsecutiveRestarts {
		// If no data has been transferred since the last restart, and we've
		// reached the consecutive restart limit, return an error
		return xerrors.Errorf("%s: after %d consecutive restarts failed to transfer any data", mc.chid, restartCount)
	}

	// Send the restart message
	log.Debugf("%s: restarting (%d consecutive restarts)", mc.chid, restartCount)
	err := mc.sendRestartMessage(restartCount)
	if err != nil {
		log.Warnf("%s: restart failed, trying again: %s", mc.chid, err)
		// If the restart message could not be sent, try again
		return mc.doRestartChannel()
	}
	log.Infof("%s: restart completed successfully", mc.chid)

	return nil
}

func (mc *monitoredChannel) sendRestartMessage(restartCount int) error {
	// Establish a connection to the peer, in case the connection went down.
	// Note that at the networking layer there is logic to retry if a network
	// connection cannot be established, so this may take some time.
	p := mc.chid.OtherParty(mc.mgr.PeerID())
	log.Debugf("%s: re-establishing connection to %s", mc.chid, p)
	start := time.Now()
	err := mc.mgr.ConnectTo(mc.ctx, p)
	if err != nil {
		return xerrors.Errorf("%s: failed to reconnect to peer %s after %s: %w",
			mc.chid, p, time.Since(start), err)
	}
	log.Debugf("%s: re-established connection to %s in %s", mc.chid, p, time.Since(start))

	// Send a restart message for the channel
	log.Debugf("%s: sending restart message to %s (%d consecutive restarts)", mc.chid, p, restartCount)
	err = mc.mgr.RestartDataTransferChannel(mc.ctx, mc.chid)
	if err != nil {
		return xerrors.Errorf("%s: failed to send restart message to %s: %w", mc.chid, p, err)
	}

	// The restart message was sent successfully.
	// If a restart backoff is configured, backoff after a restart before
	// attempting another.
	if mc.cfg.RestartBackoff > 0 {
		log.Debugf("%s: backing off %s before allowing any other restarts",
			mc.chid, mc.cfg.RestartBackoff)
		select {
		case <-time.After(mc.cfg.RestartBackoff):
			log.Debugf("%s: restart back-off of %s complete", mc.chid, mc.cfg.RestartBackoff)
		case <-mc.ctx.Done():
			return nil
		}
	}

	return nil
}

// Shut down the monitor and close the data transfer channel
func (mc *monitoredChannel) closeChannelAndShutdown(cherr error) {
	// Shutdown the monitor
	firstShutdown := mc.Shutdown()
	if !firstShutdown {
		// Channel was already shutdown, ignore this second attempt to shutdown
		return
	}

	// Close the data transfer channel and fire an error
	log.Errorf("%s: closing data-transfer channel: %s", mc.chid, cherr)
	err := mc.mgr.CloseDataTransferChannelWithError(mc.parentCtx, mc.chid, cherr)
	if err != nil {
		log.Errorf("error closing data-transfer channel %s: %s", mc.chid, err)
	}
}
