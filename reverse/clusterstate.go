package reverse

import (
	"context"
	"log/slog"
	"time"

	"github.com/lightwebinc/teranode-bridge/internal/obs"
	pb "github.com/lightwebinc/teranode-bridge/proto/blockchain_api"
)

// RunClusterState polls the cluster's FSM state and tip height over the
// blockchain connection the reverse path already holds, and publishes both as
// gauges.
//
// # Why the bridge reads state it does not act on
//
// The bridge announces and submits identically whether the cluster is RUNNING,
// IDLE or CATCHINGBLOCKS. That is deliberate — the cluster is unmodified and
// decides for itself what to do with an announcement — but it means the most
// common degraded case has no visible cause from the bridge's own metrics:
// announcements succeed, pulls never follow, and every bridge counter looks
// healthy. Publishing the state the cluster reports turns that from a mystery
// into a reading, and it is also what the health endpoint reports as a
// dependency, mirroring how Teranode's own services treat FSM state
// (teranode/services/blockchain/fsm.go, CheckFSM).
//
// Deliberately advisory: this never gates publishing. A bridge that stopped
// announcing while its cluster caught up would starve the very cluster it is
// meant to feed.
func (s *Subscriber) RunClusterState(ctx context.Context, every time.Duration, log *slog.Logger) {
	if every <= 0 {
		return
	}
	client := pb.NewBlockchainAPIClient(s.conn)
	t := time.NewTicker(every)
	defer t.Stop()

	s.pollClusterState(ctx, client, log)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.pollClusterState(ctx, client, log)
		}
	}
}

// pollClusterState reads both facts. Each is bounded and independent: a cluster
// that answers the FSM call but not the header call still publishes its state.
func (s *Subscriber) pollClusterState(ctx context.Context, client pb.BlockchainAPIClient, log *slog.Logger) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if resp, err := client.GetFSMCurrentState(cctx, &pb.Empty{}); err != nil {
		obs.ClusterProbeErrors.WithLabelValues("fsm").Inc()
		// A cluster that cannot answer is not the same as one in a known bad
		// state, so the gauge goes to the sentinel rather than to a real state.
		obs.SetFSMState("UNKNOWN", -1)
		log.Debug("cluster FSM state unavailable", "err", err)
	} else {
		st := resp.GetState()
		name := st.String()
		s.fsmState.Store(&name)
		obs.SetFSMState(name, float64(st))
	}

	if resp, err := client.GetBestBlockHeader(cctx, &pb.Empty{}); err != nil {
		obs.ClusterProbeErrors.WithLabelValues("best_block_header").Inc()
		log.Debug("cluster best block header unavailable", "err", err)
	} else {
		h := resp.GetHeight()
		s.height.Store(uint64(h))
		obs.ClusterBlockHeight.WithLabelValues().Set(float64(h))
	}
}

// ClusterState is the last successfully read cluster state, for the health
// endpoint and the stats line. state is empty when nothing has been read yet.
func (s *Subscriber) ClusterState() (state string, height uint64) {
	if p := s.fsmState.Load(); p != nil {
		state = *p
	}
	return state, s.height.Load()
}
