# Upstream asks: catchup peer attribution and recovery

Status: **prepared, not filed.** Every ask below was implemented and tested on a
branch of teranode `1cca625` (`lab/catchup-fixes`, five commits) and exercised
against a live two-cluster lab. None of them is required for our deployment —
the bridge's synthetic peer-id posture works on unmodified Teranode — but each
fixes a defect reachable without any bridge in the picture.

The reproduction used throughout: mark one block invalid on node B
(`invalidateblock`), keep node A mining descendants, let announcements flow.

## 1. A locally-invalid block must not charge any peer

**Observed** (unmodified `1cca625`): node B's catchup fails every cycle with
`BLOCK_INVALID … block already exists as invalid` — a condition created by
node B itself — and each failure charges the serving peer:
`reportCatchupFailure` at `services/blockvalidation/Server.go:1793` fires
unconditionally before the invalid-block handling below it;
`reportCatchupMalicious(peerID, "invalid_block_validation")` at
`services/blockvalidation/catchup.go:1117` brands the peer malicious from
inside the attempt; `tryAlternativePeersForCatchup`
(`peer_selection.go:132`) charges every alternative in turn. Within minutes
node B had marked its only honest chain peer malicious and reached
"No suitable sync peer found" — an unrecoverable state, since the reputation
is re-earned within seconds of any restart.

**Why it is wrong in Teranode's own terms**: the peer served bytes the node
refused on pre-existing local state; validation never examined them. Reputation
is meant to price peer behaviour, not local policy. Any operator who
`invalidateblock`s near the tip of a live network puts their node into this
state today.

**Minimal change** (validated): before charging, test whether the failure names
a block the local store already holds invalid — the block being caught up OR an
ancestor named by the error ("[ValidateBlock][<hash>] block already exists as
invalid"); the ancestor case is the common one, since announcements arrive for
descendants. Skip the charge at all three sites; on store-lookup failure, fail
in the not-charging direction. With the patch: guard fires at every site,
zero peer charges, zero malicious marks over the same drill.

A typed error from the producer (`BlockValidation.go:1382`) would remove the
one string match the patch carries.

## 2. The catchup circuit breaker can never close (and can wedge on success)

**Observed**: `catchup/circuit_breaker.go` defaults
`SuccessThreshold: 2, MaxHalfOpenRequests: 1` (and `Server.go:296` pins
`MaxHalfOpenRequests: 1`). One successful half-open probe leaves
`successCount=1 < 2` while `halfOpenRequests >= max` refuses every further
call, and no timeout transitions out of half-open: **one success wedges the
breaker permanently** — strictly worse than the failure it guards against.
Separately, probes admitted but never resolved (callers return between
`CanCall` and `Record*`) pin half-open the same way.

**Minimal change** (validated with three unit tests that fail on the unpatched
code): clamp `MaxHalfOpenRequests >= SuccessThreshold` in the constructor, and
recycle the probe budget when half-open has been stale for a further timeout.

## 3. The cached-alternatives walk skips the unhealthy-peer gate

**Observed**: the primary catchup gate consults `isPeerBad` (`Server.go:1712`)
but the cached-alternatives walk deliberately does not
("Don't filter by isPeerBad", `Server.go:1864`). Any announcer cached as an
alternative gets a full catchup attempt even when the p2p service already
considers it unfit — in degraded conditions, precisely when peer selection
matters most.

**Minimal change**: apply the same gate inside the walk. One line, symmetric
with the primary path.

## 4. Sync-failure attribution charges the current sync peer for others' failures

**Observed**: `HandleCatchupFailure` charges the CURRENT sync peer an
interaction failure regardless of which peer's catchup actually failed.

**Minimal change**: pass the failing peer id through and charge only on match;
skip entirely when the id is not in the registry.

## 5. Announce-only identities should never be auto-registered

**Context, not a defect**: an object-plane source (our bridge, but equally any
CDN-like data hub) announces with a valid-format id registered nowhere. Today
every metric RPC is a silent no-op for unknown ids and `Register` is
libp2p-source-only — which is exactly the behaviour we rely on: unregistered ⇒
catchup diverts, delivery unaffected. If registration semantics ever widen
(e.g. auto-create on metric report), such ids would flip to healthy scores and
be offered as catchup sources cluster-wide. **Ask**: keep announce-derived ids
out of the registry, or provide an explicit "object-source only" marker on the
announcement. We monitor for drift regardless
(`teranode_bridge_retrieval_unserved_route_total{class="chain_sync"}`).
