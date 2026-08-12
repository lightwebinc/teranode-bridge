# Upstream ask: origin marker on blockchain Subscribe notifications

> Status: DRAFT, ready to file against `bsv-blockchain/teranode`. The bridge
> no longer depends on it — the `-mine-tag` coinbase gate closes the block
> case with configuration only — so this is the principled fix, not a blocker.

## Problem

`Notification` (blockchain_api.proto:501) carries `type`, `hash`, `base_URL`
and a `metadata` map, but no origin: a subscriber cannot tell a block this node
MINED from one it merely VALIDATED after receiving it from a peer. The
information is in scope and discarded at the single producing funnel —
`services/blockchain/Server.go:1100` builds the `NotificationType_Block`
notification inside `AddBlock`, two statements after `request.PeerId` was
passed to `StoreBlock` (`:1072`).

External submitters (mining-adjacent infrastructure, exchange listeners,
bridges) that re-publish locally-produced blocks must otherwise reconstruct
origin from side channels. The empty-`peer_id` heuristic is not sufficient:
the legacy SV-node bridge also calls `AddBlock` with `peerID=""`
(`services/legacy/netsync/handle_block.go:283` →
`blockvalidation` → `AddBlock`), so "empty means local" misattributes
legacy-received blocks.

## Proposed change (additive, no wire break)

Stamp the existing `NotificationMetadata` map at the two producer sites:

- `services/blockchain/Server.go:1100` — add
  `metadata: {"peer_id": request.PeerId, "mined_locally": <bool>}` where
  `mined_locally` is threaded from the caller.
- `services/blockchain/LocalClient.go:114` — same for the in-process path.

`mined_locally` needs one bool threaded from block assembly's
`AddBlock(ctx, block, "")` call (`services/blockassembly/Server.go:1608`).
`AddBlockRequest` already carries a candidate field: `external` (written once
at `Client.go:328`, read nowhere) — this change could give it its meaning, or
deprecate it in favour of an explicit field.

Consumers switch on `type` first and nil-guard `metadata` today (the p2p
consumer does), so absent metadata stays valid: the rule is "absent = UNKNOWN,
never LOCAL". The seven other Block-producer sites (initial-tip replays at
`Server.go:931/936` and `LocalClient.go:332/343`, invalidate/revalidate at
`:2338/:2430`, `BlockSubtreesSet` at `:2643`) deliberately stay unstamped.

`NotificationType_Subtree` needs nothing: its only producer is block assembly
(`services/blockassembly/Server.go:643`), so subtree notifications are
structurally local already.

## Why it is safe

- No `.proto` change: `metadata` is `map<string,string>`, already used by
  `PeerFailure` (`peer_id`) and `BlockPersisted` (`height`).
- Wire-compatible: unknown map keys are ignored by every existing consumer.
- Two files stamped, one bool threaded; the tests that construct notifications
  directly (`blockchain_api_test.go:35`) are unaffected.
