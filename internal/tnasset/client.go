// Package tnasset reads objects back out of the local Teranode cluster so the
// bridge can publish what the cluster produced.
//
// It is the mirror of the retrieval plane: there the bridge answers the
// cluster's pulls, here it makes them. The endpoints and formats are the same
// ones — a subtree is its bare node hashes, a block is Teranode's block
// serialization — which is why a cluster-produced object re-encodes into a push
// frame byte-identical to one that arrived from the fabric.
package tnasset

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/lightwebinc/teranode-bridge/internal/encode"
	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/obs"
	"github.com/lightwebinc/teranode-bridge/internal/tnwire"
)

// Client fetches from the cluster's asset service.
type Client struct {
	base   string // e.g. http://192.0.2.10:20090/api/v1
	client *http.Client
}

// New returns a client. base should include the API prefix.
func New(base string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		base: strings.TrimRight(base, "/"),
		// otelhttp so the reverse path's fetch joins the cluster's trace rather
		// than starting a detached one.
		client: &http.Client{Timeout: timeout, Transport: otelhttp.NewTransport(http.DefaultTransport)},
	}
}

// BuildSubtree fetches a subtree's node hashes and encodes the BRC-143 frame.
//
// The announced hash IS the merkle root, so the frame's root field and the
// identity the cluster gave us are the same value — no recomputation, and no
// chance of publishing a root that disagrees with the node list.
func (c *Client) BuildSubtree(ctx context.Context, hash hashid.Hash) ([]byte, bool, error) {
	defer obs.Timer(obs.AssetFetchDuration, "subtree")()
	body, status, err := c.get(ctx, "/subtree/"+hash.Display())
	if err != nil {
		return nil, false, err
	}
	switch status {
	case http.StatusOK:
	case http.StatusNotFound:
		// Not ours to publish, or not written yet.
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("subtree %s: http %d", hash.Display(), status)
	}
	if len(body) == 0 || len(body)%32 != 0 {
		return nil, false, fmt.Errorf("subtree %s: %d bytes is not a whole number of hashes", hash.Display(), len(body))
	}
	nodes := make([][32]byte, 0, len(body)/32)
	for i := 0; i+32 <= len(body); i += 32 {
		var n [32]byte
		copy(n[:], body[i:i+32])
		nodes = append(nodes, n)
	}
	frame, err := encode.Subtree(hash, nodes)
	if err != nil {
		return nil, false, err
	}
	return frame, true, nil
}

// BuildBlock fetches a block and encodes the BRC-144 frame.
//
// Everything the frame needs is in the response, including the coinbase and its
// BUMP, so no part of the block is reconstructed or guessed.
func (c *Client) BuildBlock(ctx context.Context, hash hashid.Hash) ([]byte, bool, error) {
	defer obs.Timer(obs.AssetFetchDuration, "block")()

	body, status, err := c.get(ctx, "/block/"+hash.Display())
	if err != nil {
		return nil, false, err
	}
	switch status {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("block %s: http %d", hash.Display(), status)
	}
	blk, err := tnwire.FromTeranode(body)
	if err != nil {
		return nil, false, fmt.Errorf("block %s: %w", hash.Display(), err)
	}
	frame, err := blk.Encode()
	if err != nil {
		return nil, false, fmt.Errorf("block %s: %w", hash.Display(), err)
	}
	return frame, true, nil
}

// rateLimitBackoff is the retry ladder for the asset API's rate limiter.
//
// The limiter trips readily: a burst of mined blocks produces a burst of
// notifications, and each one makes us fetch. Without this, a catch-up or a
// fast-mining window silently drops objects from the fabric — they are never
// published, and nothing downstream can tell the difference between "the miner
// produced nothing" and "we were throttled".
var rateLimitBackoff = []time.Duration{200 * time.Millisecond, time.Second, 3 * time.Second, 8 * time.Second}

func (c *Client) get(ctx context.Context, path string) ([]byte, int, error) {
	var lastStatus int
	for attempt := 0; attempt <= len(rateLimitBackoff); attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
		if err != nil {
			return nil, 0, err
		}
		resp, err := c.client.Do(req)
		if err != nil {
			return nil, 0, fmt.Errorf("GET %s: %w", path, err)
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, resp.StatusCode, readErr
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return body, resp.StatusCode, nil
		}
		lastStatus = resp.StatusCode
		if attempt == len(rateLimitBackoff) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, lastStatus, ctx.Err()
		case <-time.After(rateLimitBackoff[attempt]):
		}
	}
	return nil, lastStatus, fmt.Errorf("GET %s: rate limited after %d attempts", path, len(rateLimitBackoff)+1)
}
