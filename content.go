package assetpreview

import (
	"context"
	"crypto/sha256"
)

// contentID identifies one distinct frozen content by its hash.
type contentID [sha256.Size]byte

// hashContent derives the identity of a frozen content payload.
func hashContent(content []byte) contentID {
	return sha256.Sum256(content)
}

// contentEntry tracks the shared processing lifecycle of one content
// generation. A new entry is created whenever a previously unreferenced
// content becomes referenced again, so late results from a retired entry
// can never leak into a new one.
type contentEntry struct {
	id contentID

	refs    int  // number of live asset references
	ready   bool // preview is usable
	done    bool // processing finished (success or failure)
	preview []byte

	cancel context.CancelFunc // non-nil while processing is running
}

// acquireLocked returns the live entry for id, creating and starting a new
// processing round when none exists. The caller must hold c.mu.
func (c *Coordinator) acquireLocked(id contentID, content []byte) *contentEntry {
	if entry, ok := c.contents[id]; ok {
		entry.refs++
		return entry
	}
	entry := &contentEntry{id: id, refs: 1}
	c.contents[id] = entry
	c.startLocked(entry, content)
	return entry
}

// releaseLocked drops one reference. When the last reference goes away the
// entry is retired: pending or running work is cancelled and any preview is
// invalidated by removing the entry from the live set. The caller must hold
// c.mu.
func (c *Coordinator) releaseLocked(entry *contentEntry) {
	entry.refs--
	if entry.refs > 0 {
		return
	}
	delete(c.contents, entry.id)
	if entry.cancel != nil {
		entry.cancel()
		entry.cancel = nil
	}
}

// startLocked launches processing for a fresh entry. The caller must hold
// c.mu.
func (c *Coordinator) startLocked(entry *contentEntry, content []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	entry.cancel = cancel
	go c.run(entry, ctx, content)
}

// run executes the processor and publishes the result only if the entry is
// still the live generation for its content. A processor may report success
// after cancellation; such late results are discarded here.
func (c *Coordinator) run(entry *contentEntry, ctx context.Context, content []byte) {
	preview, err := c.processor(ctx, content)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.contents[entry.id] != entry {
		return // retired generation; discard the late result
	}
	entry.cancel = nil
	entry.done = true
	if err == nil {
		entry.preview = preview
		entry.ready = true
	}
}
