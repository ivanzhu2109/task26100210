// Package assetpreview coordinates preview generation for assets synced
// from multiple sources. Asset references, processing state and preview
// bytes live only in process memory.
package assetpreview

import (
	"context"
	"sync"
)

// Processor generates preview bytes for a frozen asset content.
// It is supplied by the caller; this library performs no media decoding.
// The processor may still return a successful result after ctx is cancelled;
// such late results are discarded by the Coordinator.
type Processor func(ctx context.Context, content []byte) ([]byte, error)

// State is the visibility state of an asset reference.
type State int

const (
	// StateUnknown means the (source, path) pair was never registered.
	StateUnknown State = iota
	// StatePending means the asset is registered but its preview is not ready.
	StatePending
	// StateReady means a preview is available for the current content.
	StateReady
	// StateRemoved means the asset was taken down.
	StateRemoved
)

// assetKey identifies one asset reference: a source plus a relative path.
type assetKey struct {
	source string
	path   string
}

// assetRecord tracks the latest accepted revision of one asset reference.
type assetRecord struct {
	revision int64
	removed  bool
	content  *contentEntry // nil when removed
}

// Coordinator shares preview processing across sources that carry identical
// content. It is safe for concurrent use.
type Coordinator struct {
	mu        sync.Mutex
	processor Processor
	assets    map[assetKey]*assetRecord
	contents  map[contentID]*contentEntry
}

// New creates a Coordinator that uses processor to build previews.
func New(processor Processor) *Coordinator {
	return &Coordinator{
		processor: processor,
		assets:    make(map[assetKey]*assetRecord),
		contents:  make(map[contentID]*contentEntry),
	}
}

// Register records or replaces an asset. content must be the complete frozen
// asset bytes. Only revisions greater than the last seen one take effect;
// repeated or older requests are ignored.
func (c *Coordinator) Register(source, path string, revision int64, content []byte) {
	key := assetKey{source: source, path: path}

	c.mu.Lock()
	defer c.mu.Unlock()

	rec, seen := c.assets[key]
	if seen && revision <= rec.revision {
		return
	}

	id := hashContent(content)
	next := &assetRecord{revision: revision}
	switch {
	case seen && rec.content != nil && rec.content.id == id:
		// Same content re-registered with a newer revision: keep the
		// existing reference and its processing round untouched.
		next.content = rec.content
	case seen && rec.content != nil:
		c.releaseLocked(rec.content)
		next.content = c.acquireLocked(id, content)
	default:
		next.content = c.acquireLocked(id, content)
	}
	c.assets[key] = next
}

// Remove takes an asset down. Only revisions greater than the last seen one
// take effect. The revision is remembered so older requests stay inert.
func (c *Coordinator) Remove(source, path string, revision int64) {
	key := assetKey{source: source, path: path}

	c.mu.Lock()
	defer c.mu.Unlock()

	rec, seen := c.assets[key]
	if seen && revision <= rec.revision {
		return
	}
	if seen && rec.content != nil {
		c.releaseLocked(rec.content)
	}
	c.assets[key] = &assetRecord{revision: revision, removed: true}
}

// Status reports the current state of an asset and, when ready, the preview
// bytes of its current content.
func (c *Coordinator) Status(source, path string) (State, []byte) {
	key := assetKey{source: source, path: path}

	c.mu.Lock()
	defer c.mu.Unlock()

	rec, seen := c.assets[key]
	if !seen {
		return StateUnknown, nil
	}
	if rec.removed {
		return StateRemoved, nil
	}
	if rec.content.ready {
		preview := make([]byte, len(rec.content.preview))
		copy(preview, rec.content.preview)
		return StateReady, preview
	}
	return StatePending, nil
}
