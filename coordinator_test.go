package assetpreview

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// proc is a controllable Processor. Each call announces itself on started,
// then blocks until either a token is sent on proceed (success) or the
// context is cancelled.
type proc struct {
	mu                 sync.Mutex
	calls              int
	started            chan []byte
	proceed            chan struct{}
	canceled           chan []byte
	succeedAfterCancel bool
}

func newProc() *proc {
	return &proc{
		started:  make(chan []byte, 16),
		proceed:  make(chan struct{}, 16),
		canceled: make(chan []byte, 16),
	}
}

func (p *proc) process(ctx context.Context, content []byte) ([]byte, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	p.started <- content
	select {
	case <-p.proceed:
		return append([]byte("preview:"), content...), nil
	case <-ctx.Done():
		p.canceled <- content
		if p.succeedAfterCancel {
			return append([]byte("stale:"), content...), nil
		}
		return nil, ctx.Err()
	}
}

func (p *proc) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *proc) allowOne() { p.proceed <- struct{}{} }

func waitStarted(t *testing.T, p *proc) []byte {
	t.Helper()
	select {
	case content := <-p.started:
		return content
	case <-time.After(2 * time.Second):
		t.Fatal("processor was not invoked in time")
		return nil
	}
}

func waitCanceled(t *testing.T, p *proc) []byte {
	t.Helper()
	select {
	case content := <-p.canceled:
		return content
	case <-time.After(2 * time.Second):
		t.Fatal("processing was not cancelled in time")
		return nil
	}
}

func waitReady(t *testing.T, c *Coordinator, source, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, preview := c.Status(source, path)
		if state == StateReady {
			return preview
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("asset %s/%s did not become ready", source, path)
	return nil
}

func mustState(t *testing.T, c *Coordinator, source, path string, want State) {
	t.Helper()
	if got, _ := c.Status(source, path); got != want {
		t.Fatalf("asset %s/%s: got state %v, want %v", source, path, got, want)
	}
}

// Identical content from two sources is processed once; removing one
// reference keeps the preview available for the other.
func TestSharedContentProcessedOnce(t *testing.T) {
	p := newProc()
	c := New(p.process)
	content := []byte("same-bytes")

	c.Register("src-a", "img/logo.png", 1, content)
	c.Register("src-b", "img/logo.png", 1, content)
	waitStarted(t, p)
	p.allowOne()

	want := []byte("preview:same-bytes")
	if got := waitReady(t, c, "src-a", "img/logo.png"); !bytes.Equal(got, want) {
		t.Fatalf("src-a preview = %q, want %q", got, want)
	}
	if got := waitReady(t, c, "src-b", "img/logo.png"); !bytes.Equal(got, want) {
		t.Fatalf("src-b preview = %q, want %q", got, want)
	}
	if n := p.callCount(); n != 1 {
		t.Fatalf("processor called %d times, want 1", n)
	}

	c.Remove("src-a", "img/logo.png", 2)
	mustState(t, c, "src-a", "img/logo.png", StateRemoved)
	if got, preview := c.Status("src-b", "img/logo.png"); got != StateReady || !bytes.Equal(preview, want) {
		t.Fatalf("src-b after src-a removal: state=%v preview=%q", got, preview)
	}
	if n := p.callCount(); n != 1 {
		t.Fatalf("processor called %d times after removal, want 1", n)
	}
}

// Replacing content on one path: a late older revision and the interleaved
// completion of the old processing must never surface the old preview.
func TestReplaceContentWithInterleavedCompletion(t *testing.T) {
	p := newProc()
	c := New(p.process)

	c.Register("src-a", "img/logo.png", 1, []byte("old-content"))
	waitStarted(t, p)

	// Replace with new content before the old processing completes.
	c.Register("src-a", "img/logo.png", 2, []byte("new-content"))
	waitStarted(t, p)

	// While new content is still processing, status must be pending and
	// must not fall back to any old preview.
	mustState(t, c, "src-a", "img/logo.png", StatePending)

	// The old processing was cancelled by the replacement.
	if got := waitCanceled(t, p); !bytes.Equal(got, []byte("old-content")) {
		t.Fatalf("cancelled content = %q, want old-content", got)
	}

	// A late older revision is inert.
	c.Register("src-a", "img/logo.png", 1, []byte("old-content"))
	mustState(t, c, "src-a", "img/logo.png", StatePending)

	p.allowOne()
	if got := waitReady(t, c, "src-a", "img/logo.png"); !bytes.Equal(got, []byte("preview:new-content")) {
		t.Fatalf("preview = %q, want preview:new-content", got)
	}
	if n := p.callCount(); n != 2 {
		t.Fatalf("processor called %d times, want 2", n)
	}
}

// Removing the last reference cancels in-flight work and invalidates any
// preview; re-registering the same content starts a fresh round and a late
// success from the retired round is discarded.
func TestLastReferenceRemovalInvalidatesAndRestarts(t *testing.T) {
	p := newProc()
	p.succeedAfterCancel = true // processor reports success even after cancel
	c := New(p.process)
	content := []byte("round-trip")

	c.Register("src-a", "a.bin", 1, content)
	waitStarted(t, p)
	c.Remove("src-a", "a.bin", 2)
	waitCanceled(t, p)
	mustState(t, c, "src-a", "a.bin", StateRemoved)

	// Re-register the same content: a brand-new processing round.
	c.Register("src-a", "a.bin", 3, content)
	waitStarted(t, p)
	if n := p.callCount(); n != 2 {
		t.Fatalf("processor called %d times, want 2", n)
	}
	mustState(t, c, "src-a", "a.bin", StatePending)

	// The retired round's late success must not appear; only completing the
	// new round yields a preview, and it comes from the new round.
	p.allowOne()
	if got := waitReady(t, c, "src-a", "a.bin"); !bytes.Equal(got, []byte("preview:round-trip")) {
		t.Fatalf("preview = %q, want preview:round-trip", got)
	}
}

// Revisions gate both register and remove; a removed asset remembers its
// revision so older requests stay inert.
func TestRevisionGating(t *testing.T) {
	p := newProc()
	c := New(p.process)

	c.Register("src-a", "f.bin", 5, []byte("v5"))
	waitStarted(t, p)

	// Equal and older revisions do not replace content or restart work.
	c.Register("src-a", "f.bin", 5, []byte("other"))
	c.Register("src-a", "f.bin", 3, []byte("older"))
	if n := p.callCount(); n != 1 {
		t.Fatalf("processor called %d times, want 1", n)
	}

	// Older remove is ignored.
	c.Remove("src-a", "f.bin", 4)
	mustState(t, c, "src-a", "f.bin", StatePending)

	// Newer remove takes effect and is remembered.
	c.Remove("src-a", "f.bin", 6)
	mustState(t, c, "src-a", "f.bin", StateRemoved)
	c.Register("src-a", "f.bin", 6, []byte("v6"))
	c.Register("src-a", "f.bin", 2, []byte("v2"))
	mustState(t, c, "src-a", "f.bin", StateRemoved)
	if n := p.callCount(); n != 1 {
		t.Fatalf("processor called %d times, want 1", n)
	}

	// A newer revision revives the asset.
	c.Register("src-a", "f.bin", 7, []byte("v7"))
	waitStarted(t, p)
	mustState(t, c, "src-a", "f.bin", StatePending)
}

// Removal state is per (source, path); other sources are unaffected.
func TestRemovalIsPerSource(t *testing.T) {
	p := newProc()
	c := New(p.process)
	content := []byte("shared")

	c.Register("src-a", "p", 1, content)
	c.Register("src-b", "p", 1, content)
	waitStarted(t, p)
	p.allowOne()
	waitReady(t, c, "src-a", "p")

	c.Remove("src-b", "p", 2)
	mustState(t, c, "src-b", "p", StateRemoved)
	mustState(t, c, "src-a", "p", StateReady)

	// src-b re-registers with a newer revision: the ready preview is reused
	// directly without reprocessing.
	c.Register("src-b", "p", 3, content)
	mustState(t, c, "src-b", "p", StateReady)
	if n := p.callCount(); n != 1 {
		t.Fatalf("processor called %d times, want 1", n)
	}
}

// Different content must not share processing results.
func TestDifferentContentNotShared(t *testing.T) {
	p := newProc()
	c := New(p.process)

	c.Register("src-a", "x", 1, []byte("one"))
	c.Register("src-b", "x", 1, []byte("two"))
	waitStarted(t, p)
	waitStarted(t, p)
	p.allowOne()
	p.allowOne()

	if got := waitReady(t, c, "src-a", "x"); !bytes.Equal(got, []byte("preview:one")) {
		t.Fatalf("src-a preview = %q", got)
	}
	if got := waitReady(t, c, "src-b", "x"); !bytes.Equal(got, []byte("preview:two")) {
		t.Fatalf("src-b preview = %q", got)
	}
}

// Unknown assets report StateUnknown.
func TestUnknownAsset(t *testing.T) {
	c := New(newProc().process)
	mustState(t, c, "nope", "nope", StateUnknown)
}
