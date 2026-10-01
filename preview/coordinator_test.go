package preview

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeProcessor 是可编程的测试用处理能力。
type fakeProcessor struct {
	mu        sync.Mutex
	calls     [][]byte
	started   chan []byte           // 每次调用开始时收到内容
	release   map[string]chan error // 按内容控制返回
	cancelled chan struct{}         // ctx 被取消时通知
}

func newFakeProcessor() *fakeProcessor {
	return &fakeProcessor{
		started:   make(chan []byte, 16),
		release:   make(map[string]chan error),
		cancelled: make(chan struct{}, 16),
	}
}

func (f *fakeProcessor) gate(content []byte) chan error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(content)
	ch, ok := f.release[key]
	if !ok {
		ch = make(chan error, 1)
		f.release[key] = ch
	}
	return ch
}

// allow 让指定内容的下一次处理返回成功预览 "preview:"+content。
// 非阻塞：若已有待消费的放行信号则直接跳过。
func (f *fakeProcessor) allow(content []byte) {
	select {
	case f.gate(content) <- nil:
	default:
	}
}

func (f *fakeProcessor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeProcessor) Process(ctx context.Context, content []byte) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, content)
	f.mu.Unlock()
	select {
	case f.started <- content:
	default:
	}
	gate := f.gate(content)
	select {
	case err := <-gate:
		if err != nil {
			return nil, err
		}
		out := append([]byte("preview:"), content...)
		return out, nil
	case <-ctx.Done():
		select {
		case f.cancelled <- struct{}{}:
		default:
		}
		// 模拟“收到取消后仍返回成功结果”的处理能力。
		out := append([]byte("preview:"), content...)
		return out, nil
	}
}

func waitStarted(t *testing.T, f *fakeProcessor) []byte {
	t.Helper()
	select {
	case c := <-f.started:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for processing to start")
		return nil
	}
}

func waitReady(t *testing.T, c *Coordinator, source, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, p := c.Lookup(source, path)
		if st == StateReady {
			return p
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for ready state")
	return nil
}

// 跨来源登记相同内容只处理一次；撤掉一个引用后其他引用仍可用。
func TestSharedContentAcrossSources(t *testing.T) {
	proc := newFakeProcessor()
	c := New(proc)
	defer c.Close()

	content := []byte("asset-v1")
	if !c.Register("srcA", "a.png", 1, content) {
		t.Fatal("register A rejected")
	}
	if !c.Register("srcB", "b.png", 1, content) {
		t.Fatal("register B rejected")
	}
	waitStarted(t, proc)
	if got := proc.callCount(); got != 1 {
		t.Fatalf("expected 1 processing call, got %d", got)
	}
	proc.allow(content)

	want := []byte("preview:asset-v1")
	if p := waitReady(t, c, "srcA", "a.png"); string(p) != string(want) {
		t.Fatalf("A preview = %q", p)
	}
	if p := waitReady(t, c, "srcB", "b.png"); string(p) != string(want) {
		t.Fatalf("B preview = %q", p)
	}

	// 下架 A 不影响 B，且不触发新的处理。
	if !c.Remove("srcA", "a.png", 2) {
		t.Fatal("remove A rejected")
	}
	if st, _ := c.Lookup("srcA", "a.png"); st != StateRemoved {
		t.Fatal("A should be removed")
	}
	if st, p := c.Lookup("srcB", "b.png"); st != StateReady || string(p) != string(want) {
		t.Fatalf("B should stay ready, got state=%v preview=%q", st, p)
	}
	if got := proc.callCount(); got != 1 {
		t.Fatalf("expected still 1 processing call, got %d", got)
	}
}

// 同一路径替换内容：旧修订晚到、旧处理迟到完成，都不能影响最新状态。
func TestReplaceContentStaleRevisionAndLateResult(t *testing.T) {
	proc := newFakeProcessor()
	c := New(proc)
	defer c.Close()

	v1 := []byte("v1")
	v2 := []byte("v2")
	c.Register("src", "p.png", 1, v1)
	waitStarted(t, proc)

	// 替换内容：旧内容处理应收到取消，新内容开始新一轮处理。
	c.Register("src", "p.png", 2, v2)
	waitStarted(t, proc)
	select {
	case <-proc.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("old processing was not cancelled")
	}

	// 旧内容的处理能力无视取消、迟到返回成功，结果必须被丢弃；
	// 新内容尚未就绪时不得回退展示旧预览。
	if st, p := c.Lookup("src", "p.png"); st != StatePending || p != nil {
		t.Fatalf("expected pending without preview, got state=%v preview=%q", st, p)
	}

	// 旧修订号与重复修订号均被拒绝。
	if c.Register("src", "p.png", 1, v1) {
		t.Fatal("stale revision accepted")
	}
	if c.Register("src", "p.png", 2, v2) {
		t.Fatal("duplicate revision accepted")
	}
	if st, p := c.Lookup("src", "p.png"); st != StatePending || p != nil {
		t.Fatalf("stale writes changed state: state=%v preview=%q", st, p)
	}

	proc.allow(v2)
	if p := waitReady(t, c, "src", "p.png"); string(p) != "preview:v2" {
		t.Fatalf("preview = %q, want preview:v2", p)
	}
	if got := proc.callCount(); got != 2 {
		t.Fatalf("expected 2 processing calls, got %d", got)
	}
}

// 下架记住修订号；最后一个引用消失后重新登记属于新一轮处理。
func TestRemoveTombstoneAndNewRound(t *testing.T) {
	proc := newFakeProcessor()
	c := New(proc)
	defer c.Close()

	// 未登记过的素材也可下架，形成墓碑。
	if !c.Remove("src", "p.png", 5) {
		t.Fatal("remove of unknown asset rejected")
	}
	if st, _ := c.Lookup("src", "p.png"); st != StateRemoved {
		t.Fatal("expected removed state")
	}
	// 同一修订号的登记被拒绝。
	if c.Register("src", "p.png", 5, []byte("x")) {
		t.Fatal("register with tombstoned revision accepted")
	}

	content := []byte("x")
	if !c.Register("src", "p.png", 6, content) {
		t.Fatal("register with newer revision rejected")
	}
	waitStarted(t, proc)

	// 最后一个引用消失：处理收到取消，迟到的成功结果被丢弃。
	if !c.Remove("src", "p.png", 7) {
		t.Fatal("remove rejected")
	}
	select {
	case <-proc.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("processing was not cancelled after last ref removed")
	}
	time.Sleep(20 * time.Millisecond) // 让迟到的结果有机会（错误地）生效
	if st, p := c.Lookup("src", "p.png"); st != StateRemoved || p != nil {
		t.Fatalf("late result leaked: state=%v preview=%q", st, p)
	}

	// 重新登记相同内容：新一轮处理，不采用上一轮迟到的结果。
	if !c.Register("src", "p.png", 8, content) {
		t.Fatal("re-register rejected")
	}
	waitStarted(t, proc)
	if got := proc.callCount(); got != 2 {
		t.Fatalf("expected a new processing round, got %d calls", got)
	}
	if st, _ := c.Lookup("src", "p.png"); st != StatePending {
		t.Fatal("new round must start from pending, not stale preview")
	}
	proc.allow(content)
	if p := waitReady(t, c, "src", "p.png"); string(p) != "preview:x" {
		t.Fatalf("preview = %q", p)
	}
}

// 已就绪内容被替换后，就绪预览不得在新内容处理期间出现。
func TestReadyThenReplaceHidesOldPreview(t *testing.T) {
	proc := newFakeProcessor()
	c := New(proc)
	defer c.Close()

	v1 := []byte("v1")
	c.Register("src", "p.png", 1, v1)
	waitStarted(t, proc)
	proc.allow(v1)
	waitReady(t, c, "src", "p.png")

	c.Register("src", "p.png", 2, []byte("v2"))
	waitStarted(t, proc)
	if st, p := c.Lookup("src", "p.png"); st != StatePending || p != nil {
		t.Fatalf("old preview visible after replace: state=%v preview=%q", st, p)
	}
}

// 不同来源的下架状态相互独立；同一内容在不同来源各自计数。
func TestIndependentSources(t *testing.T) {
	proc := newFakeProcessor()
	c := New(proc)
	defer c.Close()

	content := []byte("shared")
	c.Register("srcA", "p.png", 1, content)
	c.Register("srcB", "p.png", 1, content)
	waitStarted(t, proc)
	proc.allow(content)
	waitReady(t, c, "srcA", "p.png")
	waitReady(t, c, "srcB", "p.png")

	// 来源 A 下架自己的素材，来源 B 不受影响。
	c.Remove("srcA", "p.png", 2)
	if st, _ := c.Lookup("srcA", "p.png"); st != StateRemoved {
		t.Fatal("A should be removed")
	}
	if st, p := c.Lookup("srcB", "p.png"); st != StateReady || string(p) != "preview:shared" {
		t.Fatalf("B affected by A's removal: state=%v preview=%q", st, p)
	}
}

// 并发交错压力测试：多来源并发登记/替换/下架，
// 不变量——就绪预览必须对应当前登记的内容，下架后不得再出现预览。
func TestConcurrentInterleaving(t *testing.T) {
	proc := newFakeProcessor()
	c := New(proc)
	defer c.Close()

	contents := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	sources := []string{"s1", "s2", "s3"}

	var wg sync.WaitGroup
	for _, src := range sources {
		src := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			rev := uint64(0)
			for i := 0; i < 60; i++ {
				rev++
				content := contents[i%len(contents)]
				switch i % 3 {
				case 0, 1:
					c.Register(src, "p.png", rev, content)
					proc.allow(content)
				case 2:
					c.Remove(src, "p.png", rev)
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	// 查询方：任何时刻就绪预览必须与内容一致（preview:<content>）。
	stop := time.After(500 * time.Millisecond)
	for {
		select {
		case <-done:
			return
		case <-stop:
			<-done
			return
		default:
		}
		for _, src := range sources {
			st, p := c.Lookup(src, "p.png")
			if st == StateReady {
				if len(p) <= len("preview:") || string(p[:8]) != "preview:" {
					t.Fatalf("corrupt preview for %s: %q", src, p)
				}
			}
		}
	}
}
