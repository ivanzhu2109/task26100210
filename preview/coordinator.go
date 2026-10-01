// Package preview 提供素材预览协调库：按内容共享预览处理，
// 仅使用进程内存保存素材引用、处理状态与预览字节。
package preview

import (
	"context"
	"sync"
)

// State 表示素材当前的可见状态。
type State int

const (
	// StatePending 素材在架，但预览尚未就绪。
	StatePending State = iota
	// StateReady 素材在架，预览已就绪。
	StateReady
	// StateRemoved 素材已下架（或从未登记）。
	StateRemoved
)

// Processor 由调用方提供，负责从完整素材字节生成预览。
// 实现应当尊重 ctx 取消，但允许在取消后仍返回成功结果；
// 协调库会丢弃已失效的结果。
type Processor interface {
	Process(ctx context.Context, content []byte) ([]byte, error)
}

// assetRecord 记录一份素材（来源+路径）的当前登记状态。
type assetRecord struct {
	revision uint64
	removed  bool
	group    *contentGroup // 下架时为 nil
}

// Coordinator 协调素材登记、下架与预览共享。
// 所有方法可并发调用。
type Coordinator struct {
	proc Processor

	mu     sync.Mutex
	assets map[string]*assetRecord
	groups map[string]*contentGroup
	closed bool
	wg     sync.WaitGroup
}

// New 创建一个使用给定处理能力的 Coordinator。
func New(p Processor) *Coordinator {
	return &Coordinator{
		proc:   p,
		assets: make(map[string]*assetRecord),
		groups: make(map[string]*contentGroup),
	}
}

func assetKey(source, path string) string {
	return source + "\x00" + path
}

// Register 登记或替换一份素材。content 必须是调用方已冻结的完整字节。
// 仅当 revision 大于该素材已见修订号时生效，返回是否接受。
func (c *Coordinator) Register(source, path string, revision uint64, content []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	key := assetKey(source, path)
	rec, ok := c.assets[key]
	if ok && revision <= rec.revision {
		return false
	}
	if !ok {
		rec = &assetRecord{}
		c.assets[key] = rec
	}
	rec.revision = revision
	rec.removed = false
	c.detachLocked(rec)

	ck := contentKey(content)
	g, ok := c.groups[ck]
	if !ok {
		g = &contentGroup{key: ck, content: content}
		c.groups[ck] = g
		c.startProcessingLocked(g)
	}
	g.refs++
	rec.group = g
	return true
}

// Remove 下架一份素材。仅当 revision 大于该素材已见修订号时生效。
// 下架后仍记住修订号，重复或更旧的请求不改变状态。
func (c *Coordinator) Remove(source, path string, revision uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := assetKey(source, path)
	rec, ok := c.assets[key]
	if ok && revision <= rec.revision {
		return false
	}
	if !ok {
		rec = &assetRecord{}
		c.assets[key] = rec
	}
	rec.revision = revision
	rec.removed = true
	c.detachLocked(rec)
	return true
}

// detachLocked 解除素材与其内容分组的关联；最后一个引用消失时
// 撤销处理并使已有预览失效。调用方必须持有锁。
func (c *Coordinator) detachLocked(rec *assetRecord) {
	g := rec.group
	if g == nil {
		return
	}
	rec.group = nil
	g.refs--
	if g.refs == 0 {
		g.cancelProcessing()
		delete(c.groups, g.key)
	}
}

// startProcessingLocked 为新内容分组启动后台处理。调用方必须持有锁。
func (c *Coordinator) startProcessingLocked(g *contentGroup) {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		preview, err := c.proc.Process(ctx, g.content)
		c.mu.Lock()
		defer c.mu.Unlock()
		if g.dead || err != nil {
			// 分组已失效或处理失败：丢弃结果，保持未就绪。
			return
		}
		g.ready = true
		g.preview = preview
	}()
}

// Lookup 查询素材当前状态；就绪时同时返回预览字节。
func (c *Coordinator) Lookup(source, path string) (State, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.assets[assetKey(source, path)]
	if !ok || rec.removed || rec.group == nil {
		return StateRemoved, nil
	}
	if !rec.group.ready {
		return StatePending, nil
	}
	preview := make([]byte, len(rec.group.preview))
	copy(preview, rec.group.preview)
	return StateReady, preview
}

// Close 停止所有后台处理并释放资源。
func (c *Coordinator) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	for _, g := range c.groups {
		g.cancelProcessing()
	}
	c.groups = make(map[string]*contentGroup)
	c.mu.Unlock()
	c.wg.Wait()
}
