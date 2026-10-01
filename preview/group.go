package preview

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// contentKey 以内容哈希唯一标识一份内容，不同内容不会共享处理结果。
func contentKey(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// contentGroup 表示一份唯一内容及其共享的处理状态与预览。
// 所有字段仅在 Coordinator 的锁保护下访问。
type contentGroup struct {
	key     string
	content []byte

	refs    int    // 当前引用该内容的在架素材数
	ready   bool   // 预览是否已就绪
	preview []byte // 就绪后的预览字节
	dead    bool   // 最后一个引用已消失，迟到的结果必须丢弃

	cancel context.CancelFunc
}

// cancelProcessing 撤销该内容的处理：等待中的工作被撤销，
// 正在处理的工作收到取消通知，已有预览失效。
// 调用方必须持有锁。
func (g *contentGroup) cancelProcessing() {
	g.dead = true
	g.ready = false
	g.preview = nil
	if g.cancel != nil {
		g.cancel()
	}
}
