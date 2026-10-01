# previewcoord

素材预览协调库（Go）。由上层同步任务直接调用，素材引用、处理状态和预览字节仅保存在进程内存中。

## 能力

- `Register(source, path, revision, content)`：登记或替换素材，`content` 为调用方已冻结的完整字节。
- `Remove(source, path, revision)`：下架素材。
- `Lookup(source, path)`：查询当前状态（`StatePending` / `StateReady` / `StateRemoved`）及预览字节。
- `Close()`：停止所有后台处理。

## 语义

- 素材由（来源标识, 相对路径）共同标识；每份素材只接受比已见修订号更大的变更，
  重复或较旧请求不改变状态。下架后仍记住修订号（墓碑）。
- 完整字节相同（SHA-256）的在架素材共享同一次处理与预览；已有可用预览且仍有引用时，
  新增相同内容的素材直接复用，不再处理。不同来源的下架状态相互独立。
- 新内容修订生效后，查询只能看到新内容的状态与预览；未就绪时不会回退展示旧预览。
  旧内容若仍被其他素材引用，其处理继续，结果仅供仍引用它的素材使用。
- 预览由调用方提供的 `Processor` 生成（本库不做媒体解码）。最后一个引用消失时：
  等待中的工作被撤销，处理中的工作收到 `ctx` 取消，已有预览失效；
  之后重新登记相同内容属于新一轮处理，上一轮迟到的结果会被丢弃。
- 登记或下架成功返回后，随后发起的 `Lookup` 立即反映该变更。

## 示例

```go
proc := myProcessor{} // 实现 preview.Processor
c := preview.New(proc)
defer c.Close()

c.Register("brand-dam", "hero/banner.psd", 7, contentBytes)
state, previewBytes := c.Lookup("brand-dam", "hero/banner.psd")
```

## 测试

```sh
go test -race ./...
```
