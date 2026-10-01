# task26100210

`assetpreview`：进程内存中的素材预览协调库，由上层同步任务直接调用。

## 能力

- `New(processor)`：以调用方提供的本机处理能力（`Processor`）创建协调器，本库不做媒体解码。
- `Register(source, path, revision, content)`：登记或替换素材，`content` 为已冻结的完整字节。
- `Remove(source, path, revision)`：下架素材，修订号被记住，同号或更旧的请求不再生效。
- `Status(source, path)`：查询当前状态（`StateUnknown` / `StatePending` / `StateReady` / `StateRemoved`）及预览字节。

## 语义

- 素材由（来源标识, 相对路径）共同标识；只接受比已见修订号更大的变更。
- 完整字节相同的在架素材共享一次处理与预览；内容不同绝不共享。
- 新修订生效后查询只见新内容状态，未就绪时不会回退展示旧预览。
- 最后一个引用消失时，等待中的工作被撤销、进行中的工作收到取消、已有预览失效；
  之后重新登记相同内容属于新一轮处理，上一轮迟到的结果（包括取消后仍返回的成功）会被丢弃。

## 测试

```sh
go test ./...
go test -race ./...
```
