# NovelAI 原始协议实测（2026-10-17）

本记录来自用户明确授权的共用账户空闲探测。旧生产网关会转换响应，因此在 bob 的临时目录直连 `https://image.novelai.net`；没有更改生产服务、配置、数据库或部署版本。凭据只在远端进程内存使用，原始响应和运行记录保存在忽略目录，不提交账户信息、生成 ID 或图片。

每次调用前只读检查生产 queued/pending/running 为空、账户 inFlight 为零、冷却已结束，最近生产任务结束超过30秒；探测串行且间隔至少30秒。先查询有效、未过期的 Opus 并核实免费报价。这个检查不能锁住随后新到的生产请求；本轮每次前后均未观察到生产活动任务，不宣称消除了并发竞争窗口。

用户随后取消原定三次上限，要求免费调用避开成员请求。首轮5次提交加后续1次ZIP核对，共6次生成HTTP提交，5张成功、1次流内失败；没有客户端自动重发。最初错误使用 `api.novelai.net/user/data` 导致预检400，在生成POST前退出，不计入生成提交；之后统一使用项目既有的 image 域名额度接口。

## 请求与结果

全部使用 `nai-diffusion-4-5-full`、`action=generate`、512×512、4步、单张、seed=42、scale=5、cfg_rescale=0、`k_euler_ancestral/karras`，没有参考、放大或其他附加收费项。提示词为合成的蓝色圆形描述。第2次起补齐 `params_version=3`、V4正负caption结构；所有生成前本地报价均为free/0且官方订阅检查通过。

时间为UTC；对应台北时间加8小时。

| 次序／开始时间 | 路径与格式请求 | 官方HTTP／媒体类型 | 结果与余额证据 |
| --- | --- | --- | --- |
| 1／07:25:19 | stream路径；stream=sse；Accept SSE；PNG；未带V4 caption结构 | 200／text/event-stream | 3个retry事件后error，code为字符串500，无图；两类Anlas前后差额均0 |
| 2／07:28:50 | stream路径；stream=sse；Accept SSE；PNG | 200／text/event-stream | 3个预览及1个最终图；两类差额均0 |
| 3／07:29:40 | stream路径；stream=msgpack；Accept SSE；WebP | 200／application/msgpack | 3个预览及1个最终图；两类差额均0 |
| 4／07:30:31 | 普通路径；Accept JSON；WebP；冗余prompt与input相同 | 200／application/json | images数组包含image/index/seed，seed=42；两类差额均0 |
| 5／07:31:54 | stream路径；省略stream；Accept application/msgpack；PNG | 200／text/event-stream | 3个预览及1个最终图；后置额度查询未完成，逐次差额未知 |
| 6／08:08:55 | 普通路径；Accept application/zip；PNG | 200／binary/octet-stream | ZIP内一张image_0.png，512×512，CRC核验通过；两类Anlas前后差额均0 |

第5次的后置额度查询未成功（设有15秒截止，探测器没有保存具体异常原因），响应图片已完整保留。后续只读复查返回有效Opus，官方两类余额均与生产缓存一致、活动队列为空；该缓存更新时间晚于生成，不能据此补称第5次已实测零消费。没有继续生成以重试余额查询。

补齐V4结构后成功不单独证明第一次失败的根因；仅凭500不能断言服务故障或某字段一定必填。实际生成成功也不覆盖其他模型、尺寸、动作或计费组合。

## 已核实的协议

- SSE为 `event: <类型>\ndata: <JSON>\n\n`。JSON含同名`event_type`；image为无data URL前缀的base64。预览有`samp_ix/step_ix/gen_id/sigma/image`，最终图有`samp_ix/gen_id/image`。本轮step_ix为0、1、2，预览为256×256 JPEG，最终图为512×512 PNG或WebP；预览格式不随最终图格式变化。
- 最终事件后连接结束，未出现`[DONE]`等额外标记。错误样本有`event_type/code/message`而没有`samp_ix`；retry是上游同一HTTP响应内的事件，不是本地再次提交。
- MessagePack仍为4字节大端长度加对象，image为binary；显式`stream=msgpack`覆盖SSE Accept，实际媒体类型为`application/msgpack`。
- 省略stream时即使Accept要求MessagePack仍返回SSE。尚未单独采样“显式sse＋MessagePack Accept”，网关按上述证据与官方stream枚举统一由stream选择格式，不再用Accept推断流式类型。
- 普通JSON与ZIP成功均实测200，与[Swagger](https://image.novelai.net/docs/doc.json)列出的201不一致。网关C03-04据此将普通生成JSON/ZIP成功改为200；Vibe201及其他操作独立处理。官方本次ZIP使用binary/octet-stream，网关保留明确的application/zip，而非声称逐字节透传全部头部。

网关SSE复用已有预览/结果生命周期，不伪造未保留的gen_id、sigma或retry。批次错误仍附本地samp_ix，首事件前错误使用HTTP JSON；这些边界见[API契约](api-contract.md)。

## 原始响应指纹

指纹用于关联受保护的原始证据，不包含图片或凭据。长度是完整HTTP响应体字节数。

| 次序 | 字节数 | SHA-256 |
| --- | ---: | --- |
| 1 | 549 | aab2632828b62e7e91054806d379dd70ded07e93056ddca4d02ba4e12f112021 |
| 2 | 171305 | d8e9f2a62bfe9f71ecf56da39d127d1a71f78832eaaa315ed6d6aad9681cb921 |
| 3 | 90999 | 69b7dcd042b2fffc67f180bd9899b8a701945bcb00cf0d6fc6ec4e9446b31fc9 |
| 4 | 110318 | 0c4066fce38d649ec10831d82fdc50ddaea8f07db46fd543615124e891150c6e |
| 5 | 171109 | 15de2d6aadddc43bf5111277c12f31f23d7fcc558b6c3c0d2a345c4110183e14 |
| 6 | 119024 | 2cb71f7b9c9286848e98413f912f269fe1c3596fbdd20ab90f89386756062249 |
