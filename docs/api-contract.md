# 迁移前 API 契约基线（C02）

核对日期：2026-10-17。范围为 `FAIR_SHARE_MODE=true` 的当前共享网关，包含 Native、OpenAI 风格生图、成员门户、任务与结果接口；不是 NovelAI 全量 API 的兼容声明。下文记录已经存在的行为；分资源失败退款、损耗记录与迟到结果领取已由 F03 接入，计划中的持久会话、完整批次受理、细粒度事件和幂等重试仍不能当作已经实现。

实现依据为 [HTTP 入口](../server/index.js)、[共享模式路由白名单](../server/shared-surface.js)、[输入解析](../server/native-image.js)、[multipart 解析](../server/native-body.js)、[核价](../server/native-pricing.js)、[上游适配](../server/providers.js)、[成员路由](../server/member-routes.js)。基线建立在首批安全修复之上；修改安全缺陷或实施已批准的需求时，可以更新相应基线，不能为了让测试通过而悄悄改变公开行为。

## 证据与使用边界

| 证据 | 能证明什么 | 不能证明什么 |
| --- | --- | --- |
| [Primary API 文档](https://api.novelai.net/docs/)指向的 [Image API](https://image.novelai.net/docs/index.html)、[doc.json](https://image.novelai.net/docs/doc.json) | 文档列出的路径、字段、媒体类型、状态码 | 所有模型的实际支持组合、当前账户价格、历史前端模块的持续有效性 |
| 当前源码与离线 HTTP/模块测试 | 网关实际接受、拒绝、转换和输出的行为 | 真实 NAI 会接受所有通过网关校验的参数 |
| 三个[脱敏客户端样本](../tests/fixtures/)：`novelai-image-desktop-request.json`、`littlewhitebox-v5-request.json`、`aaalice-v5-curated-request.json` | 桌面客户端别名、旧模板字段、V5 Full/Curated 请求结构的回归 | 对这些客户端所有版本、插件和功能的认证 |
| `official-image-pricing-20261009.json` 固定报价样本及其来源摘要 | 已核对样本对应的独立预期数值 | 上游逐笔实际账单；尤其 V5 百分点仍是估计 |

本次读取的 Image Swagger 为 2.0、`info.version=1.0`，原始响应 113991 字节，SHA-256：`77ee806124d7b97c60699617b7aba00f29e0287257cc665ea645753cc4eeea55`。测试不在线拉取文档；更新官方证据应另行审阅差异，不让外部变化自动扩大透传范围。

## Native 路径、状态与响应

以下四种生成操作均为 `POST`，各有同名 `/v1/ai/...` 别名。请求必须有 `Authorization: Bearer STA1N-...`；成员 Key 与共享上游 PST 是两个不同身份。Cookie 或请求体中的 Key 不能替代 Native Bearer 鉴权。共享公网入口还检查启用状态与配置要求的 Discord 资格。

| 操作 | 当前网关成功契约 | 官方定义及差异 |
| --- | --- | --- |
| `/ai/generate-image` | `200 application/zip`；文件为 `image_0.<实际扩展名>` 等；始终重打包 ZIP，`Accept: application/json` 不改变响应 | 官方列 `201`，提供 ZIP/JSON；JSON 结构为 `images` 数组，元素有 `image`、`index`、`seed` |
| `/ai/generate-image-stream` | `200 application/octet-stream`；等待批次处理后返回长度分帧 MessagePack；没有逐步实时预览事件 | 官方路径列 `200 text/event-stream` 和 intermediate/final/error；同时 `image.StreamingType` 明确包含 `msgpack`、`sse`，不能断言 MessagePack 本身非官方 |
| `/ai/encode-vibe` | `200 application/octet-stream`，保留编码原始字节 | 官方列 `201 application/binary`；对外状态/媒体类型差异留待 C03 |
| `/ai/upscale` | `200 application/zip`，一个图片文件 | 官方列 `200`，ZIP/JSON；网关不做 JSON 内容协商 |

四种操作成功响应均 `Cache-Control: no-store`。普通/流式生图带 `X-Nai-Image-Count` 和 `X-Nai-Partial`；辅助操作没有这两个批次头。Native 请求在初步身份验证后获得网关生成的 `X-Nai-Diagnostic-Id`。CORS 暴露头列表虽包含 `X-Nai-Request-Id`，当前代码未实际发送该头，客户端不能依赖它。诊断关联不是幂等保证。

MessagePack 帧是「4 字节大端帧长 + 一个 MessagePack 对象」。成功对象为 `{ event_type: 'final', samp_ix: 0..3, image: <bin> }`；部分失败的末帧为 `{ event_type: 'error', samp_ix: <失败序号>, message: <文本> }`。`image` 是二进制类型，不是 base64 字符串。此协议由[独立解码器](../tests/helpers/native-stream-client.mjs)检查。

多图 `n_samples=1..4` 拆成单图顺序入队：前一张成功后才创建下一张；第 i 张种子是 `(初始种子+i) mod 2^32`，每张重新排到当时队尾。首张失败返回错误；已有成功图时返回 `200` 与部分结果，并停止剩余张数。普通 ZIP 在请求多于一张时额外包含 `results.json`，字段为 `requested/completed/remaining/billing/results`，其中 `billing='independent_single_images'`；流式路径用上述 error 帧表达失败。当前未派发的剩余子任务不是一次性持久受理，连接断开可能停止后续创建；F06 会专门修正并更新生命周期测试。

### 输入范围与规范化

JSON 与 multipart 总请求上限均为 32 MiB。multipart 必须只有一个 `request` JSON 字符串或 JSON 文件部件；附件只能是被引用的 `image`、`mask`、`reference_image`、`ref_multiple_0..99`、`director_ref_0..99`，不能有未引用、重复或未知部件。最多 36 个总部件，每个附件非空且不超过 8 MiB。它们被展开成 base64 后执行相同输入校验；附件名不是服务器下载 URL。

| 类别 | 已支持的网关范围 | 拒绝或转换 |
| --- | --- | --- |
| 顶层 | `input/model/action/parameters/use_new_shared_trial/allowPurchasedAnlas` | 未知字段（含官方 `url`）返回 422；`input` 必须是非空字符串且最多 50000 字符，`parameters` 必须是对象，否则 400 |
| 动作 | `generate`（默认）、`img2img`、`infill` | 未知动作 422；重绘按模型映射到 `-inpainting` 名称，5C 重绘明确拒绝，不自动替换为 4.5 |
| 模型 | V3/Furry V3、V4 Curated Preview/Full、V4.5 Curated/Full、V5 Full/Curated/Full Medium；对应的已列入白名单重绘模型（不含 5C） | 其他名称 422。Swagger 的 model 是字符串，不代表任意名称都有本地计价与路由 |
| 尺寸、步数、张数 | 宽高各为 128..2048 的整数，总像素 ≤3145728；步数 1..50；张数 1..4（缺省 1） | 不是任意数值钳位。Medium 经专用规则归一到 14 步、`k_euler_ancestral`、`cfg_rescale=0` 及固定负面提示处理 |
| 种子 | 安全非负整数取模 `2^32`；省略或 -1 随机确定一次 | 小数、低于 -1、不安全整数 422；多图只在此初始种子上递增 |
| 采样器 | `k_euler`、`k_euler_ancestral`、`k_dpmpp_2m`、`k_dpmpp_2m_sde`、`k_dpmpp_2s_ancestral`、`k_dpmpp_sde`、`k_dpm_2`、`k_dpm_2_ancestral`、`k_dpm_adaptive`、`ddim`、`ddim_v3`、`k_lms` | 默认 `k_dpmpp_2m_sde`；名单表示网关校验接受，并非对每个模型做过上游组合验证 |
| 引导/调度 | `scale=0..10`，`cfg_rescale=0..1`；调度为 native/karras/exponential/polyexponential，默认 karras | 负面提示 `negative_prompt/uc` 最多 50000 字符；缺少前者时取后者 |
| 输出/流 | 可省略或 `image_format='png'`；客户端 `parameters.stream` 被移除 | WebP 输出明确 422；客户端 stream 的值不决定出站媒体类型；V4/V5 上游默认请求 msgpack |
| 图生图/重绘 | PNG/JPEG/WebP base64 输入，图片实际尺寸必须等于请求尺寸；strength/noise 0..1；重绘要求同尺寸 PNG mask | 不下载图片 URL；遮罩仅在 infill 有效；图生图/重绘不允许 SMEA；文生图无效的默认 strength/noise 滑块会清理 |
| 多角色 | `v4_prompt/v4_negative_prompt` 的 caption/char_captions；最多 24 个角色，每角色最多 16 个中心点 | 坐标 0..1；关闭坐标布局时缺失中心分量补 0.5；`characterPrompts` 只是 UI 元数据，校验后删除；V3 拒绝多角色结构 |
| SMEA | V3 文生图的 `sm/sm_dyn`，DYN 必须同时有 sm | autoSmea 启用、其他模型启用、图生图启用均 422 |
| Vibe | V3 原图；V4/V4.5 预编码数据；最多 16 个参考，数量与强度对应；旧单图字段归一成数组 | 编码输入限制 2 MiB，基本形态校验不是内部语义验证；V5、重绘、与 Precise 混用被网关拒绝 |
| Precise Reference | V4.5；最多 16 张；1024×1536 / 1536×1024 / 1472×1472；描述与参考数组一一对应 | 其他模型或与 Vibe 混用 422；强度当前接受 -1..1、secondary -1..2，较 Swagger 的文字说明宽，留待核实 |
| 缓存字段 | cached 形式必须同时给出 data，进入核价后转换为普通数组 | 不信任 `cache_secret_key` 可以免传数据或免费；图片缓存密钥字段被删除；普通和 cached 同时启用拒绝 |
| 客户端别名/空模板 | `uc_preset→ucPreset`、`quality_toggle→qualityToggle`；两名并存必须一致；关闭的 ControlNet/自动放大/LoRA 模板字段可清理 | 启用的未知功能不得透传；启用 ControlNet、LoRA、内联 upscale/upscaled_enhance 返回 422 |

其他已列入白名单的提示/兼容标志包括 `params_version`、预设 ID、legacy 系列、dynamic_thresholding 系列、`skip_cfg_*`、`uncond_scale`、`cfg_sched_eligibility`、`deliberate_euler_ancestral_bug`、`prefer_brownian`、`straight_alpha`、`tag_hint_*` 等；仍逐项检查类型或范围。完整字段与约束以 [native-image.js](../server/native-image.js) 为准，不能仅因为存在于 Swagger 就跳过校验。官方 `parameters.prompt` 当前也属于未知参数，实际正向文本使用顶层 `input` 和已验证的角色结构。

所有图片输入默认限 8 MiB、边长 ≤8192、≤16777216 像素。PNG 检查块 CRC 与实际解压扫描线；JPEG/WebP 是结构检查，不声称完整像素解码。放大和编码另有下述操作约束。

### 辅助操作

| 操作 | 接受字段及默认值 | 明确拒绝 |
| --- | --- | --- |
| encode-vibe | `image/model/information_extracted/allowPurchasedAnlas`；信息提取比例 0..1，默认 1；模型限 V4/V4.5 四种基础模型 | `mask`（即使为空）、`crop_to_mask`、`focus_seed`、`info_extract_seed`、其他模型/未知字段 |
| upscale | `image/model/declared_blur_sigma/scale/width/height/allowPurchasedAnlas`；默认模型 `nai-diffusion-5-curated`、blur=0；旧尺寸声明必须与图片一致，scale 如提供只能是 2 | blur 非 0、其他模型、伪造尺寸、输入总像素超过 3145728、未知字段；官方列出的非零 blur 档位尚未适配 |

F07 后上游产物在成功结算前验证：图片可从二进制、JSON 或 ZIP 取回；ZIP 检查目录、CRC 与解压边界；流必须有合法最终图片且帧/HTTP 传输完整。结果图片边长上限为 16384，允许已接受的8192边长输入产生两倍放大结果；输入仍限8192，结果总像素仍限16777216。单产物上限 64 MiB，msgpack 流累计上限 256 MiB，不把 HTML/空响应/预览当作成功。Vibe 上游同时接受 `application/binary`（Swagger）和 `application/octet-stream`（现有适配），拒绝明显错误文本；不对其套用图片校验或虚构编码魔数。

建立 C02 时发现 F07 首版曾仅接受 octet-stream，错误排除了官方 application/binary；已补双类型回归，保持客户端既有 octet-stream 输出。这是修复误拒绝上游成功响应，不是提前开展 C03 的对外协议变更。

## 错误与安全边界

Native 非成功响应通常为 `application/json; charset=utf-8` 的 `{ message: <文本>, error: <同一文本> }`。不是官方 `utils.JsonError` 的 `message/statusCode/details` 原样透传。路径不在共享白名单时由通用处理器返回 404，错误外形可能是通用 `{error}` 或 `/v1/` 的 OpenAI 外形，不能假设所有 `/ai/` 未知路径都使用 Native 外形。

| 状态 | 当前主要触发条件 |
| --- | --- |
| 400 | 非法 JSON/对象、缺失或错误的 prompt/parameters、multipart 结构不完整 |
| 401 | 缺少/无效成员 Bearer Key、停用成员；门户会话未登录 |
| 402 | 本地成员可用额度不足或购买 Anlas 授权/余额条件不满足（依具体核价路径） |
| 403 | Discord 资格/成员分组权限、访问其他成员结果、同源或 owner 权限失败；F04 停止接单后的新生成 |
| 404 | 共享模式未开放的路径/方法、任务或结果不存在 |
| 409 | 任务未完成、管理状态冲突、分组版本缺失/过期；F04 已取消的等待任务、停止再恢复后的旧批次下一张；不是成功生成返回值 |
| 413 / 415 / 422 | 请求过大 / 不支持的 Content-Type / 参数、图片、参考或未核价功能不合法 |
| 429 | 本地限流、成员/全局未完成任务上限或被识别的上游并发容量拒绝；Native/OpenAI 错误处理当前写固定 `Retry-After: 30`，F12 才改为实际剩余冷却 |
| 499 | Native 批次在无成功产物时检测到客户端断开；通常连接已无法接收该响应 |
| 500 | 普通上游生成失败、坏产物、内部失败；上游 400/401/402/403/5xx 不保证原样返回，当前多数转换为 500 |
| 503 / 504 | 本地共享账户/账本不可用 / 同步等待或队列期限超时；Native/OpenAI 默认等待上限 10 分钟，可由环境配置改变 |

上游 401/403 也会使共享账户进入对应异常状态；成员错误不能暴露 PST。请求仍受本地单账户并发和钱包预扣约束，不能因为请求字段在官方文档存在就绕过它们。CORS 预检的允许头已列 `idempotency-key`，但当前并没有据此实现任务去重；`x-correlation-id` 是上游诊断字段，也没有官方幂等/结果查询保证。

## 本地核价、订阅映射与共享扩展

本地账本区分 V5 百分点、订阅固定 Anlas、购买 Anlas。报价是受理/预扣依据，成功才结算，失败按当前流程退款；这不表示 NAI 已退款。购买 Anlas 需要明确 `allowPurchasedAnlas=true`，还受当前余额与固定池耗尽等条件约束；该标志不转发上游。预算/运营损耗不能偷偷变成新的成员调用资格限制。

以下是独立固定期望，迁移测试不能调用被测报价函数来生成这些 expected：

| 单张案例（Opus 本地规则） | 固定期望 |
| --- | --- |
| V4.5 Full，1024×1024，28 步，无参考 | free，0 |
| 同尺寸 29 步 | anlas，21（估价） |
| V5 Full/Curated，1024×1024，23 步 | v5，约 0.057803468208092484 个百分点，`estimated=true` |
| 上例明确按本地 Anlas 续用核价 | anlas，26；不能据此证明上游消耗一定是 Anlas |
| V4/V4.5 预编码 Vibe 五个参考，标准免费生成 | 附加 2 Anlas；前四个参考无此附加费 |
| V4.5 标准免费生成一个 Precise Reference | 附加 5 Anlas |
| encode-vibe | 2 Anlas |
| upscale，输入≤1048576 / ≤1747627 / ≤2446678 / ≤3145728 像素 | 1 / 2 / 3 / 4 Anlas |

批次每张独立核价，不等于一次上游原生多图请求的价格；例如本地两张标准 V4.5 可各为免费，而保存的官方批量样本不是这个总价。完整核价边界已由 [native-pricing.test.mjs](../tests/unit/native-pricing.test.mjs) 的独立官网报价样本覆盖。

`GET /user/subscription`、`/v1/user/subscription`、`/ai/user/subscription`、`/v1/ai/user/subscription` 都返回 `200 JSON` 的本地映射：`tier/active/expiresAt/trainingStepsLeft/perks/usage`。固定/购买 Anlas 取调用者可用余额的非负整数，usage 表示本地 V5 可用量；不是共享账户原始余额，不公开 PST 或其他成员份额。活跃 Opus 还要求绑定账户、有效订阅与到期信息。不要把这个映射误当作完整 `/user/data` 透传。

## 其他现有客户端接口

| 类别 | 当前契约 |
| --- | --- |
| `GET /v1/models` | `200 JSON` 的 OpenAI 风格目录；包含基础/2K/4K 尺寸档和采样器组合，这是网关模型别名目录，非 NovelAI `/oa/v1/models` 透传 |
| `POST /v1/chat/completions` | 成员 Bearer Key；聊天文本必须包含「提示词、尺寸、提示词引导值、缩放引导值、画师串、负面提示词」标签，前四个非空，后两个标签必须存在；可用 `nai` 覆盖部分参数，但不免除标签校验；默认固定 28 步，Medium 后续归一为 14 步 |
| OpenAI 非流式 | `200 application/json`，`object='chat.completion'`，assistant content 为图片 Markdown 链接；`usage=1/1/2` 是占位值，不是真实文本 token 账单 |
| OpenAI 流式 | `stream===true` 时 `200 text/event-stream`；`chat.completion.chunk` 的 `choices[].delta.content` 输出队列说明和最终 Markdown，随后 stop 与 `[DONE]`；不是图片二进制流。已开始 SSE 后失败写入内容，不能再改 HTTP 状态 |
| OpenAI 建连前错误 | `{error:{message,type,param:null,code:type}}`；401/403 为 authentication_error、429 为 rate_limit_error、5xx 为 server_error，其余 invalid_request_error |
| `GET /generate` | 旧查询参数接口；单图、默认可缓存，`nocache=1/true` 禁缓存；成功直接图片，含 `x-cache`。生成错误通常以 `200 image/svg+xml`、`x-error:1` 返回；公网鉴权401/403例外。不要在迁移中误改为 Native ZIP |
| `POST /api/jobs` | 网页异步接口，JSON 受理返回 `202` 任务快照；共享模式不开放 `/api/web/jobs`。与 Native/OpenAI 的同步等待契约不同 |
| `GET /api/jobs/:id` | 仅所属成员或管理员读取 `200 JSON`；核心字段包含 id/source/status、模型/步数/队列位置/进度、imageId/imageUrl、error 与时间；当前状态 queued/running/done/failed |
| `GET /api/jobs/:id/content` | 完成时返回结果原始媒体类型；未完成409；失败返回200 SVG错误图、`x-error:1`；跨成员403 |
| `GET /api/images/:id/content` | 所属成员、管理员或有效限时签名才能读取；不存在404、无权403。公网签名有效期15分钟且进程重启失效；签名本身是可转交的访问凭据 |
| `GET /api/jobs/events` | 成员 Key 的 SSE 预览订阅，消息是 `{jobId,progress,preview}`；重连只能取得仍在内存的最新预览，没有完整事件历史 |

以上接口不实现 OpenAI Images、文本模型推理或聊天工具调用语义。共享模式还明确不开放官方的 `augment-image`、`suggest-tags`、`/ai/generate`、`/ai/generate-stream`、`/oa/v1/*`、`/user/data` 和账户登录/持久令牌管理；返回404不是透传失败。旧兑换/合并、图库管理、整库导入导出等也被共享路由白名单挡住。

## 门户与管理的迁移边界

成员 `/api/member/*` 使用 `nai_member_session` Cookie，`HttpOnly/SameSite=Strict/Path=/api/member`，非本机环境附 Secure；状态变更要求本站 Origin，Discord GET 回跳用独立 OAuth state 校验并303回首页。当前会话仍在进程内、固定12小时、重启失效；A01 的成员7天/owner12小时持久会话尚未实现，不能冻结旧缺陷阻止批准的变更。

`me/key/shared-status` 返回 JSON；`me` 包含自己的 quota 和最近30条任务，`key` 返回自己的 apiKey/baseUrl。`GET me` 的额度同步及 Discord 资格检查有持久副作用，Nuxt 迁移不能把 GET 简单当作可直接读 SQLite 的接口。

`shared-events` 当前每2秒检查并以 `event: shared-status` 发状态变化，15秒保活；会话失效发 `auth-expired` 后关闭，每成员最多3连接、全局100连接。它与 Native 生图流、OpenAI SSE、任务预览 SSE 是四种不同用途，不可互换。R01 的提交后细粒度失效通知、版本/resync 与合并更新是后续明确变更。

owner 管理需会话和角色，管理员路径需本机隧道与管理员凭据；用户管理、分组、额度、账户凭据维护依现有管理接口执行。F09 后分组预览与保存必须带编辑开始时的整数 `expectedConfigVersion`，缺失/过期409。共享白名单允许的 OPTIONS 不代表该身份有操作权限；浏览器成员接口还执行同源检查。

F04 为这两个成员管理入口新增 `action='member-admission'`、`userId` 与布尔 `acceptingJobs`。停止接单在同一事务设置禁止状态、取消未派发任务并退款；已取得派发资格的 running 任务仍可正常完成。恢复不会复活已取消任务或旧 Native 批次剩余图片，登录与历史查询不受该状态影响。成员 `me` 和管理员成员列表新增 `acceptingJobs`；取消任务仍使用现有 `failed` 状态。默认成员/全局未完成上限分别为4/16，排队等待十分钟，参数和当前只计已创建子任务的边界见 [README](../README.md#额度语义与限制)。

F03 在相同管理入口新增运营份额划入、明确来源注资登记、关联损耗核销与聚合返回纠正四种操作，字段与资金语义见[失败退款与资源核对](failure-accounting.md#资金来源与核销)。同一收据同一操作可重试，冲突为 409；来源固定为配置中的 owner，不能指定其他成员的钱包。它们是管理扩展，不改变 Native/OpenAI 生图响应，也不增加补偿资金准入门槛。

F03 的任务查询新增 `lateResultAvailable`。失败退款后找回完整产物，任务仍保留 `failed`、原失败时间和退款，既有 `imageId` / `imageUrl` 可领取结果，不再补扣；重复完成不会保存两份结果。成员 `me.jobs` 同时新增 `lateResultAvailable` 和 `imageId`。五分钟后台核对从首次未知时间固定计时，重启不重置，上游离线也会自动估计结案；它与派发冷却分开，不改变已返回的 Native/OpenAI 响应。

管理入口的 `GET ?view=losses&before=<可选游标>` 提供按资源分开的候选风险、聚合新增差额、运营余额、软告警及每页 50 条审计记录；查询覆盖 SQLite 冷历史，不把不同证据相加为实耗。`POST action=loss-alert-policy` 带 `expectedVersion` 和四项阈值，返回 `{policy}`；版本冲突为 409，配置与操作者审计同事务提交。阈值与原生成准入无关，完整字段见[失败退款与资源核对](failure-accounting.md)。

成员 `me.jobs` 增加签名 `imageUrl` 和退款分池 `charge.fixed/purchased`；旧记录分池未知时为 `null`。`me.lateResults` 返回本人按找回时间排序的最近 10 个结果，覆盖冷历史。签发前同时核对当前启用成员、任务与图片归属，不扩大登录 Cookie 路径或在 URL 中传成员 Key；领取链接沿用 15 分钟有效期，刷新页面可重新取得。

F06 首片仅接入网页 `POST /api/jobs`：保留原 `202`、任务查询、完整提示词回显和图片领取契约。新请求将参数与任务在同一事务持久化，执行时按需读取；旧内联排队记录仍可恢复。正文上限保持 2 MiB，超限为 413；规范化参数默认上限 32 MiB，新路径活动载荷默认合计 64 MiB，暂满为 429。读取正文先临时占用预算，提交后移交持久计数，失败或断线释放临时计数。Native/direct/OpenAI、多图批次与可选幂等键尚未由这一片改造，具体边界见[持久请求说明](request-lifecycle.md)。

F06 排队顺序子片为各入口实际创建的 queued 任务增加持久 `queueSeq`，重启后仍按该顺序派发；它只替换排序依据，不改变公开队列进度的字段与含义。缓存命中直接完成不占号。Native 尚未创建的后续图片意图仍须等待批次子片，不能据此承诺完整多图请求已可恢复。

## N03 后的 C03 差异与待核实清单

| 编号 | 当前差异/不确定项 | 后续验收要求 |
| --- | --- | --- |
| C03-01 | 普通生成200/固定ZIP、辅助操作媒体类型与官方定义不同 | 独立评估201、JSON协商和旧客户端兼容；不能用升级框架顺带切换 |
| C03-02 | Native stream 固定最终MessagePack，忽略客户端 stream，无实时预览 | 分开验证SSE/MessagePack内容协商、每事件结构、断流和部分失败；Swagger 路径与枚举表述需结合真实样本 |
| C03-03 | 官方 URL、WebP、parameters.prompt、Vibe mask/随机种子、upscale非零blur及内联放大尚未支持 | 每项先核验需求、参数作用、计价与安全边界，再明确实现或继续拒绝；URL不能直接下载 |
| C03-04 | 错误外形、上游状态转换、批次 results.json 和独立结算是网关现状 | 分开验证官方错误兼容与多图部分成功；保留必要的成员鉴权、账本与运营限制 |
| C03-05 | 模型/采样器组合及Precise强度范围缺少全组合真实验证；Swagger无完整模型枚举 | 不把单个客户端样本或前端字段等同官方保证，新增支持前取得可复现证据 |
| C03-06 | Vibe内部编码不可由公开schema验证；官方未给幂等生成/结果重取契约 | 维持opaque产物和任务证据，不能宣称任意断电后精确一次上游消费或凭correlation-id找回 |

C03 仅在 N03 完整迁移验收后实施。F01–F12、A01/R01 等已经批准的安全、账务和生命周期修复按各自计划推进，不必等待 C03；相应行为变更需要独立提交和回归，文档记录改变后的事实。

## 验证入口

新增 [api-contract.test.mjs](../tests/unit/api-contract.test.mjs) 使用固定客户端输入和独立常量，检查公开路由范围、归一化后的关键参数、明确拒绝及本地核价；不会启动服务、访问真实NAI或读取生产数据库。已有测试补齐外层和边界：

| 证据 | 覆盖 |
| --- | --- |
| [native-http.test.mjs](../tests/integration/native-http.test.mjs) | 隔离网关+模拟上游的真实HTTP，身份/所有权、订阅映射、MessagePack/ZIP、部分失败退款、并发与重启；含200 HTML不能成功结算；OpenAI模型目录、鉴权错误、非流式Markdown结果领取，以及SSE chunk/stop/[DONE]与图片领取 |
| [provider-features.test.mjs](../tests/unit/provider-features.test.mjs) | 请求转发、双Vibe二进制类型、上游错误、坏产物、限流读取、ZIP/MessagePack/SOCKS完整性 |
| [native-body.test.mjs](../tests/unit/native-body.test.mjs)、[native-image.test.mjs](../tests/unit/native-image.test.mjs)、[native-client-regression.test.mjs](../tests/unit/native-client-regression.test.mjs) | multipart、参数/图片拒绝边界、客户端模板与多角色布局 |
| [native-pricing.test.mjs](../tests/unit/native-pricing.test.mjs) | 保存的独立官网报价样本、免费/付费边界、参考附加费、批次区别与账本衔接 |
| [native-stream.test.mjs](../tests/unit/native-stream.test.mjs)、[native-adapter.test.mjs](../tests/integration/native-adapter.test.mjs) | 独立解码与既有适配回归；后者仍有源码截取测试，T01/N03迁移时应改为真实模块/HTTP，不照搬这种测试方式 |
| [shared-surface.test.mjs](../tests/unit/shared-surface.test.mjs)、[launcher-subscription.test.mjs](../tests/unit/launcher-subscription.test.mjs)、[member-routes.test.mjs](../tests/integration/member-routes.test.mjs)、[owner-management.test.mjs](../tests/integration/owner-management.test.mjs) | 路由权限范围、本地订阅映射、门户与管理状态码 |

这些是当前基线的证据组合，不是“全部官方API契约通过”。OpenAI关键成功/流式路径已在本地隔离HTTP环境覆盖；公网代理的SSE缓冲/断连行为、真实上游非零blur/新模型组合和浏览器兼容性仍需相应迁移/测试部署阶段验收。不能以纯模块测试替代真实HTTP或浏览器证据。
