# API 契约与迁移基线（C02/C03）

核对日期：2026-10-17。范围为 `FAIR_SHARE_MODE=true` 的当前共享网关，包含 Native、OpenAI 风格生图、成员门户、任务与结果接口；不是 NovelAI 全量 API 的兼容声明。下文记录当前分支行为；F03 故障处理、F06 持久请求/可选幂等、F10 结果恢复、F12 冷却、A01 持久会话及 R01 细粒度事件已接入。Nuxt 与独立执行器的迁移边界和实际验证见[执行服务](executor-service.md)，生产仍未切换。

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
| `/ai/generate-image` | 完整成功 `201`，按 Accept 选择 ZIP/JSON；ZIP 文件为 `image_0.<实际扩展名>` 等；部分成功保留 `200` 与批次说明 | C03-01 对齐普通成功状态和 JSON 协商；`images` 元素为 base64 `image`、`index` 和实际 `seed` |
| `/ai/generate-image-stream` | `200 application/octet-stream`；等待批次处理后返回长度分帧 MessagePack；没有逐步实时预览事件 | 官方路径列 `200 text/event-stream` 和 intermediate/final/error；同时 `image.StreamingType` 明确包含 `msgpack`、`sse`，不能断言 MessagePack 本身非官方 |
| `/ai/encode-vibe` | `201 application/binary`，保留编码原始字节，不套用 JSON 协商 | C03-01 对齐官方成功状态与媒体类型 |
| `/ai/upscale` | `200`，按 Accept 选择单图 ZIP/JSON；JSON 不虚构放大过程的生成种子 | C03-01 对齐 ZIP/JSON 协商；官方 `seed` 字段并非必填 |

四种操作成功响应均 `Cache-Control: no-store`。普通/流式生图带 `X-Nai-Image-Count` 和 `X-Nai-Partial`；辅助操作没有这两个批次头。Native 请求在初步身份验证后获得网关生成的 `X-Nai-Diagnostic-Id`。持久受理后返回 CORS 已暴露的 `X-Nai-Request-Id` 父请求 ID，超时/部分失败响应也保留；受理前拒绝没有父 ID。诊断关联不是幂等保证。

MessagePack 帧是「4 字节大端帧长 + 一个 MessagePack 对象」。成功对象为 `{ event_type: 'final', samp_ix: 0..3, image: <bin> }`；部分失败的末帧为 `{ event_type: 'error', samp_ix: <失败序号>, message: <文本> }`。`image` 是二进制类型，不是 base64 字符串。此协议由[独立解码器](../tests/helpers/native-stream-client.mjs)检查。

多图 `n_samples=1..4` 一次持久受理全部单图意图：首张 queued，其余 pending；前一张成功结算时，下一张在同一事务激活并排到当时队尾。第 i 张种子是 `(初始种子+i) mod 2^32`。实际失败或未知结果终止剩余未派发图片；首张失败返回错误，已有成功图时返回 `200` 与部分结果。普通 ZIP 在请求多于一张时额外包含 `results.json`，字段为 `requested/completed/remaining/billing/results`，其中 `billing='independent_single_images'`；JSON 将同一说明放在顶层，`images` 只含已完成图片；流式路径用上述 error 帧表达失败。

Native 客户端断线或 HTTP 等待超时只结束等待，已受理且业务期限仍有效的批次继续。HTTP 超时时，若尚无图片返回 504，已有图片则沿用部分结果格式；这不表示其余图片一定已终止，随后可在成员任务记录领取。排队期限从整批提交起计算，激活下一张不重置；运行后使用执行超时。有效 queued/pending 在重启后恢复；未知的旧 running 不重发，并终止剩余图片。停止接单原子取消 queued/pending，在途图片保持结算资格，恢复接单不复活已取消任务。

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

### C03-01 响应协商与客户端证据

N03 验收后才开始本项；2026-10-17 重新取得的官方 Swagger 字节数和 SHA-256 与上面的 C02 基线相同。普通完整生成从200改为201、Vibe从200/octet-stream改为201/binary；上段C02的旧输出描述是当时事实。Native stream、direct、OpenAI 与任务领取的成功状态在本项保持原协议。

普通生成和放大使用 [negotiator 1.1.0](https://github.com/jshttp/negotiator/tree/v1.1.0) 处理 Accept 的媒体类型、参数、优先级和通配，不自行实现解析器。候选按 ZIP、`application/json; charset=utf-8` 排列，缺省或仅通配保持 ZIP；明确 JSON 或较高 JSON 权重才选择 JSON。两种都不接受时在入队前返回406，不预扣、不建立新任务。响应带 `Vary: Accept`、`Cache-Control: no-store`；同一成员同键重取可以改变 Accept，不重复生成或结算。

JSON 图片种子来自首次派发或持久任务中的实际规范化值，随机种子和重启后的结果保持一致。编码按48 KiB原始字节分块转base64，逐张读取已有文件并使用Node流背压，不构造整批base64字符串；首图读取失败仍返回正常错误，发送过程中后续文件失败则中断响应，不能把残缺JSON当作完整结果。下载断线不取消或退款已完成的任务。

| 已核对的固定版本 | 成功判定与响应要求 |
| --- | --- |
| [NovelAI Image Desktop 2.5.9](https://github.com/2786886095/novelai-image-desktop/blob/40dcf055a27d812fcbcda5a1220399135038a910/electron/ipc/nai.ts#L2279-L2338)，另复核样本对应2.5.7 | 普通生成使用Axios默认2xx判定，显式Accept ZIP/octet-stream并按二进制解码；Vibe按原始字节读取，不能因为其Axios默认Accept包含JSON就改变Vibe格式 |
| [LittleWhiteBox](https://github.com/RT15548/LittleWhiteBox/blob/9e71caef9ba7b3ca324ce9f1cb06a69ff54d734b/modules/draw/providers/novelai/novel-draw.js#L1665-L1690) | 浏览器直连使用response.ok且未设置Accept；[服务端代发](https://github.com/RT15548/LittleWhiteBox/blob/9e71caef9ba7b3ca324ce9f1cb06a69ff54d734b/server-plugin/littlewhitebox-server/providers/novelai/client.js#L194-L200)接受200–299。两种都解析ZIP/图片，缺省不能改成JSON |
| [Aaalice NAI Launcher](https://github.com/Aaalice233/Aaalice_NAI_Launcher/blob/f92ead236506e61812423ef68d70bffa9e671a1b/lib/data/datasources/remote/nai_generation_transport.dart#L120-L135) | 使用Dio默认2xx判定和通配Accept，普通生成解析ZIP；[Vibe](https://github.com/Aaalice233/Aaalice_NAI_Launcher/blob/f92ead236506e61812423ef68d70bffa9e671a1b/lib/data/datasources/remote/nai_image_enhancement_api_service.dart#L177-L205)直接读取字节，无严格200或媒体类型检查 |

以上为源码与模拟HTTP证据，不是对全部历史客户端的认证。W.O.F 缺少可对应版本的公开源码；LittleWhiteBox 的 Vibe 调用本轮也未取得可靠源码证据，不据此宣称已验证。

C03-01 源码 [238421e](https://github.com/Hime-Hina/novelai-shared-gateway/commit/238421e) 的
[Linux CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/39008740184) 已通过后端482、网页112、执行器认证3、开发与双进程14、最终镜像HTTP3和Caddy7项，均无跳过；语法、类型及三个业务镜像构建也通过。新增检查实际种子回绕、部分JSON与ZIP说明一致、零历史缓存重取、缺文件410和406无受理，并保留幂等派发/结算计数；JSON断开测试不声称精确中断在某个数据块。测试全部使用合成数据和模拟上游，生产未切换。

### C03-02 流式证据（待实现）

2026-10-17 取得的[官方网页静态包](https://novelai.net/_next/static/chunks/0wi7fejx317_y.js?dpl=b50892a-38995131626-1)为113682字节，SHA-256为`d416909d4d4c2f249bc5d1045c4790445accb9de3cce34830f8085e41a760769`。其中`requestStream`主动设置`parameters.stream="msgpack"`，该处未设置Accept；按4字节大端长度解码MessagePack，图片按字节处理。它读取以下字段，结束时检查每个样本是否收到最终图：

| 事件 | 官方网页读取的字段 |
| --- | --- |
| intermediate | image、samp_ix、step_ix |
| final | image、samp_ix |
| error | message、samp_ix |

进度由客户端结合步骤、采样器和样本数量计算，不将门户的`progress/preview`字段当作Native协议。以上为当前官方客户端实现证据，不等于稳定API字段承诺。

Swagger同时描述SSE和msgpack/sse枚举，但没有逐事件schema。[Desktop的SSE解码器](https://github.com/2786886095/novelai-image-desktop/blob/40dcf055a27d812fcbcda5a1220399135038a910/electron/ipc/nai-stream.ts#L92-L148)明确用于兼容代理，接受事件名加JSON/base64等形状；不能用它证明官方SSE字段、结束标志或Accept与stream冲突时的优先级。C03-02尚未实现或验收，本次C03-01不改Native流式帧。

## 错误与安全边界

Native 非成功响应通常为 `application/json; charset=utf-8` 的 `{ message: <文本>, error: <同一文本> }`。不是官方 `utils.JsonError` 的 `message/statusCode/details` 原样透传。路径不在共享白名单时由通用处理器返回 404，错误外形可能是通用 `{error}` 或 `/v1/` 的 OpenAI 外形，不能假设所有 `/ai/` 未知路径都使用 Native 外形。

| 状态 | 当前主要触发条件 |
| --- | --- |
| 400 | 非法 JSON/对象、缺失或错误的 prompt/parameters、multipart 结构不完整 |
| 401 | 缺少/无效成员 Bearer Key、停用成员；门户会话未登录 |
| 402 | 本地成员可用额度不足或购买 Anlas 授权/余额条件不满足（依具体核价路径） |
| 403 | Discord 资格/成员分组权限、访问其他成员结果、同源或 owner 权限失败；F04 停止接单后的新生成 |
| 404 | 共享模式未开放的路径/方法、任务或结果不存在 |
| 406 | 普通生成或放大的 Accept 不接受 ZIP/JSON；受理前拒绝 |
| 409 | 任务未完成、管理状态冲突、分组版本缺失/过期；已取消的等待任务；同成员同操作同幂等键的有效参数冲突；不是成功生成返回值 |
| 410 | 幂等映射仍在，但原任务、所需子记录或已完成产物已不可领取；不重新生成。direct 沿用 200 错误图片表达 |
| 413 / 415 / 422 | 请求过大 / 不支持的 Content-Type / 参数、图片、参考或未核价功能不合法 |
| 429 | 本地限流、成员/全局未完成任务上限或被识别的上游并发容量拒绝；有持久冷却证据时 `Retry-After` 为响应时剩余秒数向上取整，缺少证据或已到期时省略，不再固定写 30 |
| 499 | Native 批次在无成功产物时检测到客户端断开；通常连接已无法接收该响应 |
| 500 | 普通上游生成失败、坏产物、内部失败；上游 400/401/402/403/5xx 不保证原样返回，当前多数转换为 500 |
| 503 / 504 | 本地共享账户/账本不可用 / 同步等待或队列期限超时；Native/OpenAI 默认等待上限 10 分钟，可由环境配置改变 |

上游 401/403 也会使共享账户进入对应异常状态；成员错误不能暴露 PST。请求仍受本地单账户并发和钱包预扣约束，不能因为请求字段在官方文档存在就绕过它们。CORS 预检允许 `idempotency-key`，用于下述可选网关去重；`x-correlation-id` 仍是上游诊断字段，没有官方幂等/结果查询保证。

F12 在收取上游响应头时将有效 `Retry-After`（秒数或 HTTP 日期）换成绝对截止，正文中断或产物校验失败也保留该证据；本地 120 秒退避上限不截短更长的上游要求。尚未发送响应头的普通失败也可能带剩余等待头，direct 仍保持 200 错误图片格式；已开始的 SSE 不能事后改写响应头。同步等待超时只结束等待，不凭空新增任务冷却。HTTP 200 流内的明确并发错误仍按容量拒绝分类，真实状态保留用于诊断。

共享状态 SSE 增加白名单 `cooldownReason`，与已有 `cooldownUntil` 配套；到期或未知原因返回 null，不返回逐秒变化的剩余值。原因是 `success_gap`、`ordinary_failure`、`capacity_limit`、`upstream_retry_after`、`completion_gap`。客户端本地倒计时，归零仅表示等待期到达，仍以服务端资格检查和状态更新为准。

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
| `GET /api/jobs/:id` | 仅所属成员或管理员读取 `200 JSON`；核心字段包含 id/source/status、模型/步数/队列位置/进度、imageId/imageUrl、error 与时间；当前状态 pending/queued/running/done/failed；pending 等待前序图片，不占派发位置 |
| `GET /api/jobs/:id/content` | 完成时返回结果原始媒体类型；未完成409；失败返回200 SVG错误图、`x-error:1`；跨成员403 |
| `GET /api/images/:id/content` | 所属成员、管理员或有效限时签名才能读取；不存在404、无权403。公网签名有效期15分钟且进程重启失效；签名本身是可转交的访问凭据 |
| `GET /api/jobs/events` | 成员 Key 的 SSE 预览订阅，消息是 `{jobId,progress,preview}`；重连只能取得仍在内存的最新预览，没有完整事件历史 |

OpenAI 非流式/SSE 与 direct 的 HTTP 等待期限只控制连接：direct 默认 60 秒，OpenAI 默认 10 分钟；超时分别沿用错误图片、504 JSON、含超时文字与 `[DONE]` 的 SSE。断线、等待超时或返回图片失败不取消、退款或重建已受理任务。有效的旧 inline 与新父请求 queued 任务在重启后继续；旧 running 仍按未知结果规则处理，不自动重发。新任务的排队期限与执行超时独立于 HTTP 等待期限，已完成结果可从成员任务记录领取。

OpenAI SSE 进度轮询仅读取 child 小摘要，断线立即清理本连接等待者和定时器；普通 `GET /api/jobs/:id` 继续回显完整提示词。direct 明确缓存命中仍返回 `x-cache: hit`，不会为了重建连接重新生成已有结果。

以上接口不实现 OpenAI Images、文本模型推理或聊天工具调用语义。共享模式还明确不开放官方的 `augment-image`、`suggest-tags`、`/ai/generate`、`/ai/generate-stream`、`/oa/v1/*`、`/user/data` 和账户登录/持久令牌管理；返回404不是透传失败。旧兑换/合并、图库管理、整库导入导出等也被共享路由白名单挡住。

## 门户与管理的迁移边界

成员 `/api/member/*` 使用 `nai_member_session` Cookie，`HttpOnly/SameSite=Strict/Path=/api/member`，非本机环境附 Secure；状态变更要求本站 Origin，Discord GET 回跳用独立 OAuth state 校验并303回首页。A01 会话摘要保存在 SQLite：普通成员固定 7 天、owner 固定 12 小时，Cookie 的 Max-Age/Expires 对应同一服务器截止，GET 不续期或重发 Cookie。重启保留有效会话；退出、版本撤销、资格失效及 owner 归属变化按[会话规则](member-sessions.md)处理。旧内存会话无法迁移，首次升级需重新登录。

密码登录成功回包增加 `expiresAt`（整数 Unix 毫秒），`GET /api/member/me` 增加 `sessionExpiresAt`；JSON 正文不回传会话令牌或摘要。SSE 建连时仍使用正常 HTTP 鉴权；连接内确认资格/会话失效发送 `auth-expired` 并结束，Discord 503 或网络错误只中断连接。页面在 SSE 永久关闭时最多补一次状态鉴权检查，401/403 清除密钥展示并显示重新登录，503 保留页面。

`me/key/shared-status` 返回 JSON；`me` 包含自己的 quota 和最近30条任务，`key` 返回自己的 apiKey/baseUrl。`GET me` 的额度同步及 Discord 资格检查有持久副作用，Nuxt 迁移不能把 GET 简单当作可直接读 SQLite 的接口。

`shared-events` 保留 `event: shared-status`，R01 增加提交后的 `state-invalidated`：只含 `bootId/version/scopes/updatedAt/resync`。200ms 合并、持续变化最多等待1秒；每次重连主动 resync，版本允许因权限过滤跳号。scopes 为 shared、本人 member、已鉴权 owner，不能据失效事件取得其他成员数据。每2秒权限/时间边界检查和15秒保活保留；会话失效发 `auth-expired` 后关闭，每成员最多3连接、全局100连接。成员与管理摘要 GET 只在副本估算额度，不触发账本写入或通知循环。详见[状态事件](state-events.md)。它与 Native 生图流、OpenAI SSE、任务预览 SSE 是四种不同用途，不可互换。

owner 管理需会话和角色，管理员路径需本机隧道与管理员凭据；用户管理、分组、额度、账户凭据维护依现有管理接口执行。F09 后分组预览与保存必须带编辑开始时的整数 `expectedConfigVersion`，缺失/过期409。共享白名单允许的 OPTIONS 不代表该身份有操作权限；浏览器成员接口还执行同源检查。

F04 为这两个成员管理入口新增 `action='member-admission'`、`userId` 与布尔 `acceptingJobs`。停止接单在同一事务设置禁止状态、取消未派发任务并退款；已取得派发资格的 running 任务仍可正常完成。恢复不会复活已取消任务或旧 Native 批次剩余图片，登录与历史查询不受该状态影响。成员 `me` 和管理员成员列表新增 `acceptingJobs`；取消任务仍使用现有 `failed` 状态。默认成员/全局未完成上限分别为4/16，排队等待十分钟，参数和当前只计已创建子任务的边界见 [README](../README.md#额度语义与限制)。

F03 在相同管理入口新增运营份额划入、明确来源注资登记、关联损耗核销与聚合返回纠正四种操作，字段与资金语义见[失败退款与资源核对](failure-accounting.md#资金来源与核销)。同一收据同一操作可重试，冲突为 409；来源固定为配置中的 owner，不能指定其他成员的钱包。它们是管理扩展，不改变 Native/OpenAI 生图响应，也不增加补偿资金准入门槛。

F03 的任务查询新增 `lateResultAvailable`。失败退款后找回完整产物，任务仍保留 `failed`、原失败时间和退款，既有 `imageId` / `imageUrl` 可领取结果，不再补扣；重复完成不会保存两份结果。成员 `me.jobs` 同时新增 `lateResultAvailable` 和 `imageId`。五分钟后台核对从首次未知时间固定计时，重启不重置，上游离线也会自动估计结案；它与派发冷却分开，不改变已返回的 Native/OpenAI 响应。

管理入口的 `GET ?view=losses&before=<可选游标>` 提供按资源分开的候选风险、聚合新增差额、运营余额、软告警及每页 50 条审计记录；查询覆盖 SQLite 冷历史，不把不同证据相加为实耗。`POST action=loss-alert-policy` 带 `expectedVersion` 和四项阈值，返回 `{policy}`；版本冲突为 409，配置与操作者审计同事务提交。阈值与原生成准入无关，完整字段见[失败退款与资源核对](failure-accounting.md)。

成员 `me.jobs` 增加签名 `imageUrl` 和退款分池 `charge.fixed/purchased`；旧记录分池未知时为 `null`。`me.lateResults` 返回本人按找回时间排序的最近 10 个结果，覆盖冷历史。签发前同时核对当前启用成员、任务与图片归属，不扩大登录 Cookie 路径或在 URL 中传成员 Key；领取链接沿用 15 分钟有效期，刷新页面可重新取得。

F06 网页 `POST /api/jobs` 保留原 `202`、任务查询、完整提示词回显和图片领取契约。新请求将参数与任务在同一事务持久化，执行时按需读取；旧内联排队记录仍可恢复。网页正文上限保持 2 MiB；Native JSON/multipart 正文上限保持 32 MiB。规范化父参数默认上限 32 MiB，活动载荷默认合计 64 MiB，单次超限为 413，暂满为 429。Native 完整父参数只存一份，各图片子任务不复制输入图片；multipart 展开后仍须满足规范化上限。

读取正文先临时占用预算，提交后移交持久计数，失败或断线释放临时计数；断线不释放仍被任务持有的父载荷。pending、旧内联活动参数和尚未 finish 的执行尝试也计入，按父请求去重。各入口的 queued 使用持久 `queueSeq`，Native 下一张激活才取得新号；pending 与缓存命中完成不占号。公开状态新增独立 pending 数，queued 仍表示可派发等待任务，不把未激活图片当作已占队列位置。

direct/OpenAI 同样保存完整父请求；OpenAI 保持 2 MiB 正文上限，direct 查询参数在解析受理阶段计入临时预算，认证 token 不进入父参数。各同步 handler 只携带等待所需的小元数据，受理后同样返回 `X-Nai-Request-Id`。没有稳定键的第三方重试仍无法可靠识别为同一操作。字节上限是输入预算，不是进程 RSS 保证。具体边界见[持久请求说明](request-lifecycle.md)。

成员生成入口可选提供单个 `Idempotency-Key`（1–128 字节可见 ASCII，不含空格）；缺省保持原行为，非法/重复头为 400。范围是稳定成员 ID + 操作 + 键，比较实际生成意图；相同键复用原任务/结果，参数冲突为 409，结果缺失为 410，不重新派发。direct 保持原错误图片格式。随机主种子和有限默认参数沿用首请求，换 Key 后映射仍属于同一成员；响应流式选项可改变交付格式。请求及已完成图片原有显式缓存规则保持，不能把不同操作键一概当作不允许缓存。详见[幂等边界](request-lifecycle.md#可选幂等键)。

### 父请求查询、领取与取消

`GET /api/requests/:id` 使用成员 Bearer；`GET /api/member/requests/:id` 使用成员 Cookie，两者返回相同 DTO。归属按稳定成员 ID 判断，本人轮换 Key 后仍能读取；其他成员或未知 ID 为 404。网页任务与 `me.jobs[]` 增加 `requestId`；旧 inline 任务为 null，继续使用原 job 接口。

DTO 为 `{id,source,operation,createdAt,status,cancellable,counts,jobs}`。counts 包含 `total/queued/pending/running/done/failed`，failed 包含取消项；jobs 每项包含 `id/index/status/model/createdAt/updatedAt/completedAt/error/cancelled/lateResultAvailable/imageUrl`，index 从零开始。父状态从子任务派生为 running、queued、pending、done、partial、cancelled、failed 或 empty，不额外保存一份状态。查询不返回完整参数、提示词或认证信息。图片归属校验后签发既有下载链接，迟到完整结果也可领取。

上述两条路径均提供 `POST .../:id/cancel`，JSON 正文 `{}`；Cookie 入口沿用同源 Origin/CSRF。返回同一 DTO 加本次 `cancelledCount`，重复取消为 0。只取消 queued/pending 并原子退款，running 继续完成，已完成图片保留。取消与派发串行提交，不会由迟到结果或前序成功重新激活余图。成员页提供批次详情、刷新、结果领取及取消按钮；管理员成员统计也单列 pending，不混入可派发 queued。

## N03 后的 C03 差异与待核实清单

| 编号 | 当前差异/不确定项 | 后续验收要求 |
| --- | --- | --- |
| C03-01 | 普通完整生成201、Vibe201/binary及ZIP/JSON协商已实现并通过Linux CI | 客户端版本证据、回归和新契约见上节；部分成功仍为网关扩展，不冒充官方原生批次计费 |
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
| [native-stream.test.mjs](../tests/unit/native-stream.test.mjs)、[native-adapter.test.mjs](../tests/integration/native-adapter.test.mjs) | 独立解码与既有适配回归；后者仍有源码截取测试，T01清理时应改为真实模块/HTTP。N03已另用正式Nuxt/执行器产物回归，没有照搬这种测试方式 |
| [shared-surface.test.mjs](../tests/unit/shared-surface.test.mjs)、[launcher-subscription.test.mjs](../tests/unit/launcher-subscription.test.mjs)、[member-routes.test.mjs](../tests/integration/member-routes.test.mjs)、[owner-management.test.mjs](../tests/integration/owner-management.test.mjs) | 路由权限范围、本地订阅映射、门户与管理状态码 |
| [executor-lifecycle.test.mjs](../web/tests/integration/executor-lifecycle.test.mjs) | 正式 Nuxt 与 Nitro 产物：Native 别名同键重放、32 MiB multipart 附件、Vibe 原始字节、OpenAI 首帧与预览、超过45秒仍完成、direct 原超时图片、任务权限与签名图、只杀 Nuxt 后执行器继续完成；不再注入测试专用 API 路由 |
| [executor-stream.test.ts](../web/tests/nuxt/executor-stream.test.ts)、[caddy-routing.test.mjs](../tests/deployment/caddy-routing.test.mjs) | Node 流背压、提前拒绝与关停；固定 Caddy 产物验证统一 Nuxt 入口、公私管理隔离、API 不压缩和 SSE 首帧转发。Caddy 用模拟 Nuxt，不代替上行真实业务测试 |

这些是当前基线的证据组合，不是“全部官方API契约通过”。OpenAI关键成功/流式路径已在本地隔离HTTP环境覆盖；公网代理的SSE缓冲/断连行为、真实上游非零blur/新模型组合和浏览器兼容性仍需相应迁移/测试部署阶段验收。不能以纯模块测试替代真实HTTP或浏览器证据。
