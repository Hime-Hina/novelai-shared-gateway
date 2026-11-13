# 成员生成接口契约：能力、报价与受理

更新日期：2026-11-07，Asia/Taipei。契约版本：`1`。

状态：**已在重构分支实现并通过本轮验收；尚未发布到生产。** 本文是[当前重构计划](../plans/architecture-refactor-plan.md)第 8 节及 R03/R04/R07/R09 的唯一接口规范，完成依据统一记录在计划中。适用唯一执行器、单绑定 Opus、SQLite；多账号选路、多执行器及切库属于[后续计划](../plans/scaling-follow-up-plan.md)。[已有 API 契约](api-contract.md)继续约束 Native/OpenAI/direct 和旧网页入口，本文不改变它们的鉴权、状态码或请求格式。

本轮交付可供独立生成工作台使用的服务端接口、类型、样例和验收。完整官网前端、画布、云项目和素材库不在本契约中实施。编辑器草稿、选区和撤销历史留在编辑器；已受理请求、费用授权、扣退与产物归网关。

## 1. 端点与共同规则

| 方法与路径                                         | 状态               | 用途                                 |
| -------------------------------------------------- | ------------------ | ------------------------------------ |
| `GET /api/member/generation/capabilities`          | 新增               | 描述当前实际支持的操作/参数与版本    |
| `POST /api/member/generation/quote`                | 新增               | 只读规范化、报价和当前准入预览       |
| `POST /api/member/generation/requests`             | 新增               | 带费用确认和幂等键受理，返回 202     |
| `GET /api/member/requests/:requestId`              | 复用并添加兼容字段 | 持久任务/批次状态；原字段含义保持    |
| `POST /api/member/requests/:requestId/cancel`      | 复用               | 仅取消尚未派发 child；在途继续结算   |
| `GET /api/member/requests/:requestId/events`       | 新增               | Cookie 鉴权的状态/预览 SSE           |
| `GET /api/member/requests/:requestId/input`        | 新增               | 恢复本人的已提交输入与实际种子       |
| `GET /api/member/requests/:requestId/inputs/:slot` | 新增               | 按白名单槽位读取该请求的原输入附件   |
| `GET /api/member/jobs/:jobId/content`              | 新增适配           | 以 Cookie 领取已有结果，复用产物读取 |

- 身份只来自有效的 `nai_member_session` Cookie，继续检查启用状态和既有 Discord 资格。owner 在这些接口也只能代表自己生成和读取；不能通过正文指定其他成员。
- 复用现有同源检查：POST 必须带匹配的 Origin；GET 拒绝明确跨站 Origin/Sec-Fetch-Site。Cookie/Bearer 互不替代，新接口不接受请求体或 URL 中的成员 Key、PST、memberId、accountId 或 fleetId。
- 能力、报价、提交响应和输入/内容读取使用 `Cache-Control: private, no-store`；SSE 使用 `no-cache, no-transform`。不建立按 URL 共享的私人响应缓存。
- 新接口和预览心跳不延长登录期限；可见页面仍由已有 `/api/member/me` 活动访问续期。认证本身可能更新合法资格缓存或撤销会话，这不等于报价可以写钱包。
- 新 JSON DTO 带 `contractVersion: 1`；对已有请求查询只增加兼容字段，不删除现有 `id/source/operation/status/counts/jobs/cancellable`。
- 新接口的资源金额统一为十进制字符串，最多 12 位小数；单位分别是 V5 百分点、订阅 Anlas、购买 Anlas。拒绝负数、指数形式、NaN、Infinity、超过 64 字符或精度溢出的确认金额。旧接口的数值表示保持兼容。
- 本轮沿用 Native 的新意图生成语义：新幂等键代表一次新生成；同键才重放。参数复用不等于缓存命中，不增加新的缓存开关。

## 2. 共用输入和规范化

报价与提交使用同一个 `GenerationInput`：在当前 Native 请求对象的根部增加必填 `operation`，值为 `generate`、`upscale` 或 `encode-vibe`。参数仍位于现有位置，不再嵌一层不同结构。

| 操作          | 业务输入                                                                                                  | 规范化归属                                   |
| ------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| `generate`    | `input/model/action/parameters`，其中 action 为 generate/img2img/infill；`allowPurchasedAnlas` 默认 false | 现有 Native 生图解析、模型能力及 Medium 转换 |
| `upscale`     | 现有放大 body：根部 image、model、scale、width/height、declared_blur_sigma 及购买池授权                   | 现有辅助操作解析；按真实图片尺寸校验         |
| `encode-vibe` | 现有编码 body：根部 image、model、information_extracted、focus_seed/info_extract_seed 及购买池授权        | 现有辅助操作解析，不按图片产物验证编码结果   |

成员适配器只先取出 `operation` 和提交专用的 `confirmation`，其余对象传给对应 Native 解析器。未知业务字段继续拒绝；不能因为包装层改变而放宽旧模型限制。JSON 和 multipart 的同一输入必须得到相同规范化意图和报价。

两种 POST 均接受 `application/json` 与 `multipart/form-data`。multipart 的 `request` 部件放完整上述根对象，附件命名与引用复用 `readNativeBody`，包括根部/parameters 中的 image、mask 和既有参考数组；不改成让服务器下载任意 URL。

默认线传正文上限 32 MiB、单附件 8 MiB、总 multipart 部件 36，规范化后载荷仍受现有请求预算约束。实际限制来自配置并由能力接口返回。报价读取临时占用相同正文预算，结束即释放，不占活动任务预算；提交成功时在同一受理事务移交预算。不能全局放宽旧管理 JSON 的 32 KiB 或旧普通受理代理的 2 MiB 限制。

生图样例（合成输入，报价和提交的业务部分一致）：

```json
{
  "operation": "generate",
  "input": "synthetic prompt",
  "model": "nai-diffusion-4-5-full",
  "action": "generate",
  "parameters": {
    "width": 1024,
    "height": 1024,
    "steps": 23,
    "n_samples": 1,
    "seed": -1
  },
  "allowPurchasedAnlas": false
}
```

报价对随机种子只返回 `seedMode: random`，不承诺某个随机结果；实际 seed 在首次受理确定并持久化。同键重试沿用首次 seed/defaults。固定 seed 按原批次规则派生子图种子。指纹复用既有规范化执行参数/图片摘要逻辑，保留随机意图，不能把临时生成的 seed 当作用户改了请求。

## 3. 前端能力描述

`GET /api/member/generation/capabilities` 返回 200 JSON。描述当前网关真的可以接受的功能，不承诺复刻所有官方操作，不在线抓取官网生成字段表。

| 字段                    | 类型/含义                                                                                              |
| ----------------------- | ------------------------------------------------------------------------------------------------------ |
| `contractVersion`       | 整数 1                                                                                                 |
| `versions`              | 非空不透明字符串：`capabilities`、`normalization`、`defaults`、`pricing`、`billingPolicy`              |
| `operations`            | 当前开放的 generate/upscale/encode-vibe；每个操作的输入格式和产物媒体类型                              |
| `models`                | 按 operation 的模型 ID/标签、actions、参数范围/步长/枚举/默认/固定值、角色位置及参考能力               |
| `models[].requirements` | 明确的组合约束，例如 img2img 需要 image、infill 需要 image+mask，Vibe/Precise 互斥；按共同模型能力生成 |
| `models[].transforms`   | 必要转换的说明，例如 Medium 固定步数与负面词；包含可供 UI 展示的原因代码                               |
| `limits`                | 线传/规范化/附件限制、部件数、sampleCount、像素限制、现有队列上限                                      |
| `billing`               | 资源单位、V5 estimated 标记、购买池默认不授权、当前已启用的 fallback/免费政策可见信息                  |

`operations` 元素固定为 `{id, inputContentTypes, outputMimeTypes}`；`models` 以 `(operation, id)` 唯一。模型描述的最小形状如下，字段路径沿用 Native body，例如 `parameters.steps` 或辅助操作的根部 `scale`：

```ts
type FieldCapability = {
  type: 'integer' | 'number' | 'boolean' | 'string' | 'enum' | 'image' | 'array' | 'object'
  required: boolean
  min?: number; max?: number; step?: number
  values?: (string | number | boolean)[]
  default?: unknown; fixed?: unknown
  encoding?: 'base64'; mimeTypes?: string[]; source?: 'encode-vibe'
  minBytes?: number; maxBytes?: number
  items?: FieldCapability; properties?: Record<string, FieldCapability>
}
type ModelCapability = {
  operation: 'generate' | 'upscale' | 'encode-vibe'
  id: string; label: string
  actions: ('generate' | 'img2img' | 'infill')[]
  fields: Record<string, FieldCapability>
  features: { multiCharacter: boolean; coordinatePlacement: boolean; vibe: boolean; preciseReference: boolean }
  requirements: { action?: 'generate' | 'img2img' | 'infill'; when?: string; requires: string[]; excludes: string[] }[]
  transforms: { code: string; fields: string[]; description: string }[]
}
```

辅助操作的 actions 为 `[]`；涉及数组/对象的复杂编辑器消费已有请求类型，不由这个展示描述发明新的执行语法。fixed 表示服务端固定值，default 仅在缺省时采用；像素总量、参考数量和组合限制仍由共同规则作最终校验。

Vibe 的 `parameters.reference_image` 在 V3 是 `image`，内容为 PNG/JPEG/WebP 原图的 Base64；在 V4/V4.5 是 `string`，内容为 `encode-vibe` 产出的不透明二进制 Base64（`source: 'encode-vibe'`、MIME 为 `application/octet-stream`），不接受原图或 data URL。编码字节至少 100、最多 `limits.vibeEncodingBytes`；这是本地输入边界，不能据此判定任意二进制都能被上游执行。V3 图片使用 `limits.attachmentBytes`、`imageDimension` 和 `imagePixels` 限制。

`reference_image_multiple.items` 与单参考字段使用同一内容语义；`reference_image_multiple_cached.items.properties.data` 是相同的必需内容。cached 输入仍须携带 data，`cache_secret_key` 仅为旧客户端兼容输入，不代表网关会信任缓存或少计参考数量。`maxBytes` 是解码后的字节上限，数组的 max 是参考数量；JSON Base64 与 multipart 引用的分块内容按同一 parser 检查。G11 恢复 V3 原图时保留实际图片 MIME，恢复 V4/V4.5 编码时使用 `application/octet-stream`，不回传 cache secret。items/properties 只描述这些已有内容位置，不引入第二套校验器。

每条 requirements 仅在其 `action` 与输入动作相同（或没有 action），且 `when` 指向的 Native 字段已启用（或没有 when）时生效；生效后 requires 中每个字段必须启用，excludes 中每个字段必须未启用。when/requires/excludes 使用同一语义：缺省、null、空字符串、false 和空数组均未启用，非空数组以及其他值视为启用；对象包含空对象也视为启用，仍须通过字段自身的类型和结构校验。when 是一个字段路径，不是表达式或脚本。比如普通 Vibe 数组的条件规则只在该数组非空时排除 cached Vibe 与 Precise Reference；无参考输入不会被要求补上参考，普通/cached 数组各自可以单独使用，不能同时启用。

参数描述采用项目实际需要的类型和约束，不嵌可执行 JS 或新通用规则语言。它是共同规范化/能力定义的投影，服务端校验仍是权威；前端不能根据一份复制的模型表判断某功能已被支持。新模型或组合支持变化必须同时更新能力投影及对应校验/报价测试。

versions 由真实定义/相关配置版本或其规范摘要产生，不维护一套需要手工同步的第二版本。`defaults` 只覆盖影响规范化的默认值；`billingPolicy` 只覆盖执行/收费/准入相关政策。余额、当前队列长度、时钟变化不使这些版本变化。错误文案、布局和无关运营告警显示也不使报价版本失效。

Vibe 内容类型、来源及字节限制属于能力语义，进入现有 capabilities/normalization 摘要；旧报价用于新操作键提交时须重新确认。已受理操作仍按首次上下文和原 confirmation 重放，不因能力投影修正增加上游尝试或收费。

能力响应不返回上游账号 ID/PST、其他成员信息或逐步报价公式。当前上游是否暂停、余额是否够用通过 quote 的准入预览表达，不把“暂不可用”误报为“模型不支持”。

## 4. 统一报价

`POST /api/member/generation/quote` 接收 `GenerationInput`，不要求幂等键，返回 200 JSON。使用与提交相同的规范化、单图报价、分池和准入规则；在隔离快照上估算恢复，不改真实钱包、预留、任务、配置或领域事件，不请求上游生成/额度刷新。

参数合法但当前余额不足、限额已满或共享上游暂不可用时，仍可返回有效报价，`admission.allowed` 为 false，并在 reasons 中给出对应状态码/代码；认证失败或请求结构不合法仍直接返回 HTTP 错误。

完整的免费单图报价样例（版本值和时间为示例）：

```json
{
  "contractVersion": 1,
  "asOf": "2026-11-01T00:00:00.000Z",
  "versions": {
    "capabilities": "cap-v1",
    "normalization": "norm-v1",
    "defaults": "defaults-v1",
    "pricing": "price-v1",
    "billingPolicy": "billing-v1"
  },
  "requestFingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "effective": {
    "operation": "generate",
    "model": "nai-diffusion-4-5-full",
    "action": "generate",
    "width": 1024,
    "height": 1024,
    "steps": 23,
    "sampleCount": 1,
    "seedMode": "random"
  },
  "charges": {
    "perJob": [
      {
        "sampleIndex": 0,
        "resource": "free",
        "resources": { "v5": "0", "anlasFixed": "0", "anlasPurchased": "0" },
        "estimated": false
      }
    ],
    "total": { "v5": "0", "anlasFixed": "0", "anlasPurchased": "0" }
  },
  "admission": { "allowed": true, "reasons": [] },
  "warnings": []
}
```

`perJob` 按将实际派发的独立单图报价，`total` 按资源相加，不使用官方原生多图打包价。每项可增加原有 breakdown 及 `billingMode` 说明。V5 值明确是本地估算百分点；免费仍受免费次数/队列限制，不等于不受限。

`effective` 返回影响执行/收费的规范化小字段及明确的转换提示，不复制 base64 图片。warnings 使用稳定 code/message/field，指出参数被规范化的真实变化。首次实际 seed 和最终完整输入由受理后输入读取提供。

`admission.reasons` 元素为 `{code, statusCode, message, retryAt?}`；例如 insufficient_balance/402、free_image_limit/429、upstream_unavailable/503。此预览只描述 asOf，不占队列或冻结余额。报价是只读计算，不引入持久 quote 表、quoteId 或需要轮换的签名密钥。

## 5. 费用确认、受理与幂等

### 5.1 提交输入

`POST /api/member/generation/requests` 使用同一个 `GenerationInput`，根部额外提供 `confirmation`。必须有单个 `Idempotency-Key` 请求头，规则沿用现有 1–128 字节可见 ASCII；自有页面为一次明确提交生成一次键，传输失败重试继续携带原键、原输入和原确认。

在上面的免费样例根部添加以下 confirmation（示例金额均为零）：

```json
{
  "confirmation": {
    "requestFingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "versions": {
      "capabilities": "cap-v1",
      "normalization": "norm-v1",
      "defaults": "defaults-v1",
      "pricing": "price-v1",
      "billingPolicy": "billing-v1"
    },
    "maxPerJob": { "v5": "0", "anlasFixed": "0", "anlasPurchased": "0" },
    "maxTotal": { "v5": "0", "anlasFixed": "0", "anlasPurchased": "0" }
  }
}
```

每个资源字段必填，零表示未授权该资源的消费。`allowPurchasedAnlas` 即使为 true，也必须同时有购买池费用上限并满足既有官方固定池耗尽等条件；上限不能覆盖账务资格规则。只有上限而没有原购买池授权仍拒绝扣购买额度。

confirmation 是调用者同意的上限，不是服务器签发的价格证明。服务端把指纹、版本、金额均当作输入重新验证，绝不按客户端声称的价格扣款，也不把较高预算自动变成实际消费。前端以报价填充待确认值，不得在后台自动扩大上限。

确认金额先转为同一规范定点表示，再参与意图指纹；字段顺序和 `0`/`0.000` 等值写法不构成不同确认，版本字符串则按原值比较。提交不能在正文里附带其他未声明字段来覆盖服务端的规范化上下文。

### 5.2 校验次序与排队后变化

1. 鉴权、检查正文与幂等键格式；在受理事务内先查原持久映射。
2. 已有映射：使用首次规范化版本/上下文比较原业务意图和原 confirmation。同键相同返回同一 request/jobs；改变任何已确认版本、上限或购买授权为 409 idempotency_conflict。**不先用最新报价版本拒绝一个已经受理的重放。** 保留旧规范化版本的读取适配，已下架模型也可校验并领取已受理结果，不能再走当前模型准入或产生新派发。
3. 无映射：按当前定义规范化并计算指纹、报价和权限。指纹/相关版本不匹配为 409 quote_changed，费用超出单张/整批上限为 409 cost_limit_exceeded；均不创建任务或幂等映射。
4. 重新检查真实余额、免费次数、接单状态、队列和载荷；失败按当前原因返回 402/403/429/503。只有有效报价不能保证受理成功。
5. 原子保存完整规范化请求、服务端指定的 `clientContract: member-generation-v1`、confirmation、首次种子/defaults、幂等映射和 child；仍按既有规则在派发时预留，不提前预扣整批。协议来源不接受客户端覆盖；旧请求迁移保持原来源，不能补造一份用户未确认的授权。
6. 每个 child 派发前再次检查相关版本、当前收费路径及原上限。整批已结算本地消费加当前预留不得超过 maxTotal，单张不得超过 maxPerJob。已退款金额不算本地消费，迟到结果不补扣。
7. 排队后规则变化或收费路径需要未授权资源时，记录该 child 的明确失败 code，终止未派发余图；已成功图片和已结算金额保留。报价变化不修改已结算记录，也不自动提高预算、重试原 attempt 或从 V5 改扣未授权 Anlas。

V5/Anlas fallback 是互斥收费路径，不能把两者加起来显示成单次实际费用。报价展示当时路径；如果页面要授权其他可用路径，必须显式确认相应资源上限。未授权路径变化时按上述规则失败，由用户重新报价并以新意图提交。

新成员端点沿用 `(memberId, operation, keyHash)` 唯一性，在其意图指纹中加入规范化 confirmation。与旧协议偶然使用同一键但缺少/不同确认时返回 409，不能悄悄形成第二份任务；旧协议自己的指纹及同键跨交付格式重放保持不变。复用历史参数生成新图必须使用新键，不能用更新 confirmation 的方式修改原 request。

### 5.3 受理回执

首次受理与同键重放均返回 202 JSON，并带 `Location` 和 `X-Nai-Request-Id`。回执只含小字段，不携带图片或完整提示词；replayed 表示是否重放，status 是当前查询状态。

```json
{
  "contractVersion": 1,
  "requestId": "req_synthetic",
  "jobIds": ["job_synthetic_0"],
  "status": "queued",
  "replayed": false,
  "links": {
    "status": "/api/member/requests/req_synthetic",
    "events": "/api/member/requests/req_synthetic/events",
    "cancel": "/api/member/requests/req_synthetic/cancel",
    "input": "/api/member/requests/req_synthetic/input"
  }
}
```

已经受理但产物/目标历史确实缺失时，沿用缺失结果的 410 语义并返回原 requestId，不重新生成。断开页面或响应丢失不撤销已受理任务；Nuxt 不自动缓存并重发整份 32 MiB 正文，浏览器可使用原键重试。

## 6. 查询、预览、参数复用与领取

- **查询/取消**：复用现有成员 request 查询和取消。批次 status 保持 pending/queued/running/done/partial/failed/cancelled/empty；job 仍使用原状态和 cancelled 标志。添加 request 内的 `revision`、job 的稳定 errorCode、确认/实际本地费用摘要及 member content 链接。取消仍只影响 pending/queued，重复取消不再退款。
- **预览 SSE**：鉴权并校验归属后，先返回 `snapshot`（当前请求 DTO），后续 `state`（已提交状态）与 `preview`（jobId、进度、当前帧）；终态发送 `complete` 后关闭，客户端收到后主动关闭 EventSource。每 15 秒发送注释心跳并按现有会话/资格规则检查失效；确认失效发 `auth-expired` 后断开，不取消任务。
- **流的一致性**：订阅与初始快照要覆盖两者之间的提交，或用 revision 检查并补读；不能漏掉唯一一次完成通知。preview 沿用 JobPreviews 的单帧最大 512 KiB 原始图、仅保留最新帧及慢连接跳帧规则，不写领域事件。重连总是重新发 snapshot；Last-Event-ID 不承诺完整预览历史，GET 查询仍为最终事实。
- **终态后的迟到结果**：退款后找回产物继续保留 failed/退款状态和 lateResultAvailable。已关闭的预览流不承担无限等待；现有共享失效通知与历史查询发现该变化，成员内容接口可正常领取。
- **输入恢复**：`GET .../input` 返回 `{contractVersion, requestId, input, assets, confirmation, recoverable}`。input 是本人的规范化 GenerationInput，使用实际首次 seed 和原 sampleCount，可另返回各 child 实际 seed；confirmation 是原授权的只读回显。复用前重新报价，不自动沿用旧授权。
- **附件读取**：input 中的图片/编码字段使用本响应 assets 中的稳定槽位引用，assets 给出 slot、合法 MIME、长度和上述 Cookie 读取 URL。slot 只从该请求的已验证附件字段/数组索引映射，不拼任意磁盘路径；复用浏览器先读取字节再作为新 JSON/multipart 输入，不让服务器下载 URL。复用原 generation_requests 数据，不额外复制一套素材库。历史输入本来未保存则 recoverable=false，不能猜出原蒙版或提示词。
- **产物读取**：新 Cookie 内容接口仅在存在完整可领取结果时返回 200 与实际 MIME/原始字节。未完成为 409 result_not_ready，失败无结果为 409 generation_failed，原产物被清理为 410 result_unavailable；他人或不存在的 job 为 404。退款后的完整迟到结果返回 200，不补扣。旧 `/api/jobs/:id/content` 的 SVG 错误图兼容行为保持，不复制到新接口。

新关系格式的 request 状态继续区分父受理与 child 原创建时刻：job.createdAt 来自原 JobAccepted 事实，历史 Opening 仅使用有来源的可空 createdAt 字段；未知返回 NULL，不能回填父受理时间、开账/迁移日或 0。调用热力图不把未知日期归入任一天，owner 全部保留记录可显示“未知日期”分组。

历史 partial 输入的 `recoverable=false` 表示无法完整还原首次生成语义，不代表已知字段或附件已经丢失。`input` 仍返回实际保存的已知字段、空串和 NULL，`assets` 仍列出该请求的已知白名单槽位；不补当前 Native 定义、action、seed 或 sampleCount。成员可以读取并复制这些已知内容，编辑为新意图后按当前规则重新报价、确认并使用新操作键；这不重放旧 POST、不继承原费用授权，也不允许旧 partial 任务派发。未保存正文的 unavailable 输入仍返回 `input=null` 和 `recoverable=false`。

partial 附件领取仍核对当前成员、原 request/input 归属和来源摘要、槽位、同 input 的 BLOB 长度与 SHA。图片复用现有 PNG/JPEG/WebP 安全解码并核对保存 MIME；已知二进制只以 `application/octet-stream` 原样读取，不将它解释成已满足 Native/Vibe 派发条件。完整输入继续按保存的首次上下文校验，partial 字节读取不伪造该上下文。坏摘要、缺字节或无效内容返回 input_unavailable；非本人的请求或不存在的白名单槽位返回对应 404。

所有请求/附件/产物都重复校验成员归属；停止接单成员仍可查看、取消等待及领取既有结果。合法会员会话失效时返回认证错误，不把它伪装成结果丢失。输入响应不泄漏内部账号、PST、会话、签名密钥或与该成员无关的规范化上下文。

## 7. 错误与传输契约

新端点的 JSON 错误统一为 `{statusCode, code, message, details?}`，details 只允许字段名/限制、requote 提示和本人 requestId 等有限信息，不回显正文、提示词、图片或认证信息。已开始的 SSE 不能再改 HTTP 状态，使用明确的 auth-expired/error 事件或中断连接；业务任务失败由 state/complete 的持久状态描述。

| HTTP            | code 示例                                                                                     | 含义                                                  |
| --------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| 400             | invalid_body、invalid_idempotency_key、confirmation_required                                  | 结构、幂等头或费用确认缺失                            |
| 401 / 403       | session_expired、membership_required、origin_rejected、member_stopped                         | 会话/资格/同源/新增任务权限；停止接单不阻止读取旧结果 |
| 402             | insufficient_balance、purchased_anlas_not_authorized                                          | 当前余额或购买池条件不足                              |
| 404             | request_not_found、job_not_found、input_not_found                                             | 不存在或不属于调用者的资源                            |
| 409             | quote_changed、cost_limit_exceeded、idempotency_conflict、result_not_ready、generation_failed | 需重新报价、超原授权、同键冲突或结果状态              |
| 410             | result_unavailable、input_unavailable                                                         | 已知历史对象的内容确实不再可用，不触发重生成          |
| 413 / 415 / 422 | payload_too_large、unsupported_media_type、invalid_parameters                                 | 大小、媒体类型、参数/模型组合不合法                   |
| 429             | queue_full、free_image_limit、payload_budget_exhausted                                        | 临时容量限制；有明确恢复时刻才给 retryAt/Retry-After  |
| 503             | executor_unavailable、upstream_unavailable                                                    | 执行器不可用或当前上游/快照不可用                     |
| 500             | internal_error                                                                                | 未预期内部故障，保留脱敏诊断关联，不回显内部异常      |

quote 的 admission.reasons 可使用同一代码，但认证/结构错误不能塞进 200 当普通预览。提交后发生的失败写入 job.errorCode，后续 GET 查询返回 200 及真实状态，不把任务业务失败混成查询服务失败。

新增两个大正文 POST 使用 `forwardExecutorStream`，避免现有 buffered proxy 的 2 MiB/45 秒语义和无意重试；既有小 JSON 请求仍用原传输。新预览路径也走原始流转发，**不能用只允许 64 KiB 单帧的 shared-events 桥**。接口白名单、方法处理、成员路由派发和 Nuxt 文件路由一起更新。

## 8. 实现位置与验收

能力定义和类型由共同业务模块拥有，quote 与 submit 不各实现一套 parser/price。前端消费类型及 DTO；SQLite 写入、认证和生成仍只在执行器。现有 JobPreviews、原生正文解析、请求查询/取消、ImageStorage 和结果恢复继续复用。

| 编号 | 必须运行的场景                                                | 判据/对应工作包                                                                |
| ---- | ------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| G01  | 当前模型/操作/固定参数和不支持组合                            | capabilities 与规范化/报价/提交逐项一致；R03                                   |
| G02  | 同输入 JSON/multipart、免费/V5/Anlas、辅助操作                | 规范化指纹、逐图/合计和独立价格期望相同；R03/R04                               |
| G03  | 重复报价、恢复估算、输入出错、正文中断                        | 钱包/ledger/任务/预留/事件均未变化，临时预算释放；R03                          |
| G04  | Cookie/Origin、跨成员 ID、过期会话、owner 自用                | 无权限扩大，不要求在页面暴露 Key；R04/R09                                      |
| G05  | 同键并发、丢回执、重启、随机 seed、版本变化或模型下架         | 原 confirmation/seed/defaults 保留、原 request 可重放领取、不重复生成；R04/R07 |
| G06  | 伪造价格/指纹/版本、缺确认、扩大或改变同键上限                | 服务端重新计算，明确 400/409，无半份受理；R04                                  |
| G07  | 最后余额/免费名额、购买池、排队后政策或费用路径变化           | 提交/派发重验，不超单张/整批原授权；R07                                        |
| G08  | 多图部分成功、取消、失败退款与迟到结果                        | 原批次公平、已完成保留、余图停止、迟到不补扣；R07/R09                          |
| G09  | 8 MiB 附件、32 MiB 请求、展开后上限、超过 64 KiB 的合法预览帧 | 新链路完整通过且不放宽旧入口；R04/R09                                          |
| G10  | 初始订阅竞态、慢连接、断线、Nuxt/执行器重启                   | snapshot/补读无遗漏；预览可丢帧，生成和最终领取不丢；R08/R09                   |
| G11  | 参数/实际 seed 复用、蒙版/参考附件、编码二进制、旧输入缺失    | 仅本人可恢复，附件无第二份持久副本，不猜缺失字段；R04/R09                      |
| G12  | 旧 Native/OpenAI/direct/网页与新接口交叉                      | 老协议状态/鉴权/可选幂等保持；同键确认冲突不新建任务；R09/R11                  |

能力、报价和生成的 HTTP fixture 使用合成数据；费用预期引用已有独立报价样本及手算小账本，不用待测报价函数生成全部 expected。合同测试与实际 Nuxt＋唯一执行器链路都要通过，不能只验证纯函数或 DTO 编译。

文档 JSON 样例仅用于说明形状；cap/version/hash 必须由实现生成并在测试中验证，样例 hash 不是可提交到服务的签名或凭据。实现完成后把真实路径及验证记录链接回现有 API 契约，本文件继续作为新接口的唯一详细规范。
