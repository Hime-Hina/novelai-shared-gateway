# NovelAI Shared Gateway

**共享 Opus 的自托管网关，源码可查看，仅限非商业用途。禁止售卖、付费分发、商业托管和未经许可的商用。** 新增及修改部分见 [LICENSE](LICENSE)，上游 MIT 部分见 [NOTICE.md](NOTICE.md)。这不是 NovelAI 官方项目，也不是不受限的开源许可证。

此仓库发布可独立部署的基础共享框架：管理员、成员账号、额度账本、排队和生图接口。支持可选的 Discord OAuth、管理员邀请码和指定服务器成员验证；**不包含拼车大厅、聊天、收款与续车业务，也不包含线上数据。** 最近更新见 [CHANGELOG.md](CHANGELOG.md)。

已完成的修复与后续实施顺序见[共享网关改进计划](docs/modernization-plan.md)，包含前端工程化、实时状态、TypeScript、排队诊断和隔离测试部署；各项以实施记录和对应验收为准。

成员、owner 与私有管理网页及 API 入口已迁入 `web/`，使用 Nuxt、Vue 3、TypeScript 和 Naive UI。Nuxt 将鉴权、SQLite 和生图调度交给独立执行器；长请求及图片逐块转发，断开网页连接不取消已受理的任务。开发方式和 SSR 样式接入见 [Nuxt 成员页](docs/nuxt-member-portal.md)，内部边界见[执行服务](docs/executor-service.md)。

## 功能

- 管理员创建成员账号，成员独立登录、查看自己的 Key、额度和任务。
- 逐人填写具体百分比，合计 100%；也可配置四个顶层组及组内权重，只有管理员能修改分配。
- 共用一个 Opus 上游，按队列串行生图；上一张完成后至少等待 10 秒再派发下一张，官方限流时自动退避。
- V5 恢复额度、订阅 Anlas、购买 Anlas 分账，预扣与失败退款持久化。
- 可选“个人 V5 不足转扣本地 Anlas”，参数不变，默认关闭，由管理员开启。
- 查看官方共享额度状态；兼容酒馆 Native NovelAI 和 OpenAI 风格生图接口。
- 账本与队列提交后按权限推送失效通知；成员额度、调用与 owner 当前视图合并刷新，后台更新保留未保存草稿，详见[实时状态](docs/state-events.md)。
- 成员与 owner 可查看最近一年的[调用热力图](docs/call-heatmap.md)，按台北提交日逐张统计，生图/放大/Vibe 编码及成功/失败/取消分开查看；旧历史缺失明确显示未知。
- Native 接口支持 V3／V4／V4.5／V5 Full、Curated（5C）、Medium，以及图生图、局部重绘、Vibe／Precise Reference、Vibe 编码和 2 倍放大，具体组合见下表。
- 支持原生 MessagePack 实时预览与逐张最终图片流，兼容不会自动回退 ZIP 的客户端。
- V5 恢复优先分给未满额成员；官方聚合余额变化不直接扣减闲置成员钱包。
- 接受 JSON 和启动器的 multipart 请求；一次 1–4 张按独立单图依次排队、扣费与退款。
- 不提供公开注册、兑换码、充值商城或旧图库管理。

## 管理员分配与成员管理

### Discord 邀请登录

在自己的 [Discord Developer Portal](https://discord.com/developers/applications) 创建应用，在 OAuth2 的 Redirects 中登记 `https://你的域名/api/member/discord/callback`。配置 `.env` 的 `DISCORD_CLIENT_ID`、`DISCORD_CLIENT_SECRET` 和 `DISCORD_GUILD_ID`，并保持 `PUBLIC_BASE_URL` 为该 HTTPS 站点地址。只需 `identify` 与 `guilds.members.read` 两个 OAuth 权限；不需要机器人、服务器管理员权限、邮箱或消息读取权限。

设置任一 Discord 配置后，成员密码登录即被禁用；配置不完整会拒绝登录，不会回退密码。管理员仍使用本机入口与管理员密钥。完全不设置这三项时，保留原有账号密码模式。

管理员在成员行点击“邀请码”，将一次性代码交给对应成员。代码有效期七天，只保存哈希；再次生成会立即撤销旧代码。成员首次填写邀请码后通过 Discord 授权，后端确认其属于指定服务器且已完成成员验证，才将身份绑定到这份已有钱包。绑定不改变 Key、余额和份额；同一 Discord 不能领取两份钱包，未获邀的服务器成员不能自动注册。绑定后只需 Discord 登录，不再填写邀请码。

网站会话使用 SQLite 持久保存：普通成员固定七天，owner 固定十二小时，访问页面和刷新均不续期，重启保留未失效的登录。成员页面及 API Key 请求共用服务器资格检查，结果最多缓存五分钟；退服后后续校验会拒绝请求并撤销已有网页会话。Discord 临时故障返回真实错误，不伪装为退出；资格或授权失效仍拒绝访问。退出网页只撤销当前会话，API Key 仍可用于客户端；管理员停用成员会同时阻止登录和调用。owner 归属改变须重新登录，普通长会话不能直接升权。首次从内存会话版本升级需重新登录，详见[会话规则](docs/member-sessions.md)。

OAuth 回调使用五分钟、单次、绑定浏览器 Cookie 的 state，只接受配置中的回调域名。Discord access/refresh token 只保存在服务端 SQLite，不返回前端；数据库和备份均按密钥文件保护。更新部署后重启服务，再生成邀请码。不要把 Client Secret 写入网页配置、日志或 Git。

参考：[Discord OAuth2](https://docs.discord.com/developers/topics/oauth2)、[当前用户的服务器成员信息](https://docs.discord.com/developers/resources/user#get-current-user-guild-member)。

### owner 网页控制台

将 `OWNER_USER_ID` 设置为一位已绑定 Discord 成员的内部 ID（从私有成员管理 API 获取，不是用户名或 Discord ID），重启后该成员的首页会显示“管理控制台”。入口为 `/owner`，使用已有 Discord 会话；没有配置时不会授予任何成员管理权限。用户名叫 `owner` 并不会获得权限。

控制台可管理成员、一次性邀请码、份额和余额，暂停/恢复共享上游、刷新官方余额、轮换同一 NAI 账户的 PST。owner 无法停用或删除自己。恢复上游前会重新核验有效 Opus；换 Key 后保持暂停。PST 只接受写入，不在管理响应中返回；原 `/admin` 及管理员 Key 入口仍限本机访问。

“用量与记录”提供今天、近七天、近三十天的数据，按台北日期、成员和模型汇总成功/失败、免费次数、已结算的 V5 与两类 Anlas，以及排队和生成耗时。退款与尚未结算的预扣不计入消耗，缺少耗时记录的任务不计入平均值；最近五十条请求不展示提示词、图片或认证凭据。

### 份额与余额

在“分组与额度”中逐人填写 0–100% 的份额，预览后保存。0% 成员保留已有余额，但不再获得后续恢复和新增额度。直接分配默认勾选“同步分配当前剩余余额”：保留各组总额，在组内按权重重分；跨组余额转移使用成员行的“调整额度”。取消勾选则只调整未来分配，当前余额不变。

成员行支持复制分发信息、重置密码、停用/恢复、精确转移 V5 或两类 Anlas，以及删除。删除先预览余额及逐人份额变化，再输入用户名确认；全部余额与原份额交给指定接收者，其他成员的份额不变。旧登录及 Key 失效，任务和账本历史保留。启用的未分组账户可直接接替原成员的分组位置、份额及全部余额。停用仅禁止登录与调用，不自动回收额度。

有排队、运行或额度预留时暂不能修改分配。保存校验配置版本，避免覆盖其他管理操作；预览后的余额仍可能因生成或恢复变化。删除转入或调低容量后，已有 V5 可暂时超过容量，超额期间不继续恢复。

“停止接单”与停用账户分开：一次提交同时禁止该成员新增任务、取消未派发任务并退回预留；在途任务仍正常保存和结算，成员仍可登录、查询和下载。恢复接单只接受新请求，不复活被取消的等待任务，也不继续停止前多图请求的剩余图片。

## Docker 部署

需要 Linux Docker Compose。Compose 分别构建 `gateway`（独立 Nitro 执行器）与 `web`：执行器绑定回环 18081，网页绑定回环 3000。保留原 `gateway` 服务名以替换同一服务，不新增第二个数据写入者；只有它读取完整 `.env` 和数据卷。网页只获得内部地址和 `EXECUTOR_TOKEN`。复制示例配置，分别生成管理员与内部通信密钥：

```sh
cp .env.example .env
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
mkdir -p data
sudo chown 1000:1000 data
chmod 700 data
```

将上述随机生成命令运行两次，在 `.env` 中分别填写 `ADMIN_TOKEN` 和 `EXECUTOR_TOKEN`，再填写自己的 `NOVELAI_TOKEN`（PST）和公网域名。两份密钥均至少 32 字节，不要复用或使用示例值。**不要把 .env 提交到 Git。**

只在本机体验时，将 `PUBLIC_BASE_URL` 改为下文的同源分流入口 `http://127.0.0.1:18082`；18081 已是需要内部密钥的执行入口，不供浏览器直接访问。对外部署后填写自己的 HTTPS API 地址。成员配置页优先读取这个环境变量，避免数据库中的旧地址继续被复制给成员。备用域名可填入 `PUBLIC_HOST_ALIASES`，多个主机名用逗号分隔；每个域名仍需要自己的 HTTPS 反向代理配置，别名不会开放管理员入口。

先初始化，再启动服务：

```sh
docker compose build
docker compose run --rm gateway node .output/server/node_modules/novelai-shared-gateway/scripts/bootstrap.mjs
docker compose up -d
```

公开入口仍需按下文用 Caddy 提供 HTTPS 与公私管理隔离；全部网页和 API 都进入 Nuxt，执行器端口仅供内部访问。更新网页可单独重建、重启 `web`，不会联动重启 `gateway`。已有生产实例应先在隔离环境验证，不能直接用上述命令覆盖运行中的生产版本。

初始化只允许全新数据目录，拒绝覆盖已有数据库。它创建一个上游、四个等份成员，随机密码与成员 Key 写入 `data/bootstrap-credentials.json`，不会打印到日志。请私下将对应凭据交给成员。首次查询到有效 Opus 后才分配官方额度；初始化不会生图，也不会创造官方余额。

配置下文的 Caddy 分流后，本机私有管理入口为 `http://127.0.0.1:18082/admin`，使用 `.env` 中的管理员密钥。18082 仅监听回环地址，同时提供 Nuxt 页面与网关 API。需要远程管理时可用 SSH 隧道：

```sh
ssh -N -L 127.0.0.1:19080:127.0.0.1:18082 your-user@your-server
```

随后访问 `http://127.0.0.1:19080/admin`；不要将新管理页的隧道直接连到网页 3000 或网关 18081。私有反代保留浏览器原 Host 和端口，使 Origin 校验成立。owner 仍从公开 HTTPS 域名的 `/owner` 使用成员 Cookie 登录，不与私有管理员 Key 混用。后台支持组与成员管理、续用策略、上游状态、暂停/启用和更新 PST。更新 PST 后需要刷新额度、再明确启用；应只轮换同一官方账户的凭据，替换为另一官方账户前必须另行处理原账本。

## 公网与客户端

Compose 只绑定回环地址。使用 HTTPS 反向代理对外提供服务，参考 [Caddyfile.example](Caddyfile.example)，修改域名并保持与 `PUBLIC_HOST` 一致。Caddy 全部转发到 Nuxt，不再直连执行器，也不需要内部、管理员或上游密钥；API 保持不压缩并及时刷新流，网页资源可压缩。管理员路由不能公开，代理不能把外部请求伪装成 localhost。不要记录 Authorization 或带 token 的完整 URL。

成员密码登录与 Discord 登录起始请求按客户端 IP 限流。`TRUSTED_PROXY_IPS` 默认留空，此时只采用 TCP 连接来源，忽略 `X-Forwarded-For`、`X-Real-IP` 等外部头。启用反向代理时，必须填写网关实际看到的、由你控制的代理精确 IP（逗号分隔），否则所有经过同一代理的成员会共用限流桶；不接受主机名、网段或 `*`，格式错误时登录返回 503。密码模式仍额外保留每用户名限制，改变 IP 不能重置该限制。

systemd 下同机 Caddy → Nuxt → 执行器使用回环时通常为 `TRUSTED_PROXY_IPS=127.0.0.1,::1`。Docker 下还须核对链路中 Nuxt 容器和 Caddy 的实际来源 IP；容器重建或网络变更后复核，不能直接照抄回环配置，也不要信任整个私网。Compose 已通过 `env_file` 读取该配置。Nuxt 追加自己的实际入站来源，执行器仍逐跳验证。内部监听端口应只允许 Nuxt 等获准内部调用方访问，不能把内部密钥当作成员或管理员凭据。

网关仅在连接来源可信时读取 `X-Forwarded-For`，从右向左经过已配置代理，停在第一个不可信地址；畸形链回退到连接来源，不从其他请求头猜测。代理必须覆盖客户端自带的转发头，或追加它实际收到的连接来源。示例 Caddy 的默认行为会忽略外部伪造值；若前面还有 CDN，应先按 [Caddy 官方代理信任配置](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#headers) 配置其信任边界，再按实际链路配置网关，不能直接信任客户端提供的 CDN/IP 头。发布前用两个不同来源地址确认限流互不影响，并验证伪造前缀不能改变限流身份。

- 成员门户：`https://your-domain/`
- API Base：`https://your-domain`；已有 `/v1` 后缀也兼容。
- Key：成员门户中“查看我的 API Key”，不要给成员上游 PST 或管理员密钥。

接口包括 `GET /v1/models`、`POST /v1/chat/completions`、`POST /ai/generate-image`、`POST /ai/generate-image-stream`、`POST /ai/encode-vibe` 和 `POST /ai/upscale`。上述 `/ai/` 路径均提供 `/v1/ai/` 别名。Native 生图接收 NovelAI 的 `input/model/action/parameters`，普通接口按 Accept 返回图片 ZIP/JSON，流式接口返回 MessagePack 预览和逐张最终图；Vibe 编码返回二进制。所有生成接口都要求成员 Bearer Key；具体酒馆插件的 Base 拼接方式以插件说明为准。

## NAI 启动器兼容

支持 W.O.F／Aaalice NAI Launcher、柏宝绘、小白盒及 novelai-image-desktop 的对应生图接口。主 API 地址填写 `https://your-domain`，图像 API 地址可留空或填写相同地址，Token 使用成员网关 Key。不要填写网站登录页或完整的生图接口路径；客户端菜单中超出下表范围的功能仍会被拒绝。

提供 `GET /user/subscription`、`/v1/user/subscription`、`/ai/user/subscription` 和 `/v1/ai/user/subscription`，只返回调用者自己的 Anlas、V5 余量和当前恢复速度，不公开上游总钱包。

Native 的 `parameters.image_format` 接受 `png` 或 `webp`，省略时保持原有行为。响应保留上游图片的原始字节；ZIP文件名和下载MIME随实际格式变化，同一幂等键改格式会返回409。

`/ai/generate-image-stream` 缺省返回SSE；二进制客户端须显式提供 `parameters.stream="msgpack"`，响应为`application/msgpack`。`Accept`不覆盖stream，缺省或显式`sse`使用`text/event-stream`；更换交付格式不改变幂等任务。SSE图片为base64，MessagePack图片为binary并带4字节大端帧长。

网关实时发送`intermediate`预览，每张图片保存、结算后发送`final`，无需等待整批完成。预览沿用上游`step_ix`，多图使用批次内`samp_ix`；慢连接可能跳帧。断线不取消已受理任务，可通过任务记录或同一幂等键领取结果。当前没有排队心跳，长时间排队仍需客户端和反向代理设置足够超时。

Native 请求同时支持 `application/json` 与 `multipart/form-data`。multipart 的 `request` 部件可为 JSON 字符串或 JSON 文件，图片部件必须由请求明确引用。总请求上限 32 MiB，单张附件上限 8 MiB。自动角色布局的空坐标按中心点兼容；显式手动坐标仍严格验证。支持桌面客户端的 `uc_preset`／`quality_toggle` 别名；与标准字段同时填写时必须一致。未知参数错误只列出有限字段名，不回显内容。

普通 Native 生图和放大成功均返回200。两者缺省保持ZIP，指定 `Accept: application/json` 可领取含base64图片、索引及生成种子的JSON，并按Accept权重选择；不接受两种格式时在入队前返回406。Vibe编码返回201与`application/binary`原始字节。部分成功返回200，JSON与ZIP都保留批次说明；同键改变响应格式不再次生成。普通生成的200依据[官方JSON/ZIP实测](docs/novelai-protocol-probes.md)，与Swagger中的201不同；[API契约](docs/api-contract.md)记录各项边界，本分支尚未替换生产。

## 生图功能与计价

以下实现已在 2026-10-10 核对官网规则，并于 2026-10-13 同步客户端兼容修复；费用随模型、像素、步数、图生图强度和参考数量变化。先校验和计价，再进入共享队列。

| 功能 | 支持范围 | 本地记账方式 |
| --- | --- | --- |
| 文生图 | V3／Furry V3／V4／V4.5／V5 Full、5C、Medium | Opus 标准尺寸、最多 28 步：V5 扣估算百分点，较早模型基础生成免费；超出免费条件扣 Anlas |
| 图生图、局部重绘 | 对应模型及其 inpainting 变体；5C 暂仅文生图和图生图 | 按提交像素、步数与强度核价；重绘不按遮罩面积单独打折，不自动将 5C 换成 4.5 |
| V5 Medium | `nai-diffusion-5-full-medium` 及其重绘变体 | 固定 14 步、Euler ancestral、CFG rescale 0 和官网 heavy 负向；按转换后的实际参数计价 |
| Vibe | V3、V4、V4.5 | V4 系列前 4 个参考无额外费用，之后每个加 2 Anlas；V5 不支持 |
| Precise Reference | V4.5，不能与 Vibe 同用 | 每个参考加 5 Anlas；标准生成保留基础免费条件 |
| Vibe 编码 | V4、V4.5 | 每次 2 Anlas，返回编码二进制 |
| 2 倍放大 | 已校验的 PNG／JPEG／WebP | 按输入像素阶梯计 1–4 Anlas |
| 一次多张 | Native 接口 `n_samples: 1–4` | 逐张排队和结算，同一批不一次占满并发槽位 |

Medium 会替换自定义负面提示词，正向及角色提示词保留；这与该模式的固定参数一致。标准 1024×1024 的 Medium 本地估算消耗约为 Full 23 步的 58%，转扣 Anlas 时为 18 点；这是估算和本地分账，不是官方逐张用量承诺。

多图采用多次独立单图请求，费用不等同于 NovelAI 官方原生批量报价。部分成功时，ZIP 内含成功图片与 `results.json`；流式接口返回已完成的 `final` 帧及后续 `error` 帧。普通生图响应头 `X-Nai-Image-Count`、`X-Nai-Partial` 表示完成数量和是否只完成部分；实时流开始时数量未知，省略这两个头，以事件和任务查询为准。失败图片退款，未开始的图片不收费。超过费用或图片尺寸上限的请求会明确拒绝。

## 额度语义与限制

V5 用量按图片像素与步数估算。官方可能只返回整数百分比，**个人小数额度是估计值，不是官方逐张账单**。官方聚合余额只用于来源核对和派发前的安全检查，不再按差额直接扣减闲置成员的钱包，也不把突然增加的总余额当作个人充值。规则可能随官方套餐变化，需要维护。

个人容量是 `100 × 最终份额` 个百分点；组内成员的最终份额等于组份额乘以组内权重比例。恢复量按官方返回的恢复周期计算：若恢复 1 个百分点需要 `S` 秒，那么整个账户每天理论恢复 `86400 / S` 个百分点。有可用容量时，只在尚未满额且份额大于零的成员间，按原份额比例分配。

例如四人各占 25%，三人已满、只有一人未满，那么后续可分配的恢复都给该成员，补到 25% 为止；其他成员再次使用后会重新参与恢复。预留中的任务也占个人容量，避免退款后超过上限。**全体满额时，未用掉的恢复不会积攒到以后。** 成员页和启动器显示当前速度；人数、用量和官方状态变化时，预计补满时间也会变化。

计算仍受官方剩余量约束，并保留整份账户合计 1 个百分点的安全余量。官方查询出错、数据过期或出现无法可靠核对的差额时，不承诺补满时间；旧快照最多只推算十分钟。历史缺口不会因为一次读到官方 100% 就直接补满，失败任务会退回原钱包。

标准 1024×1024、23 步 V5 估算为约 0.057803 个百分点；转为完整 Anlas 估价时为 26 Anlas。开启续用后，个人 V5 不足便从本人的本地 Anlas 钱包扣费；失败退款，余额不足拒绝。**这仅是本地分配：上游仍可能使用公共 V5，官方 Anlas 未减少也不退本地扣款。** 公共恢复可能先偿还这种估算消耗，其他人的实际可用量仍受共享余量限制。

订阅与购买点数不混用。购买 Anlas 需要 `allowPurchasedAnlas: true`，并满足官方固定余额耗尽等条件。符合免费尺寸/步数条件的 V4.5 及更早支持模型不扣这两类额度，但仍排队。

共享账本对每张图片及编码、放大操作独立预扣、结算；失败幂等退款。共享上游固定为一个在途任务，按入队顺序派发；完整结果保存并结算后至少等待十秒。普通上游故障依次等待 10/20/40/60 秒，容量拒绝为 30/60/120 秒，两类独立计数，真实成功后清零；已有更长截止与有效上游 `Retry-After` 取较晚者，重启不清除。未派发的取消或过期不算上游失败。页面显示原因与本地倒计时；有冷却证据的错误响应带实际剩余秒数，没有证据时不编造固定 `Retry-After`。不会自动提升并发、换号或重发失败图片。上游流只有预览帧或最终报错时不会作为成功图片收费。详见[冷却与恢复规则](docs/request-lifecycle.md#冷却与自动恢复f12)。

默认每成员最多 4 个、全局最多 16 个未完成任务（包含在途及等待前序图片），入队与上限检查在同一事务完成；满额返回现有接口格式的 429。Native 多图一次持久受理全部意图，前一张成功后下一张才进入当时队尾，失败终止余图。未派发任务默认十分钟过期，重启与激活下一张不重新计时；HTTP 等待期限独立于任务期限。这些初值基于[队列基线](docs/queue-baseline.md)的全局等待峰值 3、单成员峰值 1、最长等待 147 秒，仍需在隔离环境验证突发流量；可通过 `.env.example` 中的数量及等待期限参数调整。

网页、Native、direct 与 OpenAI 的已受理任务不会因客户端断线或同步等待超时被取消。Native/direct/OpenAI 返回 `X-Nai-Request-Id`，成员可凭 Bearer Key 查询 `GET /api/requests/:id`，或向 `POST /api/requests/:id/cancel` 发送 `{}` 取消未派发图片；生成中的图片继续，已完成结果保留。成员页也提供批次查询、领取和取消。

共享结果先保存可恢复描述，再写临时文件、同步并发布，最后提交图片记录和结算。SQLite 使用 WAL/FULL；重启可找回已完整落盘的图片或 Vibe 编码，未完整收到或损坏的结果按未知失败退款且不自动重发。记录与文件不按默认缓存数量自动删除。正常退出默认等待在途任务和 HTTP 响应 30 秒，可用 `SHUTDOWN_GRACE_MS` 调整；系统管理器应给更长停止宽限。强杀验收不代表断电零损失，详见[恢复保证与平台边界](docs/result-recovery.md)。

生成接口可选提供 `Idempotency-Key`，同成员同操作同键重试复用原任务；有效参数冲突为 409，历史任务或产物缺失为 410，不自动重新生图。direct 仍使用原错误图片格式。无键客户端保持原行为，不能仅凭相同提示词判断是重试；完整约束与故障边界见[持久请求说明](docs/request-lifecycle.md)。这不保证找回 NAI 已执行但网关尚未保存的图片。

F06 第一片已让网页 `POST /api/jobs` 的完整参数与任务同事务持久化，历史参数不进入热缓存，未派发任务重启后按需读取执行。网页正文仍限 2 MiB；规范化参数默认限 32 MiB，新路径活动参数合计限 64 MiB，可通过 `REQUEST_MAX_PAYLOAD_BYTES` / `QUEUE_MAX_PAYLOAD_BYTES` 调整。超出单请求限制返回 413，暂满返回 429。Native/direct/OpenAI 及其在途取消边界尚未接入这片字节预算，详见[持久请求说明](docs/request-lifecycle.md)，不能将它当作全站内存保证。

公开版仍是管理员管理一个共享 Opus 的基础框架，不包含线上站点的多车队租户系统。

## 数据与测试

SQLite、图片和凭据均存于 `data/`，服务重启后保留；有效网页登录会话也保留。默认不自动删除图片/历史，请自行监控磁盘并备份。公开仓库不含生产数据、真实凭据、个人地址或部署历史。

本地开发使用 `.node-version` 固定的 Node.js **22.23.3** 与 `pnpm 10.32.1`，与 CI、Docker 构建一致；Nuxt 不支持旧的 Node 24.14.0，可选兼容分支见 `web/package.json`。pnpm 版本固定在 `package.json` 的 `packageManager`。使用 Corepack 时先启用 pnpm shim；已有其他版本 pnpm 的环境也可用 `corepack pnpm` 显式运行本项目固定版本。

```sh
corepack enable pnpm
pnpm --version
pnpm install --frozen-lockfile
pnpm run check
pnpm test
pnpm --filter @novelai-gateway/web typecheck
pnpm --filter @novelai-gateway/web test
```

唯一依赖锁文件为 `pnpm-lock.yaml`；新增直接依赖默认保存精确版本。包管理迁移保留原来的全部 38 个锁定依赖版本，`better-sqlite3` 固定为 `12.10.0`。其原生安装脚本已在 `package.json` 中明确允许，其他依赖不会因此获得执行安装脚本的许可；升级该原生依赖时需同时复核许可版本。不要使用 `--ignore-scripts` 跳过 SQLite 构建，也不要在 Windows 与 Linux 之间复制 `node_modules`。冻结安装失败时应查明锁文件或构建问题，不改用非冻结安装绕过。

初始化和启动使用 `pnpm run bootstrap`、`pnpm start`。已有 npm 布局切换时，在测试停止后移走旧 `node_modules` 再冻结安装，保留旧目录以便回退；不混用 npm 锁文件。systemd 可继续直接运行 Node 入口，无需套包管理器。安装方式与平台要求见 [pnpm 10 官方文档](https://pnpm.io/10.x/installation)。

测试集中在 `tests/unit/` 和 `tests/integration/`，合成样本及共享辅助代码分别位于 `tests/fixtures/`、`tests/helpers/`。测试使用合成账户、合成图片与临时目录，不调用官方生图。计价包含 184 个固定的官网基础报价样例和 12 组 PNG/WebP 成对样例，仓库只保存测试参数和数字结果，不依赖本机的官网脚本副本。HTTP 集成测试会启动本机模拟上游与独立网关，核对初始化、成员登录、ZIP／MessagePack、扣费、退款及持久化；临时数据保留在系统临时目录，便于复核。不要用生产数据运行初始化测试，也不要把测试临时凭据或运行日志上传。
