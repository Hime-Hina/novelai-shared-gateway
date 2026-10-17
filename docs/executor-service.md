# 独立执行服务（F11，实施中）

目标是让 Nuxt 网页/API 与任务执行拥有独立进程生命周期。执行服务独占 SQLite
写入、任务调度、上游连接、结果文件、会话和 Discord 令牌刷新；Nuxt 不另建
数据库缓存或调度器。断开 HTTP 等待不取消持久任务，结果仍可按成员权限领取。

## 已抽取的运行时

`server/gateway-runtime.ts` 保留现有业务与文件布局，仅提供两个入口：

- `handleGatewayRequest(req, res)`：完整的原 Node handler，包含访问守卫、
  各协议错误响应及载荷预算的 finally 释放，不只导出内部 router。
- `startGateway(handler)`：初始化数据库，先恢复完整结果再清理中断任务，随后
  监听端口；监听成功才启动额度刷新、未知核对和队列派发，并安装统一关闭流程。

`server/index.ts` 仍是兼容启动入口，调用同一运行时。导入运行时本身不会建库、
监听或启动后台任务；它只能由执行服务的进程入口启动，不能在 Nuxt 请求、SSR
或热更新插件中调用。旧 CLI 与 Nitro 是同一数据目录的替代启动方式，不可并行启动。

关闭流程先结束成员状态和任务预览订阅，随后允许在途任务持久化及响应完成，
最后关库。关闭订阅不发会话失效通知，不提前退款或取消等待任务。
相关修复和真实 Linux 信号验证见[结果恢复](result-recovery.md)。

抽取后 Windows 完整后端 476 项中 472 通过、4 项平台跳过；随后新增的导入
无副作用检查单独通过，未冒充已包含在前一轮全量中。Linux 镜像定向验证
导入、双 SSE 退出、在途 PNG/ZIP 完整交付与超宽限恢复，3/3 通过。
此时尚未经过 Nitro handler，因此不能据此宣布 Nitro 契约或 F11 已验收。

## Nitro 接入边界

采用当前 Nuxt 已锁定的 Nitro 2.13.4、H3 1.15.11 与 Node 22.23.3。
使用官方 `node-listener` 与 `fromNodeMiddleware` 接入原始 req/res，保留小型
Node 进程入口复用上述恢复、监听和关闭顺序；不使用 Nitro Tasks 代替持久队列。

选择 listener 是因为该版本默认 node-server 的 close hook 在 HTTP 排空后调用，
不能用它作为首次停止派发的时机；默认插件调用也不等待异步数据库恢复。
版本依据为已安装产物及官方发布提交
[`039b841`](https://github.com/nitrojs/nitro/blob/039b841669cd0641e54b443cba875b6552b24e05/src/presets/node/runtime/node-listener.ts)，
不能套用当前 Nitro 3 网站示例推断该版本生命周期。

业务代码暂不打平为 Nitro chunk：SQLite 读 worker、Drizzle SQL/journal 与
public 资源依赖真实相对文件路径。构建产物须保留文件树与 SQL 精确字节，原生
SQLite 依赖按目标平台安装；不能将 Windows 的二进制直接部署到 bob。

T01 起，执行器构建先运行根项目的 TypeScript 编译，将 `.output/backend/server/`
的 JavaScript 放回上述业务文件树，并复制原始 migrations 目录；不把 TypeScript
源码直接交付到 `node_modules`。后端业务模块已迁为严格 TypeScript；根配置的 allowJs
仍发射原有公共脚本与运维脚本，不代表所有脚本都已完成类型迁移。两份后端共用的
public 报价/Medium 模块保留浏览器 JS 路径并启用 `@ts-check`。源码启动使用 `.ts`，
构建脚本将发布包的 main/start/dev 改为编译后的 `.js`。默认数据路径和绝对 DATA_DIR
要求保持。

## 内部入口与构建产物

`executor/` 是独立 pnpm workspace。入口要求绝对 `DATA_DIR`，避免将打包目录
当作数据目录而另建账本；`EXECUTOR_TOKEN` 至少 32 字节，应使用随机生成的值。
该密钥只证明内部调用方，不能代替成员 Cookie、Bearer Key 或 owner/admin 权限。
未配置 HOST 时只监听 `127.0.0.1`；容器内显式使用 `0.0.0.0`，部署时端口仍须
限制在回环或受控内部网络。此入口不自行读取 `.env`，由进程管理器传入配置。

每次请求须带单个 `X-Executor-Token`，长度与常量时间比较通过后才转交业务
handler；该头从 headers/rawHeaders 中移除，不进入后续处理或上游请求。
正文不预读，原 Host/Origin、Cookie/Bearer 继续按现有规则验证。Nuxt 接入时
必须保留原 Host 与可信代理链，不能借内部连接把公开请求变成本机管理员请求。

在仓库根目录运行：

```sh
pnpm --filter @novelai-gateway/executor typecheck
pnpm --filter @novelai-gateway/executor test
pnpm --filter @novelai-gateway/executor build
pnpm --filter @novelai-gateway/executor test:runtime
docker build -f executor/Dockerfile -t novelai-shared-gateway-executor:check .
```

构建后的进程入口是 `.output/server/start.mjs`，执行器与后端业务分别进行严格类型
检查；发布包只使用编译后的 JavaScript，不依赖运行时加载 TypeScript。
最终镜像只携带生产依赖与输出文件，包含原字节的 SQL/journal、worker 和
业务/public/scripts 文件树，并附带编入产物的 Nitro/H3 许可。CI 在最终镜像中只读挂载集成测试，以 UID1000、
禁网及独立 tmpfs 实际启动服务；不允许缺失产物时悄悄回退源码或跳过测试。

本地验证：认证边界 3/3，typecheck、Windows 与 Linux 构建通过；Windows
产物 HTTP 两项通过、信号子场景跳过，Linux 产物 3/3 通过（约 1.97 秒）。
实际检查 11 条迁移及完整性、成员/owner 热力图 worker 查询、原 Cookie 期限、
一次模拟 V4.5 免费生成的完整 ZIP/PNG、同 key 跨格式重放的请求 ID 与 PNG
载荷、参数冲突 409、任务/文件/签名领取和普通 EOF 退出。没有增加预扣或上游
调用，模拟生成总数为 1；owner 场景为 0。这里不是所有客户端组合的完整契约验收。

验证镜像为 `novelai-shared-gateway:f11-executor`，
`sha256:809f474b2ff609c87d13fb3414b962b2fb1277db23052a74c5dc4bc55a1457b1`。
全部使用合成数据和 loopback 模拟上游；集成测试运行期间只有执行器写库，
父进程只读审计。已有根包和前端版本保持，锁文件仅新增 executor 与它的 Nitro
依赖实例；离线冻结安装通过，没有顺带升级或去重旧版本。

提交 `85dc265` 的[托管 CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/38999790224)
已通过：后端 477/477、前端 80/80、执行器认证 3/3、类型检查、三个 Docker
构建及最终 Linux 执行器产物 HTTP 3/3。该验证范围仍为单独执行器。

## Nuxt 内部客户端与双进程验证

`web/server/utils/executor-client.ts` 使用私有 runtimeConfig：
`NUXT_EXECUTOR_BASE_URL` 与 `NUXT_EXECUTOR_TOKEN`，不放入浏览器 payload。
Nuxt 的进程配置不包含 DATA_DIR、上游 PST 或 Discord refresh token，不启动存储或调度器；
仅复用已有的纯内存载荷预算函数。默认配置为空，未配置时返回 503。

F11 客户端先支持有限 GET/HEAD 查询与 `/api/jobs`、`/api/web/jobs` 的异步受理。
Node HTTP 保留原 Host、Origin、Cookie/Bearer、重复响应头与原始二进制；剔除
逐跳头后仍显式保留 Host，不能把公网入口转换为内部 localhost。覆盖来访者的
内部密钥，X-Forwarded-For 追加实际 socket 来源，由执行器继续从右向左验证。

受理正文与完整 202 回执各限 2 MiB，复用 64 MiB 进程缓冲预算，所有退出路径
释放占用。正文与操作键在重试前固定，单次内部调用最长 45 秒，仅传输故障重试
一次；业务 HTTP 错误不重试。完整收到 202 后才向外写响应头，因此部分回执
仍可安全重试。内部认证错误标记在 Nuxt 转为 503，成员自身的 401 原样保留；
本地错误保持 `{error: string}` 与 no-store，不暴露内部凭据或错误标记。
内部随机键只能识别本次 Nuxt 受理的重试，不能识别 Nuxt 崩溃后无键客户端
另发的同内容请求；不会以正文相同就自动合并用户有意重复生成。

实际测试使用当前 `web/` 的 Nuxt 4.6.2 生产构建与执行器产物，固定项目的
compatibilityDate；仅测试构建注入 handler，生产没有新增测试路由。运行：

```sh
pnpm --filter @novelai-gateway/executor build
pnpm --filter @novelai-gateway/web test:executor
```

Windows 本地三场景全部通过（Node 计父测试在内 4/4，约 107 秒，含构建）：

1. 一项在途、一项排队且已受理后只强杀 Nuxt，执行器 PID 不变，两项继续落盘；
   重启 Nuxt 后按原 ID 领取，另一成员查询/下载均拒绝。
2. 故障 relay 丢弃第一次完整 202 回执，真实客户端自动复用同键、正文和身份；
   数据库只有一项任务、请求、幂等映射、预扣及结算，上游仅调用一次。
3. 仅强杀已同步完整 `.part`、尚未结算的执行器，Nuxt PID 不变；两次启动恢复
   同一 imageId 和 PNG，账目数量及金额稳定，没有重发或重复结算。

共四次模拟生成（两次免费、两次固定 Anlas），未访问真实 NAI/Discord。
前端完整 88/88（其中内部客户端真实 loopback 8 项）、类型检查、两个候选镜像
构建通过；最终 Linux 执行器镜像既有 HTTP 检查仍为 3/3。
客户端单测另覆盖半份 202、最多两次提交、非法/重复幂等头、2 MiB 正文边界与
chunked 超限、错误包络和凭据脱敏；未进行 64 MiB 并发压力测试。
双进程场景已加入托管 CI。最终代码 `fb54a7a` 的
[Linux CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/39001370060)
全部通过：后端 477/477、前端 89/89、执行器认证 3/3、双进程 4/4（含父测试，
约 48.5 秒含构建）、最终执行器镜像 HTTP 3/3、111 文件语法、类型及三个镜像构建。

截止时间复核发现 Discord 刷新与资格查询各允许 10 秒，后续冷查询还可能等待
15 秒，因此不能使用更短的统一 15 秒外层截止。现改为 45 秒，仍不用于长期 SSE
或大图流式传输。16 秒受理响应回归在旧截止下错误提交两次，修复后只提交一次；
修复后完整前端 89/89 通过。该测试模拟正常的慢受理，不据此宣称已经验证真实
Discord 慢调用链；N01 仍须针对并发刷新、令牌持久化和失败状态验收。

`cffd205` 阶段候选镜像：网页 `sha256:7f513cb92c214da533872df7a25113a10a0c0e79fdec6603f698f292719901e9`；
执行器 `sha256:271082157eefc6a4821eb47a5e46eec37f1c7b876a29b58bf4bfdef1840b911d`。

## N01 查询与状态入口

成员 `login-options`、`me`、`key`、`shared-status`、`heatmap`、`requests/:id`，
owner 的成员管理、统计、概览，以及私有 admin 的成员管理、概览和 ping，
由 Nuxt 正式 GET 路由转交执行器。DTO、权限、会话和 Discord 刷新仍由执行器
处理；Nuxt 不打开数据库，也不启动第二个后台刷新器。查询不能据此改为公共缓存。

`shared-events` 使用单独的流式桥接：内部断线后保留浏览器连接，每五秒重试，
恢复时透传执行器的新快照、启动标识和 resync。普通故障显示重连中，明确会话
失效才退出登录。Nuxt 收到退出信号先结束 SSE，执行器独立继续运行。
帧缓冲和慢读边界见[状态事件](state-events.md)，不复用有限查询的总截止时间。

移除先于文件路由执行的宽泛 devProxy。开发与构建使用相同路由，N01 阶段未迁
接口由 Caddy 转交同一个执行器。成员 GET 精确匹配，私有 admin GET 只在回环入口转发；
公开管理入口仍拒绝，API 不压缩，SSE 及时刷新。Compose 保留 gateway 服务名，
但使用 executor 镜像与内部密钥；只有执行器挂数据目录和完整配置，网页仅接收
内部地址与密钥。N01 阶段 Caddy 也需相同密钥；N03 全部转给 Nuxt 后已移除此要求。
两层实际代理来源仍须按部署核实。

双进程测试增加并发 Discord 刷新与 SSE 恢复场景：三个正式 GET 同时到达时只
刷新一次；先持久化新令牌，再处理资格查询失败，重启后使用新令牌而不重复刷新。
查询不额外写账本，成员/owner/admin 权限保留。SSE 跨执行器重启后在同一浏览器
连接校准；明确撤销成员后才发 auth-expired。所有身份与外部响应均为合成数据。

实际 Caddy 二进制测试直接读取仓库示例配置，覆盖精确路由、未迁方法、Host/
Origin/认证头、内部密钥覆盖、公私管理隔离、API 无压缩及 SSE 首帧及时到达。
开发烟测使用实际 Nuxt dev 服务；若 web/.env 存在则停止，不读写或移动该文件。

开发烟测另暴露两处构建差异：Nitro dev 将仓库外层纯预算模块留成错误的运行时
相对路径，以及 Naive UI 的 ESM 入口直接导入 vueuc 的 CommonJS 命名导出。
分别用 Nitro 的显式 inline 与 Nuxt `build.transpile` 处理已有模块；不复制业务
实现、不升级依赖。后者采用 [Vite SSR noExternal](https://vite.dev/config/ssr-options#ssr-noexternal)
机制，Nuxt 当前版本会将 transpile 列表传给它；真实首页与默认错误页均纳入回归。

本地最终前端 97/97、类型检查、真实开发烟测 1/1、语法检查 112 文件通过。
固定 Caddy 2.11.7 的隔离容器检查 7/7，Compose 合成配置验证通过；最终执行器
镜像中的 bootstrap CLI 在禁网、只读容器内创建四名合成成员并通过完整性检查。
网页与执行器镜像构建通过；网页为 `sha256:12a1d9e67ce4d76c075f54c520fc65885b303ec97da6619eb7164b407fb1319b`，
执行器为 `sha256:97bf5081cf717caa7638be4820e149cd63a11178814f2dcf0fb5951828a78689`。
源码提交 `0dc3124` 的[最终 Linux CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/39003589699)
全部通过：后端 478/478、前端 97/97、执行器认证 3/3、开发与双进程 8/8（含父测试，
约 77.6 秒）、最终执行器产物 HTTP 3/3、Caddy 7/7、112 文件语法、类型检查及
三个业务镜像构建。无平台跳过；Nuxt SIGTERM 子场景约 42 毫秒完成普通 EOF
和正常退出，执行器保持原 PID。这里是合成测试结果，不是 bob 上的重启耗时承诺。

## N02 登录与管理写入入口

Nuxt 正式路由承接密码登录、注销、Discord 授权开始及回跳，成员管理、份额配置，
以及 owner 和私有 admin 已有的账户更新与额度刷新。仍由执行器验证原 Host、
Origin、Cookie、管理员 Key 和 owner 身份，持久会话、OAuth 临时 state、资格
刷新及业务事务不复制到 Nuxt。OAuth 回跳的跨站例外仍仅限原路径及一次性 state
与浏览器 Cookie；Nuxt 不跟随 303，原 Location 与多条 Set-Cookie 原样返回。

POST/PATCH 复用有界正文缓冲和原字节转发，执行器继续执行各接口原有 JSON、
字段和大小校验。只有异步生图受理允许自动同键重试；登录或管理请求即使带有
Idempotency-Key，也不因此获得重试或缓存语义。响应丢失时操作可能已经提交，
应先查询现状；Nuxt 不自动重发调额、转账、邀请或成员管理写入。409 原样返回，
前端继续保留草稿与版本冲突提示。

普通有限请求继续使用 45 秒截止。私有 `POST /api/admin/accounts/quota` 的既有
批量刷新按五个账户并发、每账户 15 秒截止，总时长随数量增长，因此不套统一
45 秒总限；客户端断线时停止内部 HTTP 等待，已经开始的操作仍由执行器完成，
不会自动重试。单一共享账户的 owner 刷新仍
使用普通截止。Nuxt 不配置上游凭据，但授权后的管理请求和响应可能携带 PST，
按既有契约传输且不另行记录或缓存。

N02 阶段 Caddy 按方法和精确路径转给 Nuxt，私有管理只在回环入口开放。开发环境
使用正式路由，当时生图和请求取消仍直达执行器；N03 后统一入口，不增加第二个
账本或队列。测试使用合成成员及模拟 Discord/NAI，禁止携带真实上游配置。

本地前端 104/104、类型检查和 112 文件语法检查通过；真实 Caddy 分流 9/9。
开发及双进程产物测试共 11 项，Windows 10 通过、1 项 Linux SIGTERM 跳过，
约 82.9 秒；新增三组覆盖密码会话七天及注销持久性、Discord 回跳跨 Nuxt 重启
和 owner 十二小时、所有新增管理路由的权限/正常操作/400/413/409，以及丢失
管理回执只提交一次。未用接口 mock 替代正式 Nuxt 路由或执行器的真实 SQLite。
网页镜像构建通过，候选摘要为
`sha256:02b4d350562938acb8a60bb1a8a80d1842ddf407595008d81c9a7d953a09df95`。

源码提交 `3b3b252` 的[最终 Linux CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/39004774305)
全部通过：后端 478/478、前端 104/104、执行器认证 3/3、开发与双进程 11/11
（含父测试）、最终执行器产物 HTTP 3/3、Caddy 9/9、112 文件语法、类型检查与
三个业务镜像构建。Linux 无跳过，包含 Nuxt 正常退出与执行器持续运行的信号检查。

本片不改变 schema、依赖或 Cookie 范围；退回前一入口分流不需要恢复旧数据库，
仍须保留单一执行器和最新账本。目标机部署、真实浏览器验收与生产替换尚未发生。

## N03 生图与任务传输

网页异步任务使用正式 `api/jobs.post.ts` 与既有同键受理重试；任务/父请求查询、
取消、成员 Cookie 取消、钱包和私有健康检查继续使用有限请求客户端。图片领取、
任务预览 SSE、Native、OpenAI、direct 与订阅别名进入 Nuxt 的正式路由，长响应
使用独立 `forwardExecutorStream`。API 命名空间的回退仍交给执行器共享路由
白名单，保留未支持路径/方法的原 404、OPTIONS/CORS 与错误包络；不会借 Nuxt
新增本来被禁用的旧接口。

真实产物对照发现 Nuxt 的网页兜底会把错误 API 方法改成框架错误 JSON。入口
中间件直接复用原 `sharedSurface` 纯模块，将 HEAD、OPTIONS 和该表不支持的
API 请求交回执行器最终判定；不复制白名单或在 Nuxt 重写业务错误。开发内联与
网页镜像同时携带该纯模块，测试逐项比较直连执行器与 Nuxt 的状态、正文和 CORS。

长传输复用原 Host/Origin/身份、内部密钥覆盖与响应头过滤。上传用 Node pipe、
响应用 pipeline 的背压逐块转发，Nuxt 不再完整缓存正文或图片；JSON、multipart、
32 MiB 原始载荷与规范化载荷预算仍由执行器验证。同步生图与结果下载不套普通
45 秒截止，也不自动重发；原 direct/Native/OpenAI 等待期限继续由执行器控制。
内部连接或服务密钥失败在未发头时返回不含凭据的 503；已开始的二进制响应失败
会中断连接，不把截断结果当作完整成功。执行器返回的业务响应保持原样。

客户端断线和 Nuxt 退出只结束内部 HTTP 等待。已持久受理的任务由执行器继续
派发和结算，重启网页后按原任务与成员权限领取。Nuxt 收到退出信号时，预览或
OpenAI SSE 使用普通 EOF，部分二进制中断；不会伪造完成帧、自动重试生成或
调用任务取消。成员状态流继续使用 N01 的内部重连与 resync，任务预览只保留
执行器原有的最新内存帧，不新增持久事件历史。

Caddy 只区分 API 与网页的压缩策略，全部转给 Nuxt；公网上的管理拒绝与私有
回环入口不变。Caddy 不再连接执行器，也不需要内部密钥。未知 API 的协议拒绝
由同一执行器处理，避免维护两份路由白名单。首页 `/index.html` 与现有 HTML
别名重定向到 Nuxt 页面；这仅完成入口迁移，不表示旧 JS 业务已完成 T01 类型迁移。

验证覆盖正式开发与构建路由，不再注入测试专用 handler：

- 32 MiB multipart 的真实参考图、Content-Length 与 chunked 边界，超限 413 后账本不变；
  另保留上传尚未完成时提前拒绝的场景，该测试连接不复用。
- Native 四别名共享同键结果，ZIP/MessagePack、Vibe 原始字节与放大结果逐项领取；
  OpenAI 首帧和真实上游预览在最终图释放前到达，等待超过45秒仍可完成。
- Cookie/Bearer 任务查询与取消、跨成员隔离、签名图、direct 原成功与超时图片；
  只杀 Nuxt 或客户端断开后，执行器继续结算，同键领取不增加派发。
- Node 流背压与提前拒绝、服务关闭后的 SSE 普通 EOF/二进制中断；固定 Caddy
  产物验证统一入口、原始身份头、公私管理隔离、API 不压缩与首帧即时到达。

源码提交 [e7b9407](https://github.com/Hime-Hina/novelai-shared-gateway/commit/e7b9407) 的
[Linux CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/39007009293)
通过后端478、网页112、执行器认证3、开发与双进程14、打包产物HTTP3和Caddy7项，
全部无跳过；112文件语法、两端类型检查与三个业务Docker镜像构建也通过。
双进程测试实际覆盖 Linux SIGTERM 同时结束成员状态和任务预览 SSE，执行器继续运行。
以上全部使用合成数据和模拟上游，不包含真实 NAI、生产浏览器或 bob 的部署验收。

## 当前部署与剩余验收

N03、C03 当前支持范围与计划内 C01 已完成；T01 最后入口批次正在进行发布产物验收。
bob 已运行相互独立的 systemd 测试服务，使用 CI 的 Node/Linux 产物、独立持久目录和
清理后的生产规模副本。systemd 强杀自动恢复、网页与执行器生命周期隔离及同库版本
回退已通过，详细证据与剩余性能验收见[测试部署说明](test-deployment.md)。

现有业务由同一个执行器处理，不能让旧 CLI 与独立执行器同时挂同一数据目录。
当前生产未切换；最终版本仍须 D01 验收，不能保证找回上游尚未完整返回的图片。
