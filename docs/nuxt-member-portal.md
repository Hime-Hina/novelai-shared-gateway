# Nuxt 成员页第一阶段（U01）

本阶段在独立的 `web/` workspace 中交付成员页面。现有 Node 网关继续负责
会话、Discord 资格检查、成员 Key、额度账本、SQLite、队列、生图和 SSE；
Nuxt 只负责网页渲染和浏览器端数据加载。U02 已继续迁入 owner 与私有管理
页面，当前入口见 [Nuxt 管理页](nuxt-management-portal.md)。后续 F11/N01 已加入
[独立执行服务和查询入口](executor-service.md)；下文 U01 验证与性能记录保留为历史证据。

## 当前选型与目录

直接依赖使用精确版本，由根目录唯一的 `pnpm-lock.yaml` 锁定：

| 用途 | 当前版本 |
| --- | --- |
| 网页框架 | Nuxt 4.6.2 |
| 组件框架 | Vue 3.5.44、Vue Router 5.4.0 |
| 组件库 | Naive UI 2.45.3 |
| 类型检查 | TypeScript 6.0.3、vue-tsc 3.3.13 |
| Naive UI SSR 样式收集 | `@css-render/vue3-ssr` 0.15.14 |
| 真实 Nuxt 组件测试 | `@nuxt/test-utils` 4.3.4、Vitest 5.0.5、Vue Test Utils 2.5.1、happy-dom 21.0.3 |
| 包管理器 | pnpm 10.32.1 |

`web/app/components/` 分离登录、个人额度、共享状态、Key、调用记录和批次；
`web/app/composables/useMemberPortal.ts` 管理成员页查询及会话生命周期；
`web/app/types/member.ts` 定义浏览器可见 DTO。组件和组合式函数测试放在
`web/tests/nuxt/`，使用 Nuxt 原生 TypeScript 上下文。未增加 Pinia、Vue Query
或第二套 UI 库。

`web/app/plugins/naive.server.ts` 是小型 SSR 样式适配：用 css-render 收集
当前请求的 Naive UI 样式，写入 Nuxt head，并保留 `cssr-id`，让浏览器接续
渲染时识别已有样式。它改编自
[nuxt-naiveui 的固定提交 3452646](https://github.com/becem-gharbi/nuxt-naiveui/blob/345264658e2b4dabdb30be886463e101fd8d6175/src/runtime/plugins/naive.server.ts)，
没有安装完整模块。原 MIT 版权及许可全文保留在
[web/THIRD_PARTY_NOTICES.md](../web/THIRD_PARTY_NOTICES.md)，前端镜像也携带该文件；
项目自身许可仍见根目录 [LICENSE](../LICENSE) 和 [NOTICE.md](../NOTICE.md)。

完整 Nuxt 集成模块不是使用 Naive UI 的必要条件；本项目继续直接使用它的
按钮、输入框、卡片、表格等组件，仅保留首份 HTML 所需的样式桥接。没有为
自动导入、图标或主题扩展另外安装完整模块，也没有替换 Naive UI。

## 数据、会话与交互边界

- Nuxt 不打开业务 SQLite、不挂载生产数据目录、不配置 NAI PST 或 Discord
  refresh token，也不启动队列调度器。网页热更新或重启不会另起一个账本写入方。
- 私人 profile 使用真实 `useAsyncData`，设置 `server:false`、`immediate:false`、
  `serialize:false`；浏览器挂载后才读取，不写入 SSR HTML 或 Nuxt payload。
  公开 `login-options` 与本人 `me` 并行读取；匿名 `me` 的 401 不丢弃公开登录配置。
- 沿用 [A01 固定会话](member-sessions.md)：普通成员 7 天、owner 12 小时，不滑动
  续期。Cookie 仍限于 `/api/member`，不为首页 SSR 扩大范围；写请求的同源与
  Origin 校验、owner 归属检查仍在网关完成。
- API Key 只在点击查看时读取，隐藏、组件卸载及会话失效后清空展示；迟到响应
  不能重新显示已隐藏的 Key。退出网页不撤销调用 Key。
- 调用记录沿用现有状态和分池显示，同一父请求展示一份批次。展开批次后才
  查询，通知期间保持单个在途读取并合并补读；取消通过明确的 POST 操作，只
  取消 queued/pending，running 继续执行，已完成及迟到结果仍按网关权限领取。
- 直接复用 `public/member-events.js` 的连接生命周期与
  [R01 协议](state-events.md)。共享通知不触发个人资料查询；本人失效或 resync
  才刷新额度及调用记录。查询进行中收到多次通知，完成后补读一次。
- 可见页面每 30 秒补充读取本地额度估算；隐藏或离开页面停止计时与 SSE，
  恢复可见后重连并通过 resync 校准。503 保留上次 profile 并显示错误；确认
  会话失效才清空私人状态。旧会话迟到的成功或 401 不能恢复旧视图或注销新会话。

## Node 与本地命令

构建和验证基线为 **Node 22.23.3**。`web/package.json` 要求
`^22.23.3 || ^24.15.0 || >=26`；此前记录的 bob Node 24.14.0 不满足当前前端
要求，不能直接拿来运行该前端版本。本阶段没有升级 bob 或替换其服务。

下列命令在仓库根目录、已选中 Node 22.23.3 的终端执行：

```sh
corepack pnpm install --frozen-lockfile
corepack pnpm --filter @novelai-gateway/web prepare
corepack pnpm --filter @novelai-gateway/executor build
corepack pnpm --filter @novelai-gateway/web dev
```

开发服务绑定 `127.0.0.1:3000`。当前查询与状态流使用正式 Nuxt 服务端路由，
已删除会抢先接走请求的 `/api` devProxy；`GATEWAY_DEV_URL` 不再使用。
在另一个终端配置执行器的隔离环境（绝对 `DATA_DIR`、`HOST=127.0.0.1`、
`PORT=18081`、管理员及内部密钥）后，运行 `node executor/.output/server/start.mjs`。
入口不会自动读取 `.env`，可用 Node 的 `--env-file` 指定隔离配置文件。
不要同时运行旧 CLI 和新执行器并指向同一数据目录。

Nuxt 开发终端仅设置 `NUXT_EXECUTOR_BASE_URL=http://127.0.0.1:18081` 和
`NUXT_EXECUTOR_TOKEN`。开发烟测会先检查 `web/.env` 不存在，以免测试工具默认
加载真实开发凭据；存在时明确退出，不读取或改动该文件。日常开发与生产都推荐
使用各自的进程环境。Caddy 另获得相同的 `EXECUTOR_TOKEN`，
按示例提供同源入口；本地密码模式可从 `http://127.0.0.1:18082/` 使用成员页。
Discord 开发仍需其已配置的 HTTPS Origin，不能放宽同源校验。

```sh
# 前端检查、测试与构建
corepack pnpm --filter @novelai-gateway/web typecheck
corepack pnpm --filter @novelai-gateway/web test
corepack pnpm --filter @novelai-gateway/web build

# 本地检查已经构建的 Nuxt 产物
corepack pnpm --filter @novelai-gateway/web start

# 原网关测试入口保持独立
corepack pnpm test
corepack pnpm check
```

Nuxt 输出在 `web/.output/`，运行入口为 `.output/server/index.mjs`。
Windows 使用 fnm 时可将命令写成
`fnm exec --using 22.23.3 corepack.cmd pnpm --filter @novelai-gateway/web test`。
若 pnpm 被本机已失效的代理配置阻断，可仅对安装命令加
`--proxy=false --https-proxy=false`；不修改用户全局配置。

## 生产路径分流示例的含义

[Caddyfile.example](../Caddyfile.example) 只描述将来独立服务的同源分流：

| 路径 | 所属服务 |
| --- | --- |
| `/`、`/account.html`、`/_nuxt/*` | Nuxt 网页服务；旧 `/account.html` 重定向到 `/` |
| 成员 `login-options/me/key/shared-status/heatmap/requests/:id` 的 GET、owner 查询和成员 `shared-events` | Nuxt 内部客户端 → 同一执行器；业务授权与 DTO 仍由执行器负责 |
| 其余 `/api/*`、`/ai/*`、`/v1/*`、`/user/*`、`/generate`，包括登录、管理写入及取消 | Caddy 添加内部认证头 → 同一执行器的原协议 handler |
| `/owner`、`/owner/`、`/owner.html` | Nuxt 管理页；执行器验证 owner 会话 |
| `/admin*`、`/api/admin*`、`/api/health`、`/memory` | 公网入口直接拒绝 |
| 私有回环 18082 的成员/owner/admin 页面及合法别名、`/_nuxt/*` | Nuxt；管理 GET 经 Nuxt，写操作仍直达同一执行器，详见 [管理入口](nuxt-management-portal.md#入口与分流) |

示例网页服务使用回环端口 3000、网关使用回环端口 18081；实际端口由隔离
环境配置。代理保留公共 Host，网关按实际来源配置 `TRUSTED_PROXY_IPS`，
不把公共请求改成 localhost 来绕过私有管理边界。SSE 不缓冲，不启用记录
含 Key 查询参数的访问日志。私有管理使用另一个仅绑定回环的 Caddy 监听
18082，SSH 隧道转发到该入口，并保留浏览器原 Host/Origin；不修改公网权限。

`web/Dockerfile` 用 Node 22.23.3 分阶段构建，运行镜像只复制 Nuxt 产物和
许可声明，默认 UID/GID 为 1000。它是可选构建与隔离验收方式，bob 的正式
运行仍按已确认的独立 systemd 服务规划处理。本说明不包含生产启动、Caddy
reload 或替换生产版本的操作；实际替换仍须用户批准。

`compose.yml` 的 `web` 独立绑定回环 3000，仅获得内部地址和内部密钥，不读取
完整 `.env`、不挂载数据、不依赖 gateway 的启停；可单独重启网页。`gateway`
服务名保留，但镜像已改为独立执行器，避免另起第三个写库进程。`web` 的
512 MiB 上限是测试配置，不是 bob 的峰值测量结果。完整页面仍通过同源分流
访问，3000 尚未承接登录、管理写入及全部生图接口。

## 已完成验证

2026-10-17 的 U01 验证已完成：

[实现提交 ec011b6](https://github.com/Hime-Hina/novelai-shared-gateway/commit/ec011b6)
的[托管 CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/38989723903)
已通过：后端457/457、前端20/20、105文件语法、Nuxt类型检查、两个Docker构建。

- 真实 Nuxt 环境的组件、展示与组合式函数测试 **20/20** 通过，前端 typecheck
  通过。`mountSuspended` 使用真实 Vue 与 `useAsyncData`，仅替换请求和
  EventSource 传输边界；Nuxt 自动导入的 `$fetch` 用官方 `mockNuxtImport` 替换。
- 前端 Docker 构建及隔离检查通过；网页服务与业务数据库、上游凭据保持分离。
- 合成浏览器流程覆盖 3 次生成、1 次取消、Key 查看/隐藏及注销。它们使用隔离
  合成数据和模拟响应，不代表真实 NAI 或生产环境验收。
- Node 22.23.3 下后端全量 457 项中 454 通过、3 项 Windows 平台跳过，105
  文件语法检查通过；后端与前端生产 Docker 构建均通过。网页产物不含 SQLite、
  Drizzle 或业务数据库，UID 1000，只读容器可运行。
- 开发代理实际验证 12 条路径及 POST body、Host、Origin、Cookie、Set-Cookie、
  SSE；修复 `/owner.html` 被误转成 `/owner/.html`，保留旧入口别名。Caddy
  合成验收 19 项通过，包括私有入口拒绝、静态压缩和 SSE 不缓冲。
- 手机宽表只在表格内滚动；会话失效的迟到响应与批次取消竞态由 Nuxt 测试覆盖。
  Chrome 最终页面无 hydration 警告，初始 HTML 有 Naive UI 服务端样式，
  不含合成用户名或私人 profile；Nuxt `_errors` 中空查询标记不属于私人数据。

## 首屏复测（2026-10-17）

沿用 [P01 基线](browser-baseline.md) 的 Chrome 155 / Windows、4× CPU、150ms
网络延迟、下行 200000 B/s、上行 93750 B/s、1440×900 与 390×844、DPR≈1。
匿名/成员 × 桌面/手机视口 × HTTP 缓存禁用/启用，各五次，共 **40/40**。
使用真实网关、四名合成成员、120 条终态记录；本轮性能采样生成次数为 **0**。
保留原浏览器扩展，不是全新 profile、真实手机、生产用户分位数或严格隔离实验。

本轮网页经过与 Caddy 示例相同的 `encode zstd gzip`，API/SSE 仍直达测试网关；
旧基线无静态压缩或缓存，比较包含这两项配置改进，不只比较框架。多一段本机
容器代理也会影响 TTFB。采样固定镜像为
`sha256:b9a0775852f4ea61bbff48182293f452197bd8ac71e580b9aa992de1a2c14663`；
基于 `b4ecf3a` 加本次 U01 改动，源码、helper、锁文件另有完整指纹。

保留同一 `startProbe/readProbe`、关键 DOM 条件和 `max(secondary,FCP)+1000ms`
观察窗；在 DOMContentLoaded 后注入，立即返回，不用阻塞 promise 等绘制。
40 条均可见、完整、无 ready 上界样本，所有分组与 run 1..5 唯一。
CLS 按 P01 最大 session window 重算。FCP 是加载壳首次显示，**不表示私人
数据已经可用**；LCP 也只代表本观察窗。两轮时段、工具开销和宿主机负载不同，
下面是受控观察，不将全部差值归因于某一行代码或 SSR。

单位 ms；新版为中位数（最小–最大），旧值为 P01 同组关键 DOM 中位数。

| 角色·视口·HTTP缓存 | 新 FCP | 新窗口 LCP | 新关键 DOM | 旧关键 DOM | 新窗口 CLS |
| --- | ---: | ---: | ---: | ---: | ---: |
| 匿名·桌面·禁用 | 792（704–832） | 2620（2472–2752） | 2498（2315–2675） | 3177 | 0.0053 |
| 匿名·桌面·启用 | 736（700–940） | 1752（1580–2200） | 1626（1568–1962） | 3132 | 0.0053 |
| 匿名·手机·禁用 | 720（684–836） | 2464（2368–2516） | 2326（2267–2396） | 3164 | 0.0302 |
| 匿名·手机·启用 | 748（592–860） | 1756（1284–2276） | 1648（1232–2097） | 3169 | 0.0302 |
| 成员·桌面·禁用 | 604（584–792） | 2264（2140–2464） | 2151（2120–2344） | 3227 | 0.0198 |
| 成员·桌面·启用 | 596（560–636） | 1416（1352–1652） | 1294（1260–1514） | 3942 | 0.0198 |
| 成员·手机·禁用 | 572（544–640） | 2232（2148–2276） | 2156（2099–2208） | 3644 | 0.0410 |
| 成员·手机·启用 | 588（548–764） | 1424（1364–1604） | 1325（1284–1494） | 3258 | 0.0410 |

HTML TTFB 范围 20–113ms，八组中位 22–49ms；仍未复现生产历史约29秒等待。
首批 `login-options` 与 `me` 起始差范围 0.8–12.6ms，已并行；匿名只查询一次
`me`，成员另一次来自 SSE 初始 resync，用于闭合首次查询到订阅间的变化窗口，
不是轮询反馈环。所有业务/资源响应均为200，匿名 `me` 的401除外。

代价是首次资源更大：匿名11次静态请求合计183028 B、成员12次合计184089 B
（包含响应头开销；成员的共享事件动态模块在禁用缓存时再次传输）。旧成员页
三份静态资源约12KB。新版启用缓存的所有静态条目 `transferSize=0`，均命中
缓存；带哈希资源有一年 `immutable` 缓存。私人 API 与初始 HTML 不靠公开
缓存提供。没有因此加入第三方字体、图标库或额外状态库。

初轮40条诊断样本发现桌面内容增长产生滚动条，整块居中 `main` 横移并伴随
页脚退出首屏，CLS约0.8465；一行 `scrollbar-gutter:stable` 后重做上述完整矩阵，
桌面成员CLS降到0.0198（旧版约0.0281），手机0.0410（旧版约0.0474）。匿名
旧版CLS为0，新版占位切换仍有0.0053/0.0302位移，是保留的代价。显式空图标
声明也消除了浏览器缺省 favicon 的404请求。初轮、预采样与最终40条互不混用。

私有原始文件不提交公开 fork：`_private/u01-browser-final-samples.json` 的
SHA-256 为 `29a32bb16bb4265423bb1d333f159233c26bfca4260a49a828a3f6140e8055e9`；
环境记录为 `a9cd637895ea48f26b96e2e642c5447494c592831e9e6bd41ca669f5d884aed9`；
源码指纹清单为 `59004ceb9a6421cc4c528eecf61ecfc79ec02e4e189d7bc83fedb0bfd1a0a9ad`。
核算脚本检查分组、状态、视口、缓存命中与CLS窗口；前述异常原始记录仍保留。

未进行真实 Discord OAuth、真实 NAI 或 bob 上的峰值/故障部署验收。单次本机
网页容器空闲内存快照约75MiB，不作为生产容量保证。后续 U02/U03 继续验收管理
页和热力图的加载成本，D01 排查生产链路长等待；生产服务未切换。
