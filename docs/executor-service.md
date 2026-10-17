# 独立执行服务（F11，实施中）

目标是让 Nuxt 网页/API 与任务执行拥有独立进程生命周期。执行服务独占 SQLite
写入、任务调度、上游连接、结果文件、会话和 Discord 令牌刷新；Nuxt 不另建
数据库缓存或调度器。断开 HTTP 等待不取消持久任务，结果仍可按成员权限领取。

## 已抽取的运行时

`server/gateway-runtime.js` 保留现有业务与文件布局，仅提供两个入口：

- `handleGatewayRequest(req, res)`：完整的原 Node handler，包含访问守卫、
  各协议错误响应及载荷预算的 finally 释放，不只导出内部 router。
- `startGateway(handler)`：初始化数据库，先恢复完整结果再清理中断任务，随后
  监听端口；监听成功才启动额度刷新、未知核对和队列派发，并安装统一关闭流程。

`server/index.js` 仍是兼容启动入口，调用同一运行时。导入运行时本身不会建库、
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

构建后的进程入口是 `.output/server/start.mjs`，类型检查覆盖新配置、认证与
适配器；旧 JS 通过显式入口声明接入，不声称已经完成 T01 全后端类型迁移。
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
Nuxt 不接收 DATA_DIR、上游 PST 或 Discord refresh token，不启动存储或调度器；
仅复用已有的纯内存载荷预算函数。默认配置为空，未配置时返回 503。

当前客户端支持有限 GET/HEAD 查询与 `/api/jobs`、`/api/web/jobs` 的异步受理。
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
新双进程场景已加入托管 CI，Linux 结果须以对应提交的 CI 为准。

截止时间复核发现 Discord 刷新与资格查询各允许 10 秒，后续冷查询还可能等待
15 秒，因此不能使用更短的统一 15 秒外层截止。现改为 45 秒，仍不用于长期 SSE
或大图流式传输。16 秒受理响应回归在旧截止下错误提交两次，修复后只提交一次；
修复后完整前端 89/89 通过。该测试模拟正常的慢受理，不据此宣称已经验证真实
Discord 慢调用链；N01 仍须针对并发刷新、令牌持久化和失败状态验收。

`cffd205` 阶段候选镜像：网页 `sha256:7f513cb92c214da533872df7a25113a10a0c0e79fdec6603f698f292719901e9`；
执行器 `sha256:271082157eefc6a4821eb47a5e46eec37f1c7b876a29b58bf4bfdef1840b911d`。

## 尚未完成的迁移与部署

N01–N03 再按查询、认证管理、生图与长连接的顺序迁移网页 API；内部 SSE
重连必须让仍在线的浏览器重新校准。最终仍需独立 systemd 部署和 D01 隔离验收。
尚未验证 systemd 自动拉起、bob 的目标运行时与持久卷；测试中由测试进程重新
启动服务。当前生产未切换，本文不作为部署完成或任意故障都能找回上游结果的声明。
