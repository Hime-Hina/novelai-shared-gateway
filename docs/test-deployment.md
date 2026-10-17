# D01 隔离测试部署准备

本文件与 `deploy/systemd/` 只提供测试版的交付约定和待执行步骤，尚未部署到 bob。
常驻 mock 的脚本与 unit 已准备，生产副本脱敏、安装和实机验收仍待完成；不得因模板
或 CI 产物存在就启动生产副本，也不能据此替换生产。测试通过后，生产切换仍需
用户针对明确版本另行允许。

2026-10-17 的只读复核：bob 为 Debian 12／x86_64、2 CPU，可用内存约
1897 MiB、剩余磁盘约 14.58 GiB，19080／19300／19081 当时空闲，生产 active，
尚无测试目录。这是采样结果，安装前须重新核实；不改生产 Node 或服务配置。

## CI 产物

`Gateway checks` 的 `offline-validation` 等待 `caddy-routing` 成功，现有测试、
类型检查、镜像构建及最终执行器 HTTP 验收全部成功后，才导出测试发布包：

```text
novelai-gateway-test-<commit>-linux-x64/
  bin/node                         # 同一执行器镜像的 Node 22.23.3
  executor/.output/                # Nitro 入口、业务文件、worker、SQL/journal
  executor/node_modules/           # 最终执行器镜像的完整生产依赖
  web/.output/                    # Nuxt 生产产物
  licenses/                       # Node、网页依赖许可文件和第三方说明
  systemd/                        # 执行器、网页与 mock 三个测试 unit
  docs/test-deployment.md
  RELEASE.txt                     # 提交、平台、Node、镜像 ID、锁文件 SHA-256
  pnpm-lock.yaml
  LICENSE / NOTICE.md / UPSTREAM-LICENSE
```

执行器依赖原许可随其完整依赖树保留；Nitro/H3 许可位于其输出目录。
网页镜像从冻结安装的 Linux 依赖树收集名称匹配 LICENSE、COPYING、NOTICE 的
文件，保留包路径后导出。此处记录实际携带的文件，不作额外的许可合规认证。

导出只从已验证镜像复制上述目录，不包含数据库、运行时配置或运行时测试密钥，也不
复制宿主 node_modules。用原生 `docker cp` 和 `tar` 保留 pnpm 相对符号链接，
不使用解引用选项；CI 在 Linux 重新解包并检查原生 SQLite 加载、迁移和完整性。
不要经 Windows 解包再重新压缩，也不能只复制 executor/.output 而漏掉生产依赖。

Artifact 为 `.tar.gz` 与同名 `.sha256`，保留 14 天。上传固定使用官方
[`actions/upload-artifact` v4.6.2](https://github.com/actions/upload-artifact/releases/tag/v4.6.2)
提交 `ea165f8d65b6e75b540449e92b4886f43607fa02`。tar 内保存可执行权限和链接；
下载 Actions 外层归档后，在 Linux 验证 `sha256sum -c <包名>.tar.gz.sha256`，
再用 `tar -xzf <包名>.tar.gz`。选择整个 workflow 成功且对应目标提交的产物。

## 目录、用户与配置约定

| 项目 | 测试值 |
| --- | --- |
| 只读版本目录 | `/opt/novelai-shared-gateway-test/releases/<commit>/` |
| 当前版本链接 | `/opt/novelai-shared-gateway-test/current` |
| 数据目录 | `/var/lib/novelai-shared-gateway-test`，仅执行器用户可写 |
| 执行器用户／组 | `novelai-gateway-test-executor` |
| 网页用户／组 | `novelai-gateway-test-web`，不加入执行器组 |
| mock 用户／组 | `novelai-gateway-test-mock`，不加入应用用户组 |
| 执行器配置 | `/etc/novelai-shared-gateway-test/executor.env` |
| 网页配置 | `/etc/novelai-shared-gateway-test/web.env` |
| 回环端口 | 执行器 19080、网页 19300、本地 mock 19081 |

配置文件用新生成的测试密钥，root 所有、0600，由 systemd 读取，不复制生产 .env。
执行器配置至少提供 `ADMIN_TOKEN`、`EXECUTOR_TOKEN`、测试 `PUBLIC_BASE_URL`、
`PUBLIC_HOST` 和精确的 `TRUSTED_PROXY_IPS`。初次仅经本机／SSH 隧道访问时使用
对应 localhost 原点与端口，不继承生产域名或 Discord 配置。两种 token 至少
32 字节；网页配置只放相同的 `NUXT_EXECUTOR_TOKEN`，不放 DATA_DIR、PST 或
Discord 凭据。内部地址和端口已有 unit 默认值。

unit 默认把生成与额度查询均指向 `http://127.0.0.1:19081`，并只允许回环 IP。
这些环境值可被配置文件覆盖，审核时必须确认仍指向 mock。还须在实机核验
systemd 的 IP 过滤确实生效；[Debian 文档](https://manpages.debian.org/bookworm/systemd/systemd.resource-control.5.en.html)
明确说明缺少内核支持时过滤可能不生效，不能仅凭配置文本宣称无外联。
保留测试 mock 的拒绝外联检查，清除副本中的真实上游代理地址，停止 mock 后应
失败而不回落到真实域名。新增 mock unit 尚未安装，不能假定 19081 已有服务。

两个应用 unit 无 `Requires`、`PartOf`、`BindsTo` 等联动关系；重启网页不停止
执行器。只有执行器打开业务 SQLite，旧 CLI 和其他执行器不能同时指向该目录。
默认执行器／网页 MemoryMax 为 768／384 MiB，CPUQuota 为 75%／50%，
MemoryHigh 为 512／256 MiB，均为待实测的可调初值，未将全部空闲内存分配给测试。
执行器默认应用宽限 30 秒，unit 停止超时 45 秒；若调整 SHUTDOWN_GRACE_MS，
须同步留足 systemd 停止时间。超宽限仍依赖已有恢复证据，不承诺找回 NAI 未返回的结果。

## 常驻合成上游

`scripts/mock-novelai.mjs` 只绑定 `127.0.0.1`，不读取 `.env`、数据库或外部服务，
不记录请求正文或凭据。它只接受公开的合成 PST `pst-synthetic-d01-mock-upstream-only`；
其他凭据一律 401。新合成库 bootstrap 的 `NOVELAI_TOKEN` 和脱敏副本内的上游账户
必须改成这一测试值；生成与账户查询 URL 都指向 `http://127.0.0.1:19081`。

| 路由 | 合成行为 |
| --- | --- |
| `GET /user/data` | 固定有效 Opus、10000 订阅 Anlas、0 购买 Anlas、100% V5 |
| `POST /ai/generate-image` | 延迟后返回固定 1×1 PNG，复用执行器现有 PNG 解码 |
| `POST /ai/generate-image-stream` | 使用现有 MessagePack 编码器，先发 step 0 预览，延迟后发最终 PNG |

生成正文限 32 MiB、JSON、单图；执行器 Native 批次仍逐子任务调用。错误方法、正文
和超限分别返回 405／400（类型不支持为 415）／413。其他路径直接 404，包括放大、
Vibe 和 trial 查询。余额不随调用变化，图片也不模拟所选分辨率、模型或收费；这些
测试响应不能用作官方契约、计费正确性或真实生成质量的证据。

包内手动启动命令（本文件仅记录，尚未在 bob 执行）：

```sh
/opt/novelai-shared-gateway-test/current/bin/node \
  /opt/novelai-shared-gateway-test/current/executor/.output/server/node_modules/novelai-shared-gateway/scripts/mock-novelai.mjs \
  --port 19081 --delay-ms 1000
```

延迟可在 0–60000 毫秒内调整，用于排队、网页独立重启和执行器恢复验收；没有故障
场景控制接口。端口可调整，绑定地址不可调整。mock unit 使用第三个无登录用户，
不能访问应用配置与数据目录；默认 MemoryHigh／MemoryMax 为 128／256 MiB、
CPUQuota 25%。停止时等已接请求，65 秒后关闭剩余连接，unit 留 75 秒；与应用
unit 无停止联动。此处不代表已完成 systemd 常驻或整套应用验收。

## 待执行顺序

首次测试部署使用全新合成数据，先验证登录、权限、任务、流式响应、队列和重启恢复，
不以取得或脱敏生产副本为前提。生产规模副本是后续独立验收阶段：仅在完成一致性
备份、离线脱敏与真实活动任务隔离后使用，用于历史规模、迁移、资源和性能检查。
初始合成验收通过不等于副本验收或生产切换通过，两阶段均保持生产服务原样运行。

1. 选择一个 CI 全部通过的测试提交；下载、校验、在 Linux 解包。不必等所有
   T01/C01 迁移完成才首次部署，最终完整验收仍须覆盖剩余迁移。先确认
   `bin/node --version` 为 v22.23.3、平台正确；保留旧测试发布目录以便回退。
2. 创建上述三个无登录测试用户、root 所有的版本／配置目录及执行器所有的
   0700 数据目录。发布文件只读，current 指向本次测试版本，不动生产链接。
3. 核对 mock 及两个应用的回环地址、合成 PST 与无外联配置。先用现有 bootstrap
   创建全新合成数据；初始化连接关闭后才能启动执行器。不要对已有副本再次运行
   bootstrap；不在 shell 历史或验收日志中记录新生成的成员和管理密钥。
4. 后续需要生产规模数据时，用 SQLite 在线 backup 留受保护原始快照，再制作离线
   工作副本；不要只拷贝正在写入的主文件而遗漏 WAL。完整脱敏冷表、会话、PST、
   Discord 授权、分发 Key 与代理配置，隔离真实活动任务及预留状态，再启动应用。
   不使用部分热缓存做脱敏；只复制必要图片并核对引用。此步骤尚无已交付脱敏脚本。
5. 在上述前置完成后，将包内三个 unit 安装到 `/etc/systemd/system/`，执行
   `systemd-analyze verify` 和 `systemctl daemon-reload`。先启动测试 mock，确认
   合成凭据查询成功、其他凭据被拒绝，再启动测试 executor；健康及 mock 额度
   确认后启动测试 web。停止 mock 验证应用报错且无外联回退，再恢复；此时仍不
   enable 开机启动。
6. 经独立隧道访问 19300，验证成员／owner／私有管理员、图片和跨成员权限。
   如增加测试 Caddy，使用独立配置及入口，保留原 Host/Origin；不 reload 生产 Caddy。
7. 核对唯一写入者；演练网页独立重启、执行器正常关闭及强杀恢复、systemd 自动
   拉起、持久磁盘重开和测试版本回退。记录资源峰值、磁盘增长、首屏网络与排队阶段。
   通过后才考虑启用这三个测试 unit，并交付明确测试入口及验收记录。

生产继续原版本运行。生产批准前不得替换其 unit、端口、数据或账户调度器；批准后
另走 D02 的停接单、排空、最新一致性备份和单调度器切换。新版本已有真实写入后，
不能用旧备份覆盖新账本来回滚。

## 当前验证状态

本轮工作流 YAML、导出 shell 语法和 unit 结构检查通过。现有 Linux 环境的
`systemd-analyze verify` 已分别通过三份临时 unit 副本（仅将未安装的 ExecStart 换为
`/bin/true`）；这不代表用户、目录、IP 过滤或资源限额已经在 bob 上验证。
CI 另外把临时 unit 的发布路径替换为实际产物目录后进行静态检查，不安装服务。

常驻 mock 的 Node 22.23.3 集成测试 4/4 通过：真实 provider 的账户查询、PNG、
MessagePack 预览／最终结果、独立流解码、固定余额、凭据拒绝、正文上限、断线后
继续服务及启动参数边界；`node --check` 通过。最终执行器 Linux 镜像已重建，
在断网、只读容器中从 unit 所用的包内脚本路径启动 mock，真实 provider 的鉴权、
额度、预览和最终图片验证通过；同镜像的执行器 HTTP 3/3 通过，无跳过。
CI unit 校验列表已加入 mock，含 mock 的新 CI 和 bob 安装仍待完成。

网页许可证镜像构建通过，保留 593 份匹配的许可／声明文件。使用已验证的现有
执行器镜像与本轮网页镜像，只在断网 Linux 容器内复制、tar 压缩及重新解包，
Node 可执行权限 0755、v22.23.3、包内原生 SQLite／迁移／完整性和 Nuxt SSR
HTTP 200 均通过；未经过 Windows 解包，未读取宿主 node_modules。
这仅验证交付布局，不是最终 T01/C01 版本或持久磁盘的完整验收。

发布包准备提交 c1885ba 的完整 [CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/39021213548)
已成功，导出并上传约 52 MB 的 Linux artifact；它尚不包含本轮新增 mock。

待补：包含 mock 的最终 CI artifact、bob 上 mock 常驻启动、副本脱敏、安装与实机验收；本文没有
执行这些部署操作，也未改动生产服务。
