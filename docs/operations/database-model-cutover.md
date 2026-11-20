# 数据库模型 B 切换操作单（待生产授权）

本单用于 [#53](https://github.com/Hime-Hina/novelai-shared-gateway/issues/53) 的执行准备，**尚未批准或执行生产停机、迁移、外存写删及切换**。命令按执行顺序供维护者逐项复核，不是可以整段无人值守运行的脚本。任何 `SELECTED_*`、大写路径变量或未签收门禁尚未填写时，停止在该步。

核心已随 #72 合入 main，迁移与剩余验收由 #73 承接；最终代码、迁移包和普通发行包的固定 SHA 统一填写在下述 manifest。历史 #49 候选 `0d16502` 的失败与后继修复只作追溯，不能作为最终执行版本。最终包必须实际包含所用入口及其依赖，不能把源码或另一版本脚本临时补进包。原核实的正式业务为 `3caba3dfa0b5d9f582acabfc02e36e7db4af038f`；实际版本、unit、端口和路由须在执行窗口重新核实并私有存证。

[已确认的 B、原键 410、会话保原期限、缺图 503](../history/database-model-migration-decision.md) 继续执行。退出 D4 旧在线模式，D8 缩短留存暂缓；本单不引入另一条转换路线、双写或旧模型在线开关。

## 1. 操作清单与放行条件

由维护者在受保护目录保存一份选定 manifest，至少填写以下字段；这里只提交字段说明，不提交真实身份、路径清单、URL、输入、Cookie、Key、PST 或环境原文。文件和 receipt 的 SHA 须由既有审查/交接独立选定，不能把同目录自算摘要当作认证。

| 字段组     | 必填内容与验收                                                                                                                                                                                                             |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 执行窗口   | UTC 开始/截止、负责人、通知及维护页恢复责任人；停受理、停服务、生产转换/安装、首次启动及真实烟测的明确授权记录                                                                                                             |
| 最终候选   | 同一个完整 Git/tree、普通 executor/web 实际 Linux tar 及外部 SHA、迁移工具 tar/`MIGRATION_TOOLS.json`、固定旧工具 tar/`LEGACY_TOOLS.json`、Node/ABI/native 摘要、远端审查与必需 CI 结果；当前 P1/CI 阻塞先关闭             |
| 运行配置   | `novelai-gateway.service`、`novelai-gateway-web.service` 及 drop-in、备份 timer/service、原/候选 `current`、绝对 `DATA_DIR`、实际监听地址/端口、unit 的 `ReadWritePaths`、Host/OAuth/内部 executor 鉴权及配置文件摘要/权限 |
| 最终旧 cut | 停写后的 Source 文件/SHA/长度、完整 commit cut、原发布、capture 前后时点和 sidecar/文件身份核对；独立 audit、inputs/artifacts 两个四元组和 inventory 文件/SHA                                                              |
| 原部署材料 | 新选定 cut-config/SHA、`sourceCapture`、`namespaceReceipt`、`identityReceipt` 各文件/SHA；所有原图、prepared final/part、归档及独立远端 hold 的保护位置；不是当前候选 namespace 推断旧来源                                 |
| 目标与恢复 | 全新 B 输出、逐项 reconciliation、封存新格式源 SHA/history/cut、公共与私有联合恢复、唯一 installer 成功 receipt、实际数据目录、ready 输出及 SHA                                                                            |
| 空间和预算 | 原库/sidecar/原图/归档不删；B 输出、恢复输出、shadow/临时文件、两份包、备份压缩/加密/回读、内存及维护窗口各自预算和实测上限；不足即停止，不能先删旧份腾空间                                                                |
| 故障与返还 | 每步实际开始/结束、退出码、私有 stdout/stderr、产物 SHA、是否出现新业务/认证事实或对象变化；停止点和向前修复负责人                                                                                                         |

`NODE` 为选定包自带且已校验的 Node 绝对路径；`LEGACY`、`MIGRATION`、`BACKEND` 为各自已校验解包根。`WORK` 是全新、独占、私有工作目录；`SOURCE`、`B_OUTPUT`、`READY_DATA`、`READY_DB`、`REGISTRY` 等路径只从上述 manifest 取得。Linux 目录/文件分别限制为 0700/0600，Windows 演练先核 ACL；不要将秘密作为命令参数或写入公开日志。

空间预算按仍保留的原件，加 B 全部输出、恢复库/原图、installer shadow、临时工作区及压缩/加密/回读的同时占用计算，不预扣去重或 CoW 收益。执行前记录 `df -B1`、`free -b` 和实际 unit/cgroup 限额；工具失败须保现场，因此另留失败余量。[完整材料实测](../history/database-model-final-acceptance-20261118.md)的 B 为 401.467 s／OS 峰值 1,635,426,304 B，联合恢复 430.774 s，唯一 installer 41,873.893 s（约 11 h 38 min）；16 h 运行预算不是整段维护窗口。此前较小材料测量只作历史证据，不能承诺秒级切换。

最近 Bob 清理后可用盘 3,575,296,000 B，不能再复制整套 4,232,262,764 B Local。当前发行验收已证明可复用独立数据目录内的完整 Local，但这不自动给生产目录搬移权限。最终 manifest 须列出新 cut 的每个 Local 来源、保留位置、实际 data 安装／权限步骤和同时存在的各份 DB／压缩／加密／shadow；新事实所在当前验收库也须独立保存。若按这些步骤和失败余量核算仍不足，停在节点 B 前补足空间，不能先删旧图、原库、归档或恢复依据腾空间。

先核 tar 的独立 SHA，再在相应包目录核包内文件：

```sh
"$NODE" "$LEGACY/scripts/audit-legacy-archive.mjs" --verify-package
"$NODE" "$MIGRATION/scripts/verify-migration-package.mjs"
```

包、Node/native 与运行平台不符即停止。最终运行包须使用其原构建/发布清单核验，不能用上面两个独立工具包的验证替代。核对最终 DDL/hash、writer/event/reducer；基线值为关系格式 v1、writer 5、event 8、reducer 1，最终 manifest 保存实际值及源码摘要，不手改版本标记。

## 2. 维护入口与真实排空

**授权节点 A：通知、关闭正式受理入口及管理写入。** 保存原路由完整配置，只将已确认的正式域名置维护响应，覆盖网页、Native/OpenAI/Direct，以及会产生任务的 `GET /generate`；同时限制绕过网页的直接 executor 入口和其他正式域名。不要停止共享 cloudflared 或影响测试域名。单独停止 web 不会停止已受理的生成。管理操作、人工刷新/轮换、其他写入者及新备份捕获也须纳入窗口；已开始的备份/归档 IO 应完成并登记，不能强杀后清 preparing。

旧 `3caba3d` 的现有只读观察入口如下。用预先受保护的 curl 配置提供原管理员鉴权及 executor 所需的 `X-Executor-Token`，不在命令行展示秘密；JSON 只写私有新文件，避免 `revealTokens=1`。

```sh
curl --fail --silent --show-error --config "$ADMIN_CURL" \
  --output "$WORK/old-management.json" \
  'http://127.0.0.1:18082/api/admin/member-management'
curl --fail --silent --show-error --config "$ADMIN_CURL" \
  --output "$WORK/old-summary.json" \
  'http://127.0.0.1:18082/api/admin/summary?fresh=1'
```

18082 已按旧实际 Linux unit／监听核实，执行窗口仍须重核。前者的 `shared.queued/pending/running` 来自当前共享成员任务；后者提供账户观察和汇总，**其 `jobs` 实际只返回最近 50 条 failed，不是活动全集**。组外任务、SQL 已 failed 但仍持有真实执行 control、官方直连任务均不能由这些计数排除。不要用停用成员/账户、重分配权益或修改 SQL 来“排空”。原 `member-admission` 命令会取消 queued/pending，并非纯暂停；确需取消必须逐项使用原业务命令并保存原回执，不能无声改成退款。

**实际实例观察已准备，生产观察／排空待授权执行。** 旧 `protocols/official-key-jobs.ts` 的 `jobs/queue/running` 独立存在内存，没有全局管理员枚举/drain 端点；旧 SIGTERM 也不等待该官方队列。已知原 ID 查询、持久 0、逻辑失败／超时及进程退出都不能替代真实全集和物理 IO 收尾证明。

选定私有 `observer.mjs` SHA `b3dfb0364f2fd40fbe7a5cc893c005c5f9769acc69522e4d0d2f53d08795585f`，`PREPARED.json` SHA `3e2bbd4ab4df99051a547c934504cf50377fb20ff9de584cb5f3c4094f8ce502`；固定旧源码、Node、原生三 ENV、Main PID／start ticks／Invocation 与有效窗口均须匹配。105 个无生产副作用的反例及 Linux 六项真实 transport 检查已通过；这不表示生产实例已经观测。维护者须审阅 capsule 的生产前提和逐条调用；窗口中显式批准 inspector endpoint／必要信号后才运行其 `observer.mjs --selection FILE --selection-bytes N --selection-sha256 SHA --output NEW_PRIVATE_DIR`。不得把 synthetic loopback qualifier 用于生产。

observer 只在授权窗口按实际旧 class／script／断点核实例元数据，原管理员 ping 为一次 loopback GET；arm 最长 15 s，暂停最多 2 s，并在 finally 清断点／关闭 inspector／核真实连接分离。保存输出和清理回执，再按实际成员／官方任务、control、prepared 与 IO 清单自然排空；仍有未知或在途时停在本节。当前实际 cloudflared 为共享路由，操作前另取完整实际配置，仅改正式业务 hostname，保其他 origin；先前接受版本的日志不能冒充当前管理配置或写入权限。

默认让已受理共享队列自然完成再停派发；若有 queued/pending、预扣、pending review、prepared 责任需要跨窗口保留，先逐项验证其原输入/代次/期限与 B 支持的映射，列入最终 cut 清单。不能为便于切换清零、补默认或自动结算；无法解释的责任阻止转换放行。原图发布、迟到结果、官方观察、备份及归档也必须完成当前 IO 收尾，不能仅看上游 HTTP 已返回。

## 3. 停旧 writer 与最终一致 cut

**授权节点 B：停止正式服务并固定最终来源。** 旧版没有可用的全局独立停止派发 API；自然排空后用正常停止关闭 dispatcher。只对 manifest 确认的两个正式 unit 操作：

```sh
sudo systemctl stop novelai-gateway-web.service
sudo systemctl stop novelai-gateway.service
sudo systemctl show novelai-gateway.service novelai-gateway-web.service \
  -p MainPID -p ActiveState -p SubState -p Result -p ExecMainStatus
sudo ss -ltnp
```

记录原 PID 真退出、端口释放、正常刷盘及所有写入者结束。旧程序自带宽限到期退出，unit 另有停止超时；退出码 0 不等于全部 IO 完成。不得靠强杀、放大超时掩盖未决或删除 WAL/SHM/journal 来凑封存要求；强制退出须保现场并进入故障处置。

格式门禁拒绝旧程序的业务和 schema 写入，但不构成文件写保护：旧版存储初始化可能先切换 SQLite 日志模式，再因版本不兼容拒绝启动，不能把这种报错退出记为“零文件写入”通过。反向验证只在独立可写副本执行，原封存源不得交旧程序打开；版本拒绝不能替代停旧 writer 和新数据目录独占。

使用已核定的现有备份入口制作固定保留的停机 cutover 副本，再校验它：

```sh
sudo python3 /opt/novelai-shared-gateway/backup.py --label cutover
sudo python3 /opt/novelai-shared-gateway/backup.py --verify "$CUTOVER_SNAPSHOT"
```

运行前必须核该部署脚本的实际 SHA/行为与 manifest。cutover 不做 daily 轮换；不要换成可能执行远端留存删除的 daily 命令。本地当前备份默认不包含图片，不能因此声称原图已备份。单独保全 Local 图/part、外部对象声明与访问材料、旧程序/Node/unit/config、旧归档及独立 hold。数据库副本须是普通、封存的 DELETE 单文件且无 sidecar；通过 SQLite 正规 backup 得到一致副本，不能复制运行库主文件或删除 sidecar。原始副本不用于升级/修复，任何整理仅在另一个工作副本进行。

以停机后真实结果取得 Source SHA、原完整 cut、权益/预扣/债务/免费窗口/核销基线及身份/会话期限。窗口中任何仍发生的业务、认证或外存变化都使先前最终性失效，重新固定并核对；不能继续沿用旧 receipt。

## 4. 独立旧导出和切点配置

在封存副本上运行固定旧工具，再用候选的只读 inventory。输出须为新私有文件/目录；以下重定向前先启用 shell 的 noclobber，避免覆盖已选证据。

```sh
umask 077
set -C
"$NODE" "$LEGACY/scripts/audit-legacy-archive.mjs" \
  --source="$SOURCE" --source-sha256="$SELECTED_SOURCE_SHA256" \
  --export="$WORK/legacy-export" > "$WORK/legacy-audit-stdout.json"
"$NODE" "$MIGRATION/scripts/database-model-inventory.mjs" \
  --source="$SOURCE" --source-commit="$SELECTED_OLD_COMMIT" \
  > "$WORK/inventory.private.json"
```

核 `legacy-export/audit.json` 与实际 inputs/artifacts JSONL 的 basename/长度/行数/SHA 四元组、Source/原发布/完整 cut/opening、完整性及 FK；两个工具对同源不一致即停止。保原输入、原图、原回执、排序、幂等与全部审计来源，不把 metadata 声明写成实读证明。

**cut-config 没有独立采集 CLI。** 现有接收 owner 是 `migrate-legacy-database.mjs::readLegacyCutConfiguration()`，不能编造一个 capture 命令。执行前，维护者须给出已审查的实际采集步骤与私有 receipt：停止窗口、原配置/namespace/身份来源及 SHA，明确早于或晚于 DB cut；非原子捕获如实标记，不能事后推定同切点。

新 `novelai-legacy-cut-config-v1` 恰含 `format/sourceSha256/cut/sourceCommit/captureStartedAt/configuredOwnerUserId/configuredGuildId/discordLoginEnabled/sourceNamespaces/sourceCapture/namespaceReceipt/identityReceipt/localOriginal`。其中三个 receipt 各为实际文件和独立 SHA；`sourceNamespaces` 只保存原 `dataDir/r2AccountId/r2Bucket/pikpakUrl`，不含凭据，不能由本次目标环境补原来源。有效 owner/guild 和登录配置来自选定旧部署，不从保存的 session-owner ack 猜当前配置。`localOriginal` 保留有原行 pointer 的已选单图，或确实为 null；不能填假对象通过校验。

新 cut 必须重新绑定配置与 receipt；已测 `47ea…`/cut16509、audit `f16a…`、出口 `b5d7…`、配置 `fa727…` 只是[代表性演练](../history/database-model-b-migration-acceptance-20261116.md)的独立材料，不是最终生产来源。代表样本的 13 个不可证明旧键永久 410；最终来源仍逐键判断，不按这个数量批量造 live，NULL parent/原 POST command 保持未知。

需要新的 Local 恢复输入时，按[既有选定原图准备](legacy-original-materialization.md)走 `materialize-legacy-originals.mjs` 的五项参数，输出新 FILE+SHA。它可触发真实 R2/Pik GET，须在生产读取授权与预算内单独记录；不是纯离线转换的一部分。复用原 sample 仍须实际字节校验；不凭同 SHA 合并 artifact，不重写 cut-config 单图，也不把新领取冒充原远端承诺。B 运行本身禁外部网络。

## 5. 唯一 B 转换与完整核对

**授权节点 C：对最终封存生产副本执行转换和恢复安装。** 十个必填参数，使用 rescued 输入时只多这一对；没有 rescued 输入时整对省略，不能传空串。

```sh
"$NODE" "$MIGRATION/scripts/migrate-legacy-database.mjs" \
  --source="$SOURCE" --source-sha256="$SELECTED_SOURCE_SHA256" \
  --source-commit="$SELECTED_OLD_COMMIT" --cut="$SELECTED_OLD_CUT" \
  --audit="$WORK/legacy-export/audit.json" --audit-sha256="$SELECTED_AUDIT_SHA256" \
  --artifacts="$WORK/legacy-export/artifacts.jsonl" --output="$B_OUTPUT" \
  --cut-config="$SELECTED_CUT_CONFIG" --cut-config-sha256="$SELECTED_CUT_CONFIG_SHA256" \
  --rescued-originals="$SELECTED_RESCUED_ORIGINALS" \
  --rescued-originals-sha256="$SELECTED_RESCUED_ORIGINALS_SHA256"
```

输出目录须全新；退出 2、`status:building` 是该 CLI 的明确约定，既不是 ready 成功，也不能仅据退出码判定来源核对失败。保存 `report.private.json`、`migration-inputs.private.json`、新库及物化原图/receipt；逐项审 findings 和 `sourceReconciliation.status`，不能仅因最后的旧标签或 finding 数量推断完成，也不能删标签放行。

来源核对须 `complete`：逐 account/member/resource 的余额、预扣、未决责任、基线与观察、免费窗口、资金 receipt、全实体集合/归属、原顺序与 NULL/0、保护正文/附件、键意图与回执、诊断及原图来源均与原源闭合。当前 owner 的授权/代次语义及完整回放不能由目标 shadow 自证替代。任何 blocked/未支持计划或未解释差异保持 building，修唯一 converter 后用新输出重跑；不手工修目标、补父请求、补原 POST 或重演历史收费。

## 6. 同一结果联合恢复与唯一 ready

对 B 结果关闭/封存后固定新格式源 SHA、history 和完整新 cut。旧 v4 Source 不能直接作为新格式私有快照。公共回放不得恢复认证；最终选定私有源只在同 history/cut 经原 owner 恢复，保当前 epoch、撤销和原会话期限，不使用历史会话桥接/续期工具。

先在隔离目录对这个**同一结果**完成公共+私有全表比较、完整重复零写、重启续跑及实际到达 SQL owner 后的尾错回滚。只读格式工具或一个坏 SHA 提前拒绝不能代替它。新 B 来源不同于代表样本时，不能沿用代表样本的 10173 笔、34 表、私有 21 行或成功摘要。

`READY_DATA` 为最终拟部署的独占数据目录，先复制所选实际 Local 原图/part，保原名字，不硬链接到会被旧清理器修改的目录；不使用带删除语义的同步。原副本全部保留。`READY_DB` 必须是该目录中尚不存在的 `library.sqlite`，不要覆盖 B 的 building 源。唯一六参数恢复入口如下：

```sh
"$NODE" "$BACKEND/scripts/restore-relational.mjs" \
  --source "$SELECTED_B_SNAPSHOT" --sha256 "$SELECTED_B_SHA256" \
  --history "$SELECTED_B_HISTORY" --cut "$SELECTED_B_CUT" \
  --output "$READY_DB" --data-dir "$READY_DATA"
```

本步原 R2/Pik reader 可能真实 GET；它不上传/补副本/删对象、不写 verified。需带入对应原 namespace 的受保护读取配置，网络范围和总字节另记；不能用 mock 结果放行。installer 必须逐非退休产物取得至少一份可用承诺副本的实际字节，并核 prepared final/part；Local/R2 的持久 verified 与 Pik uploaded 候选只是资格，真实 GET/解压/MIME/SHA/长度及前后票据核对不可省。旧 locator 不受当前 reader 支持、坏/缺内容、只有无字节的声明或未闭合 prepared 都应失败；补足合法输入后重跑，不改数据库为 ready。在线缺图 503 约定不等于 installer 可跳过必需字节。

只有退出 0 和 `restored:true`、全部表/所选私有切点及实际字节核验通过才接受输出。记录 `artifactsVerified/preparedResultsVerified` 与原全集口径；`stagingCleanup:incomplete` 是已发布后的清理提示，保留该私有路径供核查，不能覆盖输出重试。发布前错误按 phase 保现场，新输出路径重跑。部署前再核实际文件 SHA/封存状态、权限及所有原图路径。

## 7. 旧归档保护、备份与容量

本节沿现有保留源 CLI，须在最终发行物实际纳入并验收后执行。使用 `resolve-legacy-retained-dependencies.mjs` 九项 kebab 参数，原 source/audit/artifact 元组、namespace authority、完整 inventory 三个独立 pin 不得替换为当前配置：

```sh
"$NODE" "$MIGRATION/scripts/resolve-legacy-retained-dependencies.mjs" \
  --source="$SOURCE" --audit="$WORK/legacy-export/audit.json" \
  --audit-sha256="$SELECTED_AUDIT_SHA256" \
  --artifacts="$WORK/legacy-export/artifacts.jsonl" \
  --namespace-authority="$SELECTED_CUT_CONFIG" \
  --namespace-authority-sha256="$SELECTED_CUT_CONFIG_SHA256" \
  --inventory="$WORK/inventory.private.json" --inventory-sha256="$SELECTED_INVENTORY_SHA256" \
  --output="$WORK/resolved-dependencies.jsonl"
```

显式选定整份 resolved SHA 后，以原路径/原 Source ID 登记或对已有 unknown 追加解释。`ARCHIVE_REQUEST` 是私有 JSON 文件，字段为原 `source/manifest/dependencies/resolvedDependencies`，已有来源另含 `sourceId`；不能在 body 自报 resolved pin。新建 registry 只在已确认不存在且获准时使用现 `init`，不得覆盖/重建旧 registry 消 pin。

```sh
"$NODE" "$BACKEND/scripts/retained-sources.mjs" archive "$REGISTRY" \
  "$SELECTED_AUDIT_SHA256" "$SELECTED_RESOLVED_SHA256" < "$ARCHIVE_REQUEST"
```

原库、原 inputs/artifacts 导出、固定旧工具、有效权益/未决/键依据、原图与全部归档来源脱离 daily 轮换保留。prepared-domain 只保护其原 Local 根，未知依赖继续 pin；在线 0 ref 不是孤儿。旧 R2-only snapshot 不能借当前 Pik 宣称可删。范围还须覆盖独立远端 hold、旧无 manifest/opaque/preparing 来源，不以新空 registry 或自报 inventory 获得删除资格。

若要启用 #49 自动源释放，另须已有受保护关系 snapshot 的原 manifest 绑定 `config/retained-inventory-scope.json`，其 retained checkpoint 被独立选定；scope 只定义 local/archive/R2 域，owner 在 lease 下实际枚举成员。`INVENTORY_REQUEST` 只给 `checkpoint/scope/scopeSha256`，checkpoint pin 为独立位置参数：

```sh
"$NODE" "$BACKEND/scripts/retained-sources.mjs" adopt-inventory "$REGISTRY" \
  "$SELECTED_CHECKPOINT_SHA256" < "$INVENTORY_REQUEST"
```

采用后的目录身份/完整 roster、部署资格与原证明必须继续有效；restore 后资格 unknown，不能复制 live-root qualification 冒充新域授权。**生产对象删除不在本单默认范围内。** 未获得完整来源闭包、Linux 目录持久性证据及明确删除授权前，不配置 `RETAINED_SOURCES_DIR` 开自动释放；不调用 retire/confirm、`--clean-archived-images` 或云 DELETE。备份捕获仍须保存 retained catalog，不能因关闭图像释放而丢来源保护。

同一最终候选须另完成实际加密业务备份、receipt/GCM 回读、归档恢复及容量验收；业务备份不含图，不能将两个 DB 文件差当总节省。现 `r2-backup.mjs upload` 附带 daily 留存处理，不能当作无副作用的上传测试。需真实云写删时单独提交该实际对象/调用计划并获授权；不能在本单中顺带执行。新格式恢复、旧归档工具及原对象访问材料均须可从已选备份重获。

## 8. 实际包启动与返还入口

**授权节点 D：安装数据/配置和启动唯一新 writer。** 在维护状态核旧服务均停、最终 cut 未变、文件系统余量足够。由 manifest 给出已经审查的实际部署路径与逐条数据安装/权限步骤：新数据放到 unit/备份共同使用的绝对 `DATA_DIR`，旧 data/program/config/原图另存且不覆盖。原 `backup-production.py` 默认读取 `/var/lib/novelai-shared-gateway`；改 DATA_DIR 却不同时核备份路径不可放行。实际 unit、`current` 和 `ReadWritePaths` 须匹配这一安装，不能把 staged 路径当生产路径。路径搬移也须保持原图与归档物理定位可恢复；本单不生成通用覆盖/删除命令。

从同一已验 tar 安装后先执行 `systemd-analyze verify` 检查实际 unit，再 daemon-reload，只启动一个 executor，随后 web：

```sh
sudo systemd-analyze verify "$STAGED_EXECUTOR_UNIT" "$STAGED_WEB_UNIT"
sudo systemctl daemon-reload
sudo systemctl start novelai-gateway.service
sudo systemctl start novelai-gateway-web.service
sudo systemctl show novelai-gateway.service novelai-gateway-web.service \
  -p MainPID -p ActiveState -p SubState -p Result -p ExecMainStatus
```

这里不能只做“试监听”：`gateway-runtime.ts` 启动前在私有副本核原 DB+sidecar/hash、唯一 DDL、ready 和 current configuration，再打开真实库；`queue.prepare()` 可恢复/补事实，真实 listen 后即 activate 队列、额度刷新和副本工作。维护路由不停止这些行为。因此首次启动本身就处于新事实/外部 IO 边界，不能先偷偷启动再决定是否授权，也没有 `--skip-ready`、旧格式 fallback 或手工设置 ready 的步骤。executor 的真实入口是候选包 `executor/.output/server/start.mjs`，要求绝对 DATA_DIR；不要另启 `server/index.js` 与 systemd 并行占用同库。

在入口仍维护时，用真实 executor/web/Host/内部鉴权与同一数据目录完成选定的包级验收：Cookie/Key 当前 epoch、管理和成员薄读、历史/parent/原回执、防重 410 不重执行、缺图 503 保引用、实际 Local/R2/Pik 领取、SSE、重启/故障恢复，以及 Native/OpenAI/Direct 与调度的同提交结算。非法/撤销身份不能因恢复重获授权；原有效会话不延长。原 normalize/报价/submit/领取参数与期望状态来自已签收的私有烟测计划，不临时拼新参数或重复生图。

真实生产生成会产生收费和外部消费，只运行授权计划中的少量 canary，记录原请求键/回执/内容摘要及实际单次执行；重领和重复提交不得重发。隔离 mock 网络用例、Node22/24 编译用例和代表性 B 恢复各保原证据，不能写成这次 Linux 发行物/生产端点验收。通过实际验收、备份和容量门禁后才恢复原正式路由，通知维护结束；保存完整原路由及返还后观察记录。

## 9. 失败处置与签收

| 停点                                       | 操作                                                                                                                                                                             |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 关闭入口前缺候选/CI/权限/空间/易失队列证据 | 不停生产；补齐对应证据，不扩执行范围                                                                                                                                             |
| 停入口后尚未固定 cut                       | 保持维护、沿原 owner 处理未决；禁止强杀凑 0。若决定返还旧服务，先核原程序/库/配置及外部状态仍一致，保存这次中断记录                                                              |
| 导出/B/恢复/installer 失败                 | 保原封存库、完整 sidecar/receipt/输出现场和 building；不覆盖、不删 finding、不手修 ready。修工具后选新输出，重新关联输入和核对                                                   |
| 启动或烟测后失败                           | 立即阻断新受理并正常停止新 writer，保留新库/cut、所选私有状态、所有新回执/账务及外部对象/IO 记录；用兼容候选向前修复、恢复和重核                                                 |
| 评估恢复旧服务                             | 只有证明没有必须保留的新业务/认证事实、消费或对象变化，才可在明确授权下整体评估旧 cutover 来源。不能让旧程序打开新库，不能用旧快照覆盖新增事实；已有新事实或外存变化时不走该回退 |

最终签收逐项保存：最终 manifest/批准、真实停止与易失 drain、最终 cut 和私有 namespace 来源、原导出四元组、B 来源核对、同结果联合恢复与 ready receipt、实际包 HTTP/调度/重启、备份/归档/外存及资源测量、返还路由和遗留限制。[完整材料与实际 normal 记录](../history/database-model-final-acceptance-20261118.md)已证明全量 B、同结果联合恢复、9642 原图唯一安装、合法 ready、真实 HTTP／dirty-WAL 重启和独立归档；2026-11-20 又以获授权 owner 的 V5 Medium 单图补齐真实派发、同键重复与同目录重启重领，三次成功领取只有一次执行及一次结算。前两次 4.5 上游错误保留为终态，当前运行新事实用新的完整快照封存，没有覆盖回原 ready。各验收实例的正常停止只证明自有副本，不代替本单对生产停止受理／派发、易失 drain、最终 cut 和最终布局的签收。旧[代表性 B 记录](../history/database-model-b-migration-acceptance-20261116.md)只证明当时固定材料与 building 结果；这些记录都不代替生产最终新 cut 或实际切换授权。
