# 新格式离线联合恢复

`scripts/restore-relational.mjs` 从一份明确选定的当前关系格式 SQLite 快照恢复到**全新输出**。它逐完整提交恢复业务，使用同一来源的保护正文和私有认证，再调用唯一 `verifyAndInstallRelationalStorage()` 核验全部表、实际对象和未决准备文件并安装 `ready`。它不解释旧格式、不选择 A/B 迁移路线，也不决定会话保留政策；选定私有快照的授权与会话取舍必须在运行前完成。

## 格式、完整提交与私有恢复

唯一 DDL 归 `relational-schema.ts`，格式门禁核源码 hash、实际表/索引/trigger、writer/minimumWriter 与外键开启；仅空目标可初始化为 building。字段/封套由 fact contracts 校验，完整提交另核原 event_count、事件顺序与精确 64 位游标，业务关系仍由各原 owner 验证。初始化、字段合法或源/目标两库相等均不能代替 ready 验收。

业务回放只恢复公共决定，按完整原提交顺序在共同保存点写事实、回执、流头、游标与投影。保护 input/context/diagnostic/configuration 必须与来源的原完整 commit 相同，并核实际正文/附件的归属、长度与 SHA；重复仍验证源和目标，缺失或损坏直接拒绝，不补写修复。全业务新提交回放须 private-empty，私有恢复留在最终业务 cut。

私有恢复要求目标为 building，来源只接受明确选定 SHA 的普通封存 SQLite 单文件、DELETE header、无 sidecar，拒绝源/目标相同真实文件及符号链接。核对相同 history、连续完整 cut、stream heads、稳定成员和账户代次/连接来源后，复制成员 `auth_epoch/note_json` 及密码、Key、Discord、invite、owner绑定、Cookie、上游凭据的原私有表。`note_json` 已知空也属于非空私有状态；不同的非空私有目标拒绝覆盖，相同状态重复零写入。保留原期限，不按当前时间续期；撤销、停用和移除仍受原成员状态/认证 owner 约束，业务事件不得复活旧 Key/Cookie/PST。

唯一 `verifyAndInstallRelationalStorage()` 从空 shadow 经过真实完整 replayer 与所选私有源重建，枚举唯一 DDL 的全部表逐项比较；公共比较只由私有 owner 的列/表清单排除私有值。选定 cut 必须有真实有效 current configuration，两边都缺 current 也拒绝。对象与准备结果的实际字节 IO 在 SQL 外，结束后以同一最终 immediate 事务重验 source/cut、全部业务表、目标私有指纹和选定源 SHA，再恢复私有并装 ready；shadow 关闭/清理失败也回滚安装。失败被外层捕获后，事实、回执、游标、正文、私有值和状态均须保留原样。

## 新空库初始化

`scripts/bootstrap.mjs` 复用真实成员注册、首次 `SharingConfigured`、账户首次注册/绑定及显式 Configuration 初始化，不生成 Opening/ImportSource，不解释旧库或自动查询/启用上游。四组及一次性用户名/密码/Key 按原语义交付；账户暂停、代次 1、零权益和未知官方观察。后续启用必须走实际 prepare/query/complete 的当前授权、代次与有效 Opus 核验。

预检、持久独占锁和先保存一次性私有凭据保护现有文件。初始化只写本次私有 staging，以同最终 cut 的独立封存源经唯一 installer 核完整 shadow、profile、全表与私有状态，再封存并无覆盖发布 `library.sqlite`。失败保留锁/凭据供核查，不换身份重试、覆盖旧输出或手写 ready；Windows 须预先限制父目录 ACL，POSIX mode 不能充当 Windows 权限证明。入口说明见 [scripts](../../scripts/README.md)。

## 输入准备

- 使用获准的一致性数据库副本；快照必须已封存为单个普通 SQLite 文件，使用 DELETE journal header，且没有同名 `-wal`、`-shm`、`-journal`。不要直接指向运行中的数据库或软链接，不要在恢复期间修改快照、产物或配置。
- 从该副本的可信清单取得 SHA-256、`projection_state.history_id` 和完整 `last_commit_seq`。cut 必须是大于零的规范十进制整数；CLI 不推测 cut、不自动采纳当前最大值，也不把空库安装为 ready。
- `--data-dir` 指向这次恢复使用的实际数据目录，保留原 `images/` 文件名与 `.part` 准备文件。若原有效副本位于 R2/PikPak，使用现有 `R2_*`、`PIKPAK_WEBDAV_*` 环境变量；认证格式仍归 `r2Config()`、`pikpakConfig()` 和原 IO owner。
- 输出父目录必须已存在，位于支持原子硬链接的文件系统，并由操作者独占管理。暂存目录申请 `0700`，SQLite 文件申请 `0600`；Windows 的 Node mode 不设置 ACL，必须先限制父目录 ACL。恢复出的文件包含密码验证数据、Key、Cookie 会话与上游认证，应按私有备份保护。

## 运行

发布包的入口是编译后的 `scripts/restore-relational.mjs`，内部静态导入已由现有后端构建改写为 `.js`。在已安装对应 Node/native 依赖的发布包目录运行，六个参数均必填且只能出现一次：

```sh
node scripts/restore-relational.mjs --source /private/selected.sqlite --sha256 <清单中的64位小写SHA256> --history <选定history_id> --cut <选定完整commit序号> --output /private/recovered.sqlite --data-dir /private/recovery-data
```

仓库源码入口相同，使用仓库声明支持的 Node 版本。恢复不会启动 HTTP、调度器或上游生成；外部存储只通过原读取能力取得并校验字节，不上传、补副本、删除对象或写回 verified 标记。所有 queued/未决核对期限保留原值，不按运行时刻延长。

CLI 在输出同父目录创建私有 `.restore-relational-*` 暂存目录，以 128 个完整提交为一页，从空的 `building` 库调用共同 replayer。业务回放本身不恢复私有认证；唯一 installer 在完整 shadow、各表、来源、保护正文、实际对象和当前私有切点核对后恢复选定私有状态并提交 ready。CLI 随后 checkpoint、关闭并封存单文件、同步文件、再次核对源 SHA，以不覆盖的原生硬链接发布输出。

现有输出、输出 sidecar、源路径和源 sidecar 路径均拒绝使用，发布竞争不会替换既有文件。**独占硬链接成功就是恢复的最终成功点**：所有可能拒绝恢复的业务、来源、封存和输出检查均在之前完成，之后绝不依据可变 output 路径撤回或删除输出。清理只处理本次私有暂存目录，先核对绝对路径确实是指定输出父目录的直接子目录。发布后若暂存清理失败，恢复仍成功，以 `stagingCleanup: "incomplete"` 和 `stagingPath` 报告保留下来的私有目录；其中可能保留另一份指向同一恢复库的硬链接。由维护者核对后清理，不应再恢复到现有输出。没有 resume 或复用先前半成品的机制。

成功退出码为 0，stdout 仅有一行 JSON，例如：

```json
{"restored":true,"commitsRestored":"142","artifactsVerified":1,"preparedResultsVerified":0,"stagingCleanup":"complete"}
```

发布前失败退出码为 1，stderr 只给固定错误码 `RELATIONAL_RESTORE_FAILED` 和阶段 `arguments`、`paths`、`source`、`replay`、`verify`、`seal`、`publish` 或 `cleanup`，不输出 owner 异常、源路径、正文、Key 或 PST。不要据此放宽验证；按阶段核对获准清单和来源文件。失败的 `cleanup` 指发布前的错误处理也未完成私有暂存清理；与发布成功后的 `stagingCleanup` 警示分别处理。若进程被强制终止或文件系统清理失败，应在显式输出父目录检查本次暂存与输出，由维护者处理；不得仅依据暂存目录存在认定恢复成功。再次恢复应使用新的、尚不存在的输出路径。

## 验收和切换边界

专属入口为 `tests/integration/restore-relational.test.mjs`。它真实启动 CLI 子进程，以真实 owner 产生跨页提交、保护正文、轮换私有认证、queued 责任、待核对退款、人工历史退休、诊断和真实 Local 产物；恢复后逐表核对、检查 ready 与原字节，拒绝坏 SHA/cut/history/格式、缺正文或对象及输出覆盖。发布竞争保留既有文件，发布后清理故障仍成功并准确留下私有暂存路径，外部替换输出不被删除；原 stat→unlink 竞态有可运行反例。文件系统故障只注入发布边界，不替换业务 replayer、installer 或对象验证。

编译入口可用同一专项验证：先执行现有后端构建，将 `RESTORE_RELATIONAL_TEST_ENTRY` 设为 `.output/backend/scripts/restore-relational.mjs` 再运行该测试。该模式在 CLI 子进程关闭 TypeScript strip，确认实际编译入口及其 `.js` 依赖可运行。

这些合成夹具和 Local 文件是本地工具验收，不代表真实 R2/PikPak 端点、同规模容量性能、完整候选发布包、生产恢复或切换通过。生产恢复、删除和切换仍须明确授权，并完成[数据库重构计划](../plans/database-model-redesign.md)要求的同一候选版本整体验收。切换前保留原快照、清单及原产物；部署入口的只读 ready 门禁不能由手工 SQL 代替。
