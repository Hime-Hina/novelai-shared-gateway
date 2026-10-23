# C06：Drizzle 存储评估

日期：2026-10-17。结论：**采用 Drizzle 承担 schema、普通查询及经过审阅的增量迁移，保留 better-sqlite3、现有单写入方和必要的原生 SQL。先接入可验证的小范围，不重写整个账本，也不新增通用 Repository 或 Unit of Work。**

本轮已按评估结果正式接入主工程：9 张既有表的 schema、普通 prepared 查询/upsert 和版本迁移均由 Drizzle 支持；领域投影与同步提交边界保留。正式回归位于 `tests/integration/storage-orm.test.mjs`，另继续运行 F02 的存储失败回归。以下微基准与最初 3 表实验属于评估证据，最后一节记录实际接入范围。未迁移生产数据库、未部署或替换生产服务。

## 版本与验证范围

| 项目 | 选择与证据 |
| --- | --- |
| Drizzle ORM | 精确锁定 `0.45.4`；核对 npm 官方 registry 的稳定 `latest`，不是 RC/beta |
| Drizzle Kit | 精确锁定 `0.31.11`，仅开发/迁移生成依赖 |
| SQLite 驱动 | 复用项目的 `better-sqlite3@12.10.0`，没有改成其他驱动 |
| 包管理器 | 显式运行 `corepack pnpm@10.32.1`，独立 package 与锁文件；安装禁用生命周期脚本 |
| 实际运行 | Windows、Node `22.22.2`、驱动内 SQLite `3.53.1`；WAL、写入实验使用 FULL |
| 测试数据 | 真实 `JsonStore.init/createSchema` 建表，真实 `configureFairQuota/syncFairQuota` 初始化；20,000 个合成任务、4 个合成成员、1 个合成账户 |
| 覆盖表 | `jobs`、`accounts`、`app_settings`，共 57 个原有列、10 个显式索引；任务表的 3 个部分索引均覆盖 |

ORM 和 Kit 的所选发布包没有声明 `engines.node`，这不等于支持所有 Node 版本。本次实测满足现有项目 Node ≥22 下的本机环境；目标 Linux/Node 24 的安装、原生驱动和构建仍须在 D01 验证。运行时继续复用唯一的 better-sqlite3 连接，读 worker 继续使用自己的只读连接。[官方 SQLite 驱动说明](https://orm.drizzle.team/docs/sqlite/get-started-sqlite)、[ORM 0.45.4 发布记录](https://github.com/drizzle-team/drizzle-orm/releases/tag/0.45.4)

官网当前部分页面展示 1.0 RC 示例，实际采用的 API 同时核对了已安装 `0.45.4` 的源码与运行结果，不照抄 `@rc` 安装命令。[官方迁移生成说明](https://orm.drizzle.team/docs/drizzle-kit-generate)

## 已通过的实测

| 验证项 | 方法与结果 |
| --- | --- |
| 真实表读取/写入 | Drizzle 在现有 `jobs` 上读取并更新；保持 `data TEXT` 的 JSON 字符串与 `status/account_id/updated_at` 等影子列同步 |
| 任务与预扣原子性 | 使用现有 `reserveFairQuota` 计算真实预扣，同一同步事务写 `app_settings`、`jobs` 和 `accounts`；成功后任务、余额和账户在途状态同时存在 |
| SQL 失败 | `BEFORE UPDATE ON jobs` 触发器中途 `RAISE(ABORT)`；已执行的 settings 写入也回滚，三个持久记录均与事务前一致 |
| 旧事务与 ORM 混用 | 在现有 better-sqlite3 的 `transaction()` 内运行同连接 ORM 语句，随后抛错；所有写入一起回滚，不需要自写事务框架 |
| 内存行为 | SQL 回滚后 JavaScript 对象仍是修改后的值，已明确断言；F02 的失败重载必须保留 |
| 重复预扣 | 同一任务第二次调用真实 `reserveFairQuota` 不再次扣余额；这是已有业务幂等规则的结果，不是 ORM 自动提供 |
| 部分索引 | 手动校准后的 schema 与原库逐项比较 `table_info/index_list/index_xinfo` 及 WHERE 条件；列约束、默认值、排序方向和 3 个部分索引一致 |
| 只读 worker | 使用现有 `sqlite-read-worker.js` 读取这个合成库，统计成功；单独的 readonly/query_only 连接通过 ORM 读成功、写操作被 SQLite 拒绝 |
| 升级与重复执行 | 基线核对后登记已采用基线，生成一条仅增加原型 `evaluation_note TEXT` 的迁移；连续执行两次，列只添加一次、原有 20,000 行和全部 jobs 索引不变 |
| 升级失败 | 后续原型迁移先 `ALTER TABLE`，再故意访问不存在表；整次升级回滚，新增列和迁移完成记录均不存在 |
| 事务中进程退出 | 子进程在 settings 写入后、提交前直接退出；重新打开数据库，未看到半笔提交 |
| 提交后进程退出 | 子进程提交 settings 与 jobs 两处更新后退出；重新打开，两处结果均在，`integrity_check` 为 `ok` |

进程退出试验验证了 SQLite 事务恢复边界，没有模拟磁盘故障、整机断电、不同文件系统或上游已扣费后的图片丢失。ORM 也不负责图片文件与 SQL 的原子性，F07/F10 的产物恢复仍须单独实现。

## 必须避免的三个接入错误

### 1. 不能把反向生成当作准确基线

对真实现有表运行稳定版 `drizzle-kit pull` 后，观察到：

- `idx_jobs_active_created`、`idx_jobs_failed_order`、`idx_jobs_error_log_updated` 的 WHERE 条件全部缺失。
- `idx_accounts_order`、`idx_jobs_order`、两个降序部分索引的 DESC 方向缺失。
- `jobs.cost` 的 `DEFAULT 0` 在生成的 `schema.ts` 中缺失，尽管 pull 的 SQL 文件仍带默认值。
- 直接使用生成的 `.primaryKey()` 再 generate，会把既有 SQLite `TEXT PRIMARY KEY` 变为 `TEXT PRIMARY KEY NOT NULL`。现有 schema 的 `PRAGMA table_info` 显示这些列 `notnull=0`；不能在 ORM 接入时悄悄收紧约束。

原型用静态 WHERE、SQL 降序表达式、显式默认值和表级 `primaryKey({columns:[...]})` 校准后，再建立空库并与旧库实际元数据逐项比较，57 列及 10 个索引才全部一致。**不在生产运行 schema push，不将未经比对的 pull 结果直接作为迁移输入。** 官方也将 pull 定位为反向生成工具；已有上游报告部分索引条件丢失，本次结论以本项目复现实验为准。[官方 pull 文档](https://orm.drizzle.team/docs/drizzle-kit-pull)、[部分索引反向生成问题 #6223](https://github.com/drizzle-team/drizzle-orm/issues/6223)

旧库首次接入采用一次性、可审阅的基线登记：先验证结构，再登记该基线已应用；新库执行对应初始 SQL。原型只验证了 3 个表，正式基线必须包含其管理的完整表集合及旧版本升级路径，不能直接照搬原型的 3 表登记。

所测稳定版迁移器按已执行时间跳过旧迁移，虽然保存 hash，**不会拒绝后来改写的已执行迁移文件**。原型改写已执行 SQL 后再次 migrate，没有报错。已应用迁移应视为不可改写历史；修正用下一条迁移，审查/CI 需检测历史文件变化，不另外开发一套迁移引擎。

### 2. ORM 参数化查询可能丢掉部分索引

对同一份 20,000 行任务库，以下两种条件返回相同任务，但查询计划不同：

```js
// 参数化后成为 IN (?, ?)，SQLite 没有选择现有部分索引。
inArray(jobs.status, ['queued', 'running'])

// 固定内部枚举可以保留字面量；用户输入仍然参数化。
sql`${jobs.status} IN ('queued','running')`
```

`EXPLAIN QUERY PLAN` 的实际结果分别为：

```text
SCAN jobs USING INDEX idx_jobs_created_stats
SCAN jobs USING INDEX idx_jobs_active_created
```

原生 SQL 的 `INDEXED BY` 路径可以继续保留；普通 ORM 查询至少要保住同样的查询计划。不能为追求“全部 ORM 化”把活跃任务查询变成扫描整个历史。静态枚举不应拼接外部输入。

### 3. 共用连接不等于位于同一事务

当前真实流程是：

```text
store.update 排队 → await mutator 修改内存 → 计算脏行
→ persistIncremental/persistDirtyRows 开启同步 SQLite 事务 → 写所有相关行 → 提交
→ 更新持久化缓存；失败由 F02 重载已提交状态
```

在 async mutator 中直接执行 ORM 写入，会发生在现有 SQL 事务开始之前。即使 ORM 使用同一连接，之后的账本提交失败也不能撤销那笔早写，形成半提交。**这条路径禁止接入。**

最小安全路径是：在现有提交阶段，用 ORM 的已准备语句替换需要替换的普通 SQL；settings、任务、账户等所有受影响写入仍由同一个同步事务包住。异步鉴权、网络及等待必须在事务之外；事务内部不能 `await`。原型已验证 better-sqlite3 事务可以直接包含同连接 Drizzle 语句，因此不需要为了过渡增加通用事务队列、Repository 或 Unit of Work。[官方事务 API](https://orm.drizzle.team/docs/transactions)

首次接入只读查询和 schema 不会引入早写问题。后续 F03/F06 若增加规范化表，应将**一个具体业务命令涉及的新旧表写入一起迁入唯一同步事务**，确认提交后才发布事件/暴露内存新状态，失败继续重载；不能先写新表再让旧 `store.update` 另外提交。先明确这条提交边界，再写该业务命令的实现与失败测试。

## 查询与写入开销

这是本机热缓存微基准，不是生产吞吐承诺。读取先预热 200 次，5 轮取中间值；写入采用 WAL+FULL，200 个两表事务/轮、6 轮交替顺序，以下中位数按排序第 4 个值记录。比较项使用相同数据和返回字段，ORM 使用 prepared query，避免每次重新构建查询。[官方 prepared query 说明](https://orm.drizzle.team/docs/perf-queries)

| 操作 | 原生 better-sqlite3 | Drizzle prepared | 解释 |
| --- | ---: | ---: | --- |
| 按任务主键读取 data | 2.176 μs | 2.218 μs | 约 +1.9%；绝对差约 0.04 μs |
| 活跃任务查询，保持部分索引 | 6.396 μs | 8.635 μs | 约 +35.0%；绝对差约 2.24 μs，包含 ORM 结果映射 |
| 活跃任务查询，参数化状态且未命中部分索引 | 同上 6.396 μs | 919.680 μs | 约 144 倍；这是查询计划退化，应修复或保留原生 SQL |
| settings + jobs 两表原子更新 | 532.879 μs | 584.905 μs | 约 +9.8%；提交落盘成本占主要部分，仍须在目标环境复核 |

这组数据支持保留 prepared query 和部分索引的有界接入，不支持全量替换后性能天然更好。原型没有测 20 万/百万历史、并发读 worker、真实磁盘拥塞或 F05 账本热状态规模；这些属于相应实施项和 D01 的验收。

## 能减少什么，仍需保留什么

现有 `store.js` 的四块实测非空、非纯注释行数：字段名称列表 29 行、建表/索引代码 89 行、prepared statement 设置 39 行、领域记录到影子列投影 75 行；存储与读 worker 共 45 处 `.prepare(`。这不是 232 行都可删除，也不是 45 处都重复。

| 当前维护点 | 接入后的实际收益与边界 |
| --- | --- |
| 字段名同时出现在列数组、DDL、SQL 字符串 | schema 作为声明来源，列名/类型由库构建查询，迁移 SQL 由工具生成后审查；不再新增第三套字段列表 |
| 通用 INSERT/UPDATE 字符串手工拼装 | 库负责列转义、占位符和常规 upsert；仍需按主键及现有业务明确写入哪些字段 |
| `ensureColumn` 散落启动升级 | 后续升级走有版本的迁移；既有 legacy JSON/app_records 导入必须先覆盖再退出，不能直接删除兼容路径 |
| JSON 和影子列的同步 | ORM 不知道 `request.model`、`fairCharge`、`stats_excluded` 等领域规则；保留一个明确投影点和一致性测试，不能只改 `data` 漏改索引列 |
| 整个 data 文本与内存缓存 | 不在本次改成 JSON mode 自动序列化。当前 persister 已传 JSON 字符串，再由 ORM 当对象编码会双重序列化；F02/F05 的缓存与热状态问题仍独立处理 |
| owner/管理统计 SQL | 保留参数化原生 SQL及其索引提示；用库不等于把清晰的聚合 SQL 改写成更多 builder 调用 |

原型校准后的 3 表 schema 为 81 行，其中有重复的既有记录列。正式接入改为复用一次共同字段声明：`store.js` 删除 231 行、增加 63 行，净减少 168 行；新增 schema 58 行及历史库基线/迁移适配 90 行，合计手写存储实现净减少 20 行。另有 5 行生成配置、10 项正式测试、冻结的旧 schema 测试样本以及工具生成的迁移 SQL/snapshot/锁文件；这些不能算作零成本。收益是移除重复字段清单与启动 DDL，而不是靠行数宣称架构已经简化到最优。

Kysely 和 Prisma 暂不新增原型：Drizzle 已通过需要的运行时能力验证，剩余问题有明确的有限处理边界；同时安装三个库不会提高此时决策质量。若后续无法保住准确基线或同一提交边界，再根据具体失败重新比较，不能继续堆自写存储框架。

## 正式接入范围与验证

| 文件/范围 | 已实施行为 |
| --- | --- |
| `package.json`、`pnpm-lock.yaml` | 精确 ORM 0.45.4、Kit 0.31.11；原有 38 个锁定版本全部保留，better-sqlite3 仍为 12.10.0；没有移动/替换运行中服务的 node_modules |
| `server/storage-schema.ts` | 用一份共同记录字段定义声明 9 张既有表、167 列、20 个显式索引；保留 3 个部分索引与原主键/默认值语义 |
| `server/store.ts` | 移除 `recordColumns/jobMetricColumns`、手工 DDL 和通用 SQL 字符串拼装；从 schema 派生空影子行、占位符与 upsert 字段。JSON 完整保存，领域投影仍在 `sqlRecord` 一处 |
| `server/storage-migrations.ts` | 首次接入以已审阅基线 SQL 建内存参考库，核对所有现存受管表的列、已存在同名索引及谓词；只补旧 jobs 曾逐步增加的 3 个度量列。补列、补建表/索引与基线登记在同一事务；后续交由 Drizzle migrator |
| `server/migrations/` | `0000` 完整基线及 `0001` 旧冗余索引清理，保留生成 snapshot/journal；原每次启动的清理转为一次有版本迁移 |
| `drizzle.config.mjs` | 只配置本地 schema 与迁移输出位置，没有默认生产连接，也没有运行 schema push |
| `tests/integration/storage-orm.test.mjs` | 新增 10 项正式回归；冻结迁移前 schema 到 `tests/fixtures/sqlite-before-drizzle.sql`，以独立旧结构核对新库，避免测试仅与实现互相照抄 |

旧 typed tables、仅旧 app_records、旧 JSON 与全新库均经过实际 JsonStore 初始化验证。原有受管表结构不匹配时，迁移明确失败，不重建表或猜测修复；事务中途失败不留下已补列。未知的外部表和额外自定义索引不删除。缺少旧 jobs 统计列时沿用原 `ensureColumn` 的 `DEFAULT 0` 语义，不在 C06 暗中从历史 JSON 回填 cost/统计标记；要校正这种极老库的历史统计，应另做明确的数据回填迁移并复用领域投影。对已经应用的迁移还比较保存的 hash，拒绝修改历史文件或用不匹配的版本打开数据库。

所有业务数据的 ORM 写入继续发生在现有 `persistIncremental/persistDirtyRows/replaceAll` 等持久提交事务内，没有在 async mutator 中提前写新表，也没有新增 Repository/UoW。图片/任务清理等原有独立同步事务同时适配 prepared 参数；活跃任务与复杂统计保留原生 SQL、静态谓词和索引提示。只读 worker 不承担迁移或写入。未来增加规范化新表时仍须对一个业务命令的新旧表写入统一事务；本次没有新增账务、队列或会话业务表。

已执行：完整 `pnpm test` **244/244**（包含实际十秒间隔的 Native HTTP 用例，约 135 秒）；正式 ORM 集成 10/10；既有 store/bootstrap 11/11；语法检查 64 个文件。正式用例覆盖元数据、旧库导入、已知缺列升级、错误结构拒绝、部分索引 EXPLAIN、迁移失败/重复/hash 改写拒绝、真实预扣与 JSON/影子列一致、只读 worker、实际 persister 提交中/后进程退出。F02 测试继续验证触发器失败后的内存重载、部分历史缓存与延迟写恢复。

后续 schema 变更从声明生成，审阅 SQL 后提交；已有迁移不得修改：

```sh
corepack pnpm@10.32.1 exec drizzle-kit generate --config=drizzle.config.mjs --name=描述本次变更
corepack pnpm@10.32.1 test
```

生成与校准流程：`storage-schema.ts` 是现行声明来源，先 generate，再审查 SQL 与 snapshot；首次基线只有经过审阅的 `IF NOT EXISTS` 调整，使已验证旧库可以原地登记，不能把它当成一般 schema 自动同步。后续结构变更新增迁移，不编辑 `0000/0001`；运行时会拒绝已有 hash 不匹配。`.gitattributes` 仅对迁移 SQL 固定 LF，保证 Windows checkout 与 Linux 部署的 hash 一致，不归一化其他源文件。测试样本 `sqlite-before-drizzle.sql` 是冻结的迁移前结构，不能为了让新 schema 测试通过而随意一起修改。

部署前通过 SQLite 在线 backup API 或既有一致性备份流程保存数据库，同时保留需要的图片/结果文件；WAL 模式不能只复制主 `.sqlite` 文件就声称备份完整。D01 先在受保护副本运行同一发布产物，核对迁移、业务计数、索引和结果可领取，再申请生产切换。首次自动接入发现不支持的结构会停止启动，保留数据供检查，不能用 schema push 或删表绕过。

没有提供自动 down migration。本次只登记基线、补原版本已有的度量列及清理原版本已移除的冗余索引，没有重建业务表或改变 JSON 编码；仍须用实际副本确认旧发行版读取能力，不能把本次情况推广为所有后续迁移都可回退。后续迁移优先向前修正；若必须恢复备份，先停止写入并核对备份之后的新任务/结果，避免回滚丢数据。生产替换及恢复操作仍按 D02 单独批准。

运行时启动在接单和创建读 worker 之前执行已打包迁移，不运行 Kit，也不下载依赖。现有 Docker COPY 整个 `server/`，会包含迁移 SQL 与 metadata；本次 Docker 构建及镜像内只读根文件系统、断网、UID 1000 的真实 JsonStore 写入/关闭重开、两条迁移记录与 integrity_check 已由主代理验证通过（镜像 `sha256:d0ca0d81e9748cd14a1f6b3859bbe5b08437a79c91d33dfe30b301a6837e507a`）；生产数据副本的完整隔离验收仍归 D01，生产替换仍需另行授权。

原型过程及微基准结果暂存 `_private/orm-evaluation/`；正式回归已经提炼到 tests，原型不进入运行产物。没有更换 SQLite 驱动、压缩存储历史、迁移公平额度算法或新增自动删除策略。
