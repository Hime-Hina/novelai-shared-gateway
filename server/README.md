# 服务端

T3/T4 的认证接入进度见[数据库计划第 9.6 节](../docs/plans/database-model-redesign.md#96-t3t4稳定身份私有凭据和会话事务)。会话规则继续归 `members/authentication.ts`，Discord 邀请、OAuth 和刷新归 `members/discord-auth.ts`；存储实现 `memberSessionTransaction()` / `discordAuthenticationTransaction()` 的窄事务能力，读取模块使用 type-only `Pick`。旧运行时的 SQL 映射位于 `storage/sqlite-store.ts`，不让新格式认证 owner 依赖旧用户集合或表类型。

HTTP 网关、成员鉴权、共享额度账本与持久化模块。来源和非商用许可范围见仓库根目录 NOTICE.md 与 LICENSE。

当前执行器独占业务写入和上游调度，Nuxt 只通过内部 HTTP 转发。根部 `index.ts` 与独立 Nitro 入口复用 `gateway-runtime.ts`；它们是替代启动方式，不能据此启动两个当前版本的账本写入者。后端只提供 API，不读取或回退到静态门户；网页入口仅由 Nuxt 提供，旧网页与资源地址返回 404。

| 规则/事实                                 | 当前归属                                                                                                                   |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| 受理、费用确认、执行、查询、队列与批次    | `generation/`：`admission.ts`、`member-contract.ts`、`execution.ts`、`request-queries.ts`、`batch-requests.ts` 等          |
| 分配、恢复、预扣、退款、资格与运营账      | `quota/`：`fair-share.ts`、`eligibility.ts`、`account-refresh.ts`、`operating-accounting.ts` 等                            |
| 领域事实、提交、投影、重放及状态通知      | `events/`：`domain-events.ts`、`event-store.ts`、`quota-projections.ts`、`state-notifications.ts` 等；通知不是权威事件日志 |
| 会话、Discord、成员 HTTP 与 owner 操作    | `members/`：`authentication.ts`、`discord-auth.ts`、`http-routes.ts`、`owner-operations.ts`                                |
| SQLite、worker、产物及本地/R2/PikPak 存储 | `storage/`：`sqlite-store.ts`、`sqlite-schema.ts`、`sqlite-migrations.ts`、`sqlite-read-worker.ts` 等                      |
| Native/OpenAI 协议、NAI 请求与计价        | `protocols/`：`native-*.ts`、`novelai-client.ts`、`openai-models.ts` 及计价/Medium 规则                                    |
| Host、来源 IP、API 白名单及公开错误       | `http/`；`prompt-api-settings.ts` 仅保留现有管理摘要的配置规范化与脱敏，不提供提示词转换服务                               |
| 调用热力图、服务资源与 R2 分析            | `reports/`                                                                                                                 |

根 `gateway-runtime.ts` 负责装配和 HTTP/进程生命周期，但仍包含 OpenAI 请求解析、账号管理与统计适配。后续真实需求触及这些规则时，分别归入现有 `protocols/`、`members/`/`quota/`、`reports/` owner；不按文件行数机械拆分。默认数据位置仍为仓库根下的 `data/`。SQL、journal 和快照仍在 `migrations/`，文件字节与版本不因源码移动变化。没有旧平铺模块的转发或重导出。

SQLite 句柄、语句、热缓存和内部持久化过程是 `JsonStore` 的私有实现；查询/协议使用实际读取方法的窄类型，业务变更经现有命令提交。运行时只通过活动任务判断和瞬时进度方法访问热任务，不为每帧进度追加业务事件。`architecture:check` 固定纯金额/reducer、查询、Web 和装配入口的依赖方向；`migration:check` 比较可信 Git 基线的历史 SQL 字节和 journal 前缀。检查范围、命令和维护者复核要求见 [开发与验证](../CONTRIBUTING.md)，这些检查不代替远端权限与强制合并保护。

新关系报告由 `storage/relational-report-store.ts` 提供薄 SQL：用量、热力图、错误和损耗复用各自原展示 owner；损耗的精确风险汇总/事实适配继续在 `quota/failure-accounting.ts`，候选金额语义归 `generation/accounting.ts`。受保护请求/诊断冷读归 input store，不从旧 JsonStore 回退，也不借报告修改账务。新 HTTP 装配与整体上线验收仍按数据库计划接续。

本分支的账务和任务生命周期以已提交的领域事件为权威；钱包、预留、任务状态、冷结算和免费次数由同一事务更新投影。金额核心使用 12 位定点整数运算，兼容旧 DTO 时才转换为 number。`fairQuotaSummary()` 为纯查询，需要恢复估算时使用独立的 `estimateFairQuota()`；能力、报价与提交共用 Native 规范化和计价规则，Cookie 成员闭环见[成员生成接口](../docs/contracts/member-generation-contract.md)。

首次转换从旧库建立 opening facts，不补造过去没有保存的逐次事件。启动先恢复文件结果和中断任务，再核对事件投影并启用事件权威，成功后才监听及派发；任何不一致会阻止切换。首次升级应按同规模副本的开账、恢复与核对实测安排维护等待，systemd active 或早期健康探针失败都不能单独判断就绪。完整输入、首次规范化上下文、认证数据和产物字节仍需与事件一起备份，重放命令不会调用上游。具体边界、等待和命令见[结果恢复](../docs/architecture/result-recovery.md)。

当前 writer/minimumWriter 和 reducer 版本为 4，事件格式仍为 v1。已有 v1/v2/v3 权威库在启动恢复前原子重建、核对并安装投影；旧事件缺少但旧查询记录实际保存的上游时间/状态，以注明来源的 `ResponseEvidenceRecorded` 追加补充，已有事件、输入和原回执不改写。未知历史值保持未知。离线重放旧库前，先用当前程序在隔离备份副本上完成升级，不能直接重放旧 source 丢掉查询证据；旧 checkpoint 在当前 source 下可重建续跑。升级后的库拒绝 v1/v2/v3 writer；只有尚无新增持久写入时才可恢复升级前完整备份，新增补充事实也属于写入。发生新写入后保留账本并使用兼容程序，未知更新版本在迁移或投影安装前拒绝。流程见[结果恢复](../docs/architecture/result-recovery.md)。

这些是当前代码边界，不代表生产已完成升级；实际版本与授权以[发布记录](../docs/operations/production-release.md)为准。多执行器、多账号调度和数据库系统迁移仍在[后续计划](../docs/plans/scaling-follow-up-plan.md)。

继续工作先读该计划的目标、事实表和工作包状态，再按所改规则读取具体模块。[API 契约](../docs/contracts/api-contract.md)维护外部行为，[结果恢复](../docs/architecture/result-recovery.md)维护产物提交边界，[发布记录](../docs/operations/production-release.md)维护运行版本与回退证据。

数据库模型重构按 [PR #33 的目标与工作包](../docs/plans/database-model-redesign.md)另行实施。T1 由 `storage/relational-schema.ts` 的唯一 DDL、`storage/relational-storage.ts` 的初始化/格式门禁、`events/relational-fact-contracts.ts` 的单源字段契约及 `events/relational-event-store.ts` 的原子提交/完整提交回放组成；字段 owner 见[第 9.1 节](../docs/plans/database-model-redesign.md#91-t1-基础物理字段格式和生命周期契约)。T2 的 `generation/relational-input.ts` 复用原规范化/槽位/协议规则，`storage/relational-input-store.ts` 保存唯一正文、input 内 BLOB 和首次上下文，提供受理引用、授权输入/附件读取及执行冷读取；范围和回归见[第 9.3 节](../docs/plans/database-model-redesign.md#93-t2统一输入附件和首次上下文2026-11-12)。这些窄入口供 T3/T4 接入，不双写旧 `JsonStore`，也未替换启动入口。新库保持 building，等待完整业务 owner、迁移和重放核对；现行库不会自动转换。

T3 的 `storage/relational-member-store.ts` 复用原认证/Discord/会话 owner，使用稳定成员 ID 与私有凭据事务。T4 的 `storage/relational-generation-store.ts` 在该认证事务内使用 T2 受理输入，复用 `quota/fair-share.ts` 的窄收费规划、`generation/member-contract.ts` 的确认和 `generation/accounting.ts` 的证据；关系表中的预扣、唯一尝试、原资源扣退、任务终态与下一 child 同笔提交。物理执行槽和 SQL 外的字节发布仍分别由现有执行/文件 owner 持有；有界业务回放不恢复私有认证、不重发上游、不安装 ready。实际范围、未发布契约补全与剩余工作见[第 9.8 节](../docs/plans/database-model-redesign.md#98-t4账务与任务事务闭环)。

T4 的 `storage/relational-quota-store.ts` 复用原观察查询、发起顺序、`syncBoundFairQuota()` 与运营资金 owner；当前金融窄视图只来自关系行，额度列映射由生成终态共用。观察、恢复、资金收据/到账/纠正/核销和钱包投影在命令事务内提交，网络取数在事务外；原决定通过同一投影有界回放，仍不安装 ready。新格式事实补全、schema hash 与当前验收见[第 9.9 节](../docs/plans/database-model-redesign.md#99-t4额度观察恢复与资金事务)。
