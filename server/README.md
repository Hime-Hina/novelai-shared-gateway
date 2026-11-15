# 服务端

会话规则继续归 `members/authentication.ts`，Discord 邀请、OAuth 和刷新归 `members/discord-auth.ts`；存储实现 `memberSessionTransaction()` / `discordAuthenticationTransaction()` 的窄事务能力，读取模块使用 type-only `Pick`。独立旧工具的 SQL 映射位于 `storage/sqlite-store.ts`，新格式认证 owner 不依赖旧用户集合或表类型。

HTTP 网关、成员鉴权、共享额度账本与持久化模块。来源和非商用许可范围见仓库根目录 NOTICE.md 与 LICENSE。

当前执行器独占业务写入和上游调度，Nuxt 只通过内部 HTTP 转发。根部 `index.ts` 与独立 Nitro 入口复用 `gateway-runtime.ts`；它们是替代启动方式，不能据此启动两个当前版本的账本写入者。后端只提供 API，不读取或回退到静态门户；网页入口仅由 Nuxt 提供，旧网页与资源地址返回 404。

| 规则/事实                              | 当前归属                                                                                                                                           |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| 受理、费用确认、执行、查询、队列与批次 | `generation/`：关系绑定 `relational-*.ts`，复用 `admission.ts`、`member-contract.ts`、`execution.ts`、`request-queries.ts`、`batch-requests.ts` 等 |
| 分配、恢复、预扣、退款、资格与运营账   | `quota/`：`fair-share.ts`、`eligibility.ts`、`account-refresh.ts`、`operating-accounting.ts` 等                                                    |
| 领域事实、提交、投影、重放及状态通知   | `events/relational-*.ts`、`storage/relational-replay.ts`，复用 `events/state-notifications.ts`；通知不是权威事件日志                               |
| 会话、Discord、成员 HTTP 与 owner 操作 | `members/`：`authentication.ts`、`discord-auth.ts`、关系薄绑定 `relational-*-http.ts`、`owner-operations.ts`                                       |
| SQLite、产物及本地/R2/PikPak 存储      | `storage/`：`relational-*.ts`、`result-files.ts`、`r2-storage.ts`、`pikpak-storage.ts` 等                                                          |
| Native/OpenAI 协议、NAI 请求与计价     | `protocols/`：`native-*.ts`、`novelai-client.ts`、`openai-models.ts` 及计价/Medium 规则                                                            |
| Host、来源 IP、API 白名单及公开错误    | `http/`；`prompt-api-settings.ts` 仅保留现有管理摘要的配置规范化与脱敏，不提供提示词转换服务                                                       |
| 调用热力图、服务资源与 R2 分析         | `reports/`                                                                                                                                         |

根 `gateway-runtime.ts` 只装配当前关系 owner 和 HTTP/进程生命周期，`index.ts` 与 Nitro 的两个既有 caller 不变。它要求已存在的完整新格式 ready 数据库，不在线初始化或转换旧库；磁盘预检、实际监听后激活与关闭等待见[第 9.75 节](../docs/plans/database-model-redesign.md#975-唯一关系运行入口装配)。OpenAI 解析和 Native/OpenAI/Direct transport 归现有 `protocols/`，关系 HTTP 绑定归 `generation/relational-*-http.ts` 及 `members/relational-*-http.ts`；图片分享签名由 `http/public-image.ts` 的同一进程实例提供。业务规则仍由原 `members/`/`quota/`/`reports/` owner 实现，默认数据位置仍是仓库根下 `data/`。阶段实现及完整同候选验收统一见[数据库模型重构计划](../docs/plans/database-model-redesign.md)。

个人配额和四个启动器订阅 GET 的关系绑定归 `quota/relational-quota-http.ts`，只组合 Quota/Gen 原窄读，sole 用同一 `protocols/quota-http.ts` 五路径判定。当前 Key→Discord IO→原稳定身份/epoch/ready 的授权回调归 `members/relational-api-key-access.ts`，Bearer 父请求查询与取消也复用它。金融估算仍归 Quota owner，PUBLIC/Host/CORS 仍归中央路由；薄 HTTP 不续 Cookie、不恢复官方 PST 模式。

独立旧工具的 SQLite 句柄、语句、热缓存和内部持久化过程仍归 `JsonStore`；当前在线入口只使用各关系 owner 的窄方法，业务变更经同一事务内的完整提交。活动控制和瞬时预览不为每帧追加业务事件。`architecture:check` 固定纯金额/reducer、查询、Web 和装配入口的依赖方向；`migration:check` 比较可信 Git 基线的历史 SQL 字节和 journal 前缀。检查范围、命令和维护者复核要求见 [开发与验证](../CONTRIBUTING.md)，这些检查不代替远端权限与强制合并保护。

原记录 ID 与 JSON 摘要共用纯 `storage/record-values.ts`，幂等 context 引用共用 `generation/request-fingerprint.ts`；受理、执行不为这些函数加载旧 Store。旧 Store 暂时 re-export 同一函数供独立工具，当前 sole、关系 bootstrap/adapters/stores 的运行时闭包不加载旧 Store/migrations；type-only 旧类型不产生加载，普通包移除旧资产和完整切换仍按 D4 后继验收。

新关系报告由 `storage/relational-report-store.ts` 提供薄 SQL：用量、热力图、错误和损耗复用各自原展示 owner；损耗的精确风险汇总/事实适配继续在 `quota/failure-accounting.ts`，候选金额语义归 `generation/accounting.ts`。受保护请求/诊断冷读归 input store，不从旧 JsonStore 回退，也不借报告修改账务。

当前规范化配置由 `storage/relational-configuration-store.ts` 显式系统初始化：11 个原默认值的专用保护 profile 与成员公共地址回退值，单一来源事实和单例投影；未配置明确不可用。profile 共享 `normalization_contexts` 载体，保护内容归 InputStore，既有生成 context 仍必须包含 Native 定义。没有管理 CRUD、任意 settings 对象或 PromptAPI 秘密恢复；读取与原 helper 语义、完整回放及 ready 验收见[第 9.47 节](../docs/plans/database-model-redesign.md#947-当前规范化配置的显式来源和保护恢复)。

本分支的账务和任务生命周期以已提交的领域事件为权威；钱包、预留、任务状态、冷结算和免费次数由同一事务更新投影。金额核心使用 12 位定点整数运算，兼容旧 DTO 时才转换为 number。`fairQuotaSummary()` 为纯查询，需要恢复估算时使用独立的 `estimateFairQuota()`；能力、报价与提交共用 Native 规范化和计价规则，Cookie 成员闭环见[成员生成接口](../docs/contracts/member-generation-contract.md)。

这些是当前代码边界，不代表生产已完成升级；实际版本与授权以[发布记录](../docs/operations/production-release.md)为准。多执行器、多账号调度和数据库系统迁移仍在[后续计划](../docs/plans/scaling-follow-up-plan.md)。

继续工作先读[数据库模型重构计划](../docs/plans/database-model-redesign.md)的目标、事实表和工作包状态，再按所改规则读取具体模块。[API 契约](../docs/contracts/api-contract.md)维护外部行为，[结果恢复](../docs/architecture/result-recovery.md)保留原产物提交规则及独立旧工具的升级/重放说明，[发布记录](../docs/operations/production-release.md)维护运行版本与回退证据。

关系格式的唯一 DDL 与格式门禁归 `storage/relational-schema.ts`、`storage/relational-storage.ts`，字段/封套契约与原子提交归 `events/relational-fact-contracts.ts`、`events/relational-events.ts`、`events/relational-event-store.ts`。`generation/relational-input.ts` 复用原规范化/槽位/协议规则，`storage/relational-input-store.ts` 保存唯一正文、input 内 BLOB 和首次上下文，提供受理引用、授权输入/附件读取及执行冷读取。

`storage/relational-member-store.ts` 复用原认证/Discord/会话 owner；`storage/relational-generation-store.ts` 组合规范输入、`quota/fair-share.ts` 的收费规划、`generation/member-contract.ts` 的确认和 `generation/accounting.ts` 的证据。`storage/relational-quota-store.ts` 复用原观察查询、发起顺序、恢复与运营资金 owner；物理执行槽和 SQL 外的字节发布仍分别由现有执行/文件 owner 持有。

初始化、未完成恢复或仅作归档保全的目标可保持 building；完整提交重建归 `storage/relational-replay.ts`，合法 ready 安装归 `storage/relational-ready.ts` 的唯一 installer。运行方式与完整核对见[关系格式离线恢复](../docs/operations/relational-recovery.md)，旧格式解释见[独立旧归档工具](../docs/operations/legacy-archive-lookup.md)。这些工具能力不替代同候选整体验收或生产切换记录。
