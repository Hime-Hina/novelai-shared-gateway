# 服务端

HTTP 网关、成员鉴权、共享额度账本与持久化模块。来源和非商用许可范围见仓库根目录 NOTICE.md 与 LICENSE。

当前执行器独占业务写入和上游调度，Nuxt 只通过内部 HTTP 转发。`index.ts` 与独立 Nitro 入口复用 `gateway-runtime.ts`；它们是替代启动方式，不能据此启动两个当前版本的账本写入者。

| 规则/事实 | 当前归属 |
| --- | --- |
| 参数、上游协议、报价 | `native-image.ts`、`providers.ts`、`native-pricing.ts` |
| 成员能力、费用确认、受理与查询 | `generation-capabilities.ts`、`member-generation.ts`、`generation-admission.ts`、`generation-execution.ts`、`generation-queries.ts` |
| 分配、恢复、预扣与退款 | `fair-quota.ts` |
| 任务证据、未知结果与运营账 | `generation-accounting.ts`、`operating-accounting.ts` |
| 队列、批次、冷却与免费限制 | `queue-policy.ts`、`generation-request.ts`、`shared-capacity.ts`、`free-image-policy.ts` |
| 会话、Discord 与管理入口 | `member-auth.ts`、`discord-auth.ts`、`member-routes.ts`、`owner-operations.ts` |
| 领域事件、定点金额、重放与查询投影 | `domain-events.ts`、`event-store.ts`、`quota-events.ts`、`resource-amounts.ts`、`store.ts`、`sqlite-read-worker.ts` |
| 产物恢复、本地/R2 存储与通知 | `result-files.ts`、`r2-storage.ts`、`cold-images.ts`、`state-events.ts` |

本分支的账务和任务生命周期以已提交的领域事件为权威；钱包、预留、任务状态、冷结算和免费次数由同一事务更新投影。金额核心使用 12 位定点整数运算，兼容旧 DTO 时才转换为 number。`fairQuotaSummary()` 为纯查询，需要恢复估算时使用独立的 `estimateFairQuota()`；能力、报价与提交共用 Native 规范化和计价规则，Cookie 成员闭环见[成员生成接口](../docs/contracts/member-generation-contract.md)。

首次转换从旧库建立 opening facts，不补造过去没有保存的逐次事件。启动先恢复文件结果和中断任务，再核对事件投影并启用事件权威，成功后才监听及派发；任何不一致会阻止切换。完整输入、首次规范化上下文、认证数据和产物字节仍需与事件一起备份，重放命令不会调用上游。具体边界和命令见[结果恢复](../docs/architecture/result-recovery.md)。

当前 writer/minimumWriter 和 reducer 版本为 2，事件格式仍为 v1。已有 v1 权威库会在启动恢复之前，从不可变事件重建投影，并在同一事务内核对、安装和提升版本标记；事件、输入和提交回执不变。重放的 v1 checkpoint 也会先重建再续跑。升级后的库不允许旧 v1 程序写入或重新安装投影，回退须使用升级前的完整备份；未知更新版本在迁移或投影安装前被拒绝。

这些是当前代码边界，不代表生产已完成升级；实际版本与授权以[发布记录](../docs/operations/production-release.md)为准。多执行器、多账号调度和数据库系统迁移仍在[后续计划](../docs/plans/scaling-follow-up-plan.md)。

继续工作先读该计划的目标、事实表和工作包状态，再按所改规则读取具体模块。[API 契约](../docs/contracts/api-contract.md)维护外部行为，[结果恢复](../docs/architecture/result-recovery.md)维护产物提交边界，[发布记录](../docs/operations/production-release.md)维护运行版本与回退证据。
