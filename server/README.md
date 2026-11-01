# 服务端

HTTP 网关、成员鉴权、共享额度账本与持久化模块。来源和非商用许可范围见仓库根目录 NOTICE.md 与 LICENSE。

当前执行器独占业务写入和上游调度，Nuxt 只通过内部 HTTP 转发。`index.ts` 与独立 Nitro 入口复用 `gateway-runtime.ts`；它们是替代启动方式，不能据此启动两个当前版本的账本写入者。

| 规则/事实 | 当前归属 |
| --- | --- |
| 参数、上游协议、报价 | `native-image.ts`、`providers.ts`、`native-pricing.ts` |
| 分配、恢复、预扣与退款 | `fair-quota.ts` |
| 任务证据、未知结果与运营账 | `generation-accounting.ts`、`operating-accounting.ts` |
| 队列、批次、冷却与免费限制 | `queue-policy.ts`、`generation-request.ts`、`shared-capacity.ts`、`free-image-policy.ts` |
| 会话、Discord 与管理入口 | `member-auth.ts`、`discord-auth.ts`、`member-routes.ts`、`owner-operations.ts` |
| 原子持久化、冷历史与查询投影 | `store.ts`、`storage-schema.ts`、`sqlite-read-worker.ts` |
| 产物恢复、本地/R2 存储与通知 | `result-files.ts`、`r2-storage.ts`、`cold-images.ts`、`state-events.ts` |

已提交 SQLite 记录是当前权威数据；活动预留在 fairQuota，冷结算凭据优先于 job 中的旧副本。`fairQuotaSummary()` 当前仍会修改输入，查询调用者须使用独立副本。新版本将调整这些边界，详见[架构重构计划](../docs/architecture-refactor-plan.md)；事件溯源及[成员生成新接口](../docs/member-generation-contract.md)尚未实现。多执行器、多账号调度和数据库系统迁移已移至[后续计划](../docs/scaling-follow-up-plan.md)，不作为当前重构前置。

继续工作先读该计划的目标、事实表和工作包状态，再按所改规则读取具体模块。[API 契约](../docs/api-contract.md)维护外部行为，[结果恢复](../docs/result-recovery.md)维护产物提交边界，[发布记录](../docs/production-release.md)维护运行版本与回退证据。
