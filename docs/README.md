# 文档导航

按用途查阅，不按文件名猜实施状态。计划和目标接口不代表已实现；正式运行版本、发布与回退依据统一看运维发布记录，历史资料中的版本和测量只代表当时阶段。

开发入口、架构与 migration 守卫、格式/注释规范和实际验证命令见 [开发与验证](../CONTRIBUTING.md)；当前模块归属见 [服务端](../server/README.md)。

| 目录 | 内容 |
| --- | --- |
| [plans](plans/) | 当前重构与后续扩展计划 |
| [contracts](contracts/) | 现有 API 行为及目标接口契约 |
| [architecture](architecture/) | 服务边界、业务规则、数据流和前端接入 |
| [operations](operations/) | 测试部署、生产发布、备份与回退 |
| [history](history/) | 已完成阶段、评估、实测与性能基线 |

## 计划

- [当前重构计划](plans/architecture-refactor-plan.md)：单执行器、单 Opus、SQLite 下的事件溯源、成员生成接口、工作包与验收。
- [后续扩展计划](plans/scaling-follow-up-plan.md)：多执行器、多账号/车队及数据库迁移，未纳入当前实施范围。

## 接口契约

- [现有 API 契约](contracts/api-contract.md)：Native、OpenAI、成员门户及任务/结果的兼容边界。
- [成员生成契约](contracts/member-generation-contract.md)：已在重构分支实现的能力描述、统一报价、费用确认、受理及配套读取；验收状态统一见当前计划。

## 架构与实现

- [执行服务](architecture/executor-service.md)：Nuxt 与独立执行器的进程、权限和传输边界。
- [请求生命周期](architecture/request-lifecycle.md)：持久请求、幂等、批次、取消、排队和冷却。
- [结果恢复](architecture/result-recovery.md)：产物发布、结算与崩溃恢复保证。
- [失败退款与资源核对](architecture/failure-accounting.md)：本地退款、未知消费、运营损耗及核销。
- [成员会话](architecture/member-sessions.md)：登录期限、续期、撤销与角色变化。
- [状态通知](architecture/state-events.md)：事务后失效通知、授权过滤及两段 SSE。
- [调用热力图](architecture/call-heatmap.md)：统计口径、查询和页面展示。
- [Nuxt 成员页](architecture/nuxt-member-portal.md)：成员前端开发与接入。
- [Nuxt 管理页](architecture/nuxt-management-portal.md)：owner/管理员共用界面与操作边界。

## 运维

- [正式发布与回退](operations/production-release.md)：当前发布记录、生产配置、备份及回退依据。
- [隔离测试部署](operations/test-deployment.md)：测试环境、模拟上游、发布产物和阶段验收。

## 历史与证据

- [现代化实施记录](history/modernization-plan.md)：上一轮计划、决定、提交与验收。
- [防滥用方案与实施更新](history/novelai-gateway-abuse-prevention-proposal.md)：原始提案及当时实施状态。
- [ORM 评估](history/orm-evaluation.md)：Drizzle 接入的选择、实验和边界。
- [官方协议实测](history/novelai-protocol-probes.md)：固定时间的请求/响应证据。
- [队列基线](history/queue-baseline.md)：排队数据与采样限制。
- [浏览器基线](history/browser-baseline.md)：首屏、缓存和浏览器验收证据。
- [事件重构性能验证](history/refactor-performance-evidence.md)：对照方法、正式运行口径、容量和重放证据；小样本与正式结果分别标识。

新增文档放进对应目录并补充此索引；技术规则维护在所属文档，历史测量保留原日期和证据，不复制多个“当前版本”说明。
