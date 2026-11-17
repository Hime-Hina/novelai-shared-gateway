# 文档导航

按用途查阅，不按文件名猜实施状态。计划和目标接口不代表已实现；正式运行版本、发布与回退依据统一看运维发布记录，历史资料中的版本和测量只代表当时阶段。

开发入口、架构与 migration 守卫、格式/注释规范和实际验证命令见 [开发与验证](../CONTRIBUTING.md)；当前模块归属见 [服务端](../server/README.md)。

| 目录                          | 内容                                 |
| ----------------------------- | ------------------------------------ |
| [plans](plans/)               | 当前重构与后续扩展计划               |
| [contracts](contracts/)       | 现有 API 行为及目标接口契约          |
| [architecture](architecture/) | 服务边界、业务规则、数据流和前端接入 |
| [operations](operations/)     | 测试部署、生产发布、备份与回退       |
| [history](history/)           | 阶段记录、评估、实测与决策材料       |

## 计划

- [架构重构计划](plans/architecture-refactor-plan.md)：单执行器、单 Opus、SQLite 下的事件溯源与成员生成接口目标、工作包及分阶段验收记录。
- [数据库与数据模型重构](plans/database-model-redesign.md)：数据模型、owner、已确认迁移选择和剩余验收；进行中状态统一见 [#32 顶部状态](https://github.com/Hime-Hina/novelai-shared-gateway/issues/32)；观察/资金来源契约见 3.3，跨对象反例验收见 7.3。离线恢复与合法 ready 安装见[关系恢复指南](operations/relational-recovery.md)，实际部署和生产切换以[正式发布记录](operations/production-release.md)为准。
- [后续扩展计划](plans/scaling-follow-up-plan.md)：多执行器、多账号/车队及数据库迁移，未纳入当前实施范围。

## 接口契约

- [现有 API 契约](contracts/api-contract.md)：Native、OpenAI、成员门户及任务/结果的兼容边界。
- [成员生成契约](contracts/member-generation-contract.md)：已在重构分支实现的能力描述、统一报价、费用确认、受理及配套读取；验收状态统一见当前计划。

## 架构与实现

- [执行服务](architecture/executor-service.md)：Nuxt 与独立执行器的进程、权限和传输边界。
- [请求生命周期](architecture/request-lifecycle.md)：持久请求、幂等、批次、取消、排队和冷却。
- [结果恢复](architecture/result-recovery.md)：原产物提交规则及独立旧工具的升级/重放说明；当前关系格式恢复见[运维指南](operations/relational-recovery.md)。
- [失败退款与资源核对](architecture/failure-accounting.md)：本地退款、未知消费、运营损耗及核销。
- [成员会话](architecture/member-sessions.md)：登录期限、续期、撤销与角色变化。
- [状态通知](architecture/state-events.md)：事务后失效通知、授权过滤及两段 SSE。
- [调用热力图](architecture/call-heatmap.md)：统计口径、查询和页面展示。
- [Nuxt 成员页](architecture/nuxt-member-portal.md)：成员前端开发与接入。
- [Nuxt 管理页](architecture/nuxt-management-portal.md)：owner/管理员共用界面与操作边界。

## 运维

- [正式发布与回退](operations/production-release.md)：当前发布记录、生产配置、备份及回退依据。
- [隔离测试部署](operations/test-deployment.md)：测试环境、模拟上游、发布产物和阶段验收。
- [关系格式离线恢复](operations/relational-recovery.md)：完整提交与保护内容重建、所选私有快照及合法 ready 安装；不解释旧格式或执行生产切换。
- [数据库模型 B 切换操作单](operations/database-model-cutover.md)：最终 cut、排空、转换/恢复、实际包启动及失败处置的待签收步骤；当前未授权或执行生产切换。
- [独立旧归档精确查询](operations/legacy-archive-lookup.md)：固定旧包的原身份、原回执、保护输入与本地图审计，不接入在线运行时。
- [B 迁移选定原图准备](operations/legacy-original-materialization.md)：选定 R2/PikPak/已取本地原图与同一离线转换入口的来源绑定，不构成旧远端承诺或 ready。

## 历史与证据

- [现代化实施记录](history/modernization-plan.md)：上一轮计划、决定、提交与验收。
- [防滥用方案与实施更新](history/novelai-gateway-abuse-prevention-proposal.md)：原始提案及当时实施状态。
- [ORM 评估](history/orm-evaluation.md)：Drizzle 接入的选择、实验和边界。
- [官方协议实测](history/novelai-protocol-probes.md)：固定时间的请求/响应证据。
- [队列基线](history/queue-baseline.md)：排队数据与采样限制。
- [浏览器基线](history/browser-baseline.md)：首屏、缓存和浏览器验收证据。
- [事件重构性能验证](history/refactor-performance-evidence.md)：对照方法、正式运行口径、容量和重放证据；小样本与正式结果分别标识。
- [数据模型实施与验证记录](history/database-model-execution-records.md)：固定 6279450 原第 9 章全文，保留各候选的失败、独审、执行边界与原锚点；现行规则回到所属契约。
- [数据模型迁移前置盘点](history/database-model-preflight.md)：固定线上副本的聚合盘点、终态开账类别试验、实际回放覆盖及待决定的恢复范围；不代表正式迁移或切换验收。
- [真实旧归档恢复验证](history/database-model-archive-recovery-20261114.md)：固定旧包的真实源审计、从零事件重建、资源测量及历史查询/审计来源缺口；不代表完整归档或新格式 ready。
- [数据模型迁移选择](history/database-model-migration-decision.md)：已确认的 B、旧键、会话、缺图四项选择、保留范围及执行顺序。
- [真实 B 转换与联合恢复](history/database-model-b-migration-acceptance-20261116.md)：固定实际迁移包的完整来源核对、34 表联合恢复、原生 SQL 故障、重复零写和资源测量；目标仍为 building，后续 ready 与整体验收分别记录。
- [完整 B、ready 与实际发行物验收](history/database-model-final-acceptance-20261118.md)：完整原图材料、唯一合法安装、实际 executor／Nuxt／dirty-WAL 恢复、独立归档及分项容量；真实新派发和生产切换另行签收。
- [数据模型完整源码验证](history/database-model-full-source-20261115.md)：固定 df0、b9、0094243 与 147aabd 四轮完整后端的实际失败阶段、封存证据与各轮环境资格；不是最终验收。
- [Issue #14 后续验收](history/issue14-validation/summary-6d64fa1.json)：本轮候选、跨平台测试、原失败及日志摘要；实现和容量/权限限制见[计划第 16 节](plans/architecture-refactor-plan.md#16-issue-14-后续实现与验收2026-11-07)。

新增文档放进对应目录并补充此索引；技术规则维护在所属文档，历史测量保留原日期和证据，不复制多个“当前版本”说明。
