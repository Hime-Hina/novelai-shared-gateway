# 数据模型固定候选的完整源码验证（2026-11-15）

固定候选 `df0ac66c353af7c0142ed3f33b83545f7b60f665`，在真实 Git 隔离工作树运行原 `pnpm test`：Node 22.23.4 / ABI 127、pnpm 10.32.1、Windows better-sqlite3 12.10.0。原命令为 `node --test --test-concurrency=4 tests/unit/*.test.mjs tests/integration/*.test.mjs`，包含 44 个 unit 和 126 个 integration 文件；没有增加超时、跳过已知失败或自动修复重跑。

一次执行 exit 1，TAP 用时 **318.3372094 秒**，共 **2,883 个节点：2,838 pass、40 fail、5 项原条件 skip、0 cancel**。2,077 个顶层中有 29 个失败，另 11 个子级失败已包含在 40 内，不能相加。该候选尚未通过完整后端验收。

## 首次失败阶段

下表按共同入口分组，不将早期拒绝称为目标业务反例通过，也不预判所有失败均为夹具问题。修复须先追当前事实、模块 owner 和原断言，保留 NULL/已知 0、金额、授权及回滚的辨别力。

| 范围与原测试文件                                              | 顶层失败数 | 首次实际阶段                                                             |
| ------------------------------------------------------------- | ---------: | ------------------------------------------------------------------------ |
| `failure-accounting`、`generation-review`                     |          2 | 真实 child 启动退出；后者为旧数据库格式拒绝，未到迟到图片领取            |
| `native-http`                                                 |          4 | 共享夹具 SQL 的 `near ")"` 错误，未到 HTTP/退款/Direct/multipart         |
| `r2-history-http`                                             |          1 | child 在启动等待阶段退出，未到 Local/R2/PikPak 领取                      |
| `relational-admin-summary-http`、`relational-management-http` |          2 | `OpeningRequestRecorded` 精确字段校验，未到未知时间/金额和最近 30 条查询 |
| `relational-generation-credential-failure`                    |          1 | 两个 401/403 子例的旧文案正则与实际 `quota_sync_error` 拒绝不同          |
| `relational-input`                                            |          2 | 活动行来源/完整输入 DDL 门禁；partial 原请求缺先行来源登记               |
| `relational-opening-entities`                                 |          1 | 副本来源/状态拒绝，未到唯一 ready 安装与缺字节反例                       |
| `relational-opening-replay`                                   |          2 | `OpeningAccountRecorded` 字段校验早于预期的来源和提交顺序检查            |
| `relational-storage`                                          |         10 | 共用物理 seed 被活动行门禁拒绝，未到金额、附件、尝试、来源和旧键等边界   |
| `package-backend`                                             |          4 | 合成发射目录缺 `scripts/retained-sources.mjs`，遮挡闭包及多个坏依赖反例  |

## 证据与运行资格

源码的 615 项 Git/raw 绑定、Node、pnpm、共享 native 及批准 preload 的 hash/mtime 前后相同。真实测试 worker、server 和 CLI 的入口前网络守卫有自记录；纯测试主编排有实际 argv 和 OS 退出证据，不冒称它执行了项目 preload。四项独立校准核对同步子进程、原 promisify、fork/IPC、原 mock 顺序和非 loopback 的 IO 前拒绝。完整测试及已记录的实际子进程均已退出；重复 PID 按进程身份区分，没有终止无关进程。

**严格环境白名单未获实际 Node 环境块证据。** 原 runner 执行了 .NET 清理环境变量，但未保存 Node 内实际变量名集合；不能将脚本意图写成已经证明没有 ambient 配置，也不能由此推断发生凭据泄漏。该限制已在独立追加记录说明；原日志、报告和 manifest 未覆写。后继验证改为显式新进程环境块，并在实际 Node 内核对变量名；这不追溯升级本轮资格。

封存证据位于本地 `H:/TEMP/novelai-root-full-df0ac66-20261115/`：

| 文件                           | SHA256                                                             |
| ------------------------------ | ------------------------------------------------------------------ |
| `REPORT.md`                    | `09918c3bf2d42875d62b2627c60d53dc4ea644f204c55af9a5bdbdcdc5808c38` |
| `evidence-manifest.json`       | `e11e1c6707e167db74dcec06326ba5b76c28dbfd7c33bf214dad44080068c48f` |
| `FAILURES.md`                  | `2fc5cfdf71de97f9d44dbeb5392a908986c76374c992198cd8a1454e9daacbc0` |
| `ENVIRONMENT-QUALIFICATION.md` | `6f801b130206a8c716b32befd81cdba3ea05b38c085a7ec388e3306e87b97514` |

`failures-final.json` 保留全部 40 个原 TAP 失败块、精确源码位置及子例归属。后继候选的窄通过只能按其提交和实际范围另列；不能从本轮结果、旧 PR CI 或局部包检查推导 compiled、三包、Nuxt、Docker、真实外部存储、迁移、同规模恢复或生产切换已验收。总体状态与待选事项仍见[数据库模型计划](../plans/database-model-redesign.md)和[迁移决策材料](database-model-migration-decision.md)。
