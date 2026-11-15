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

## 后继 b9 的第二次完整运行

固定 `b9fef2bbebf07e886f6469250fa97b978c9fef5a` 在另一真实 Git 隔离树运行同一原命令，仍为 44 unit、126 integration、并发 4 和原期限/断言/平台条件。一次 exit 1，TAP **323.2108703 秒**、Popen 323.796 秒：**2,885 节点为 2,873 pass、6 fail、1 cancelled、5 原 skip**。2,079 个顶层的七个 `not ok` 已包含六项失败与一项超时取消，不能再加算，也不是从旧 40 fail 扣减得到。

六项失败的实际首次阶段为：`failure-accounting` 原部分收费历史用例 child 启动退出 1；四项 `native-http` 仍在共享旧夹具 SQL `near ")"` 初始化失败；`r2-history-http` 在 child 启动等待时退出 1。本轮均未到对应内容/资金的完整业务 oracle，不能凭这些早期错误判定更深产品原因。历史到期核对、查询、输入、凭据、打包与存储边界的已整合后继实际进入本轮，具体修复及独立反例见计划 9.101–9.103；完整运行仍非通过。

取消项为原 `result-recovery-http` 的 503 条冷历史用例，在原 60 秒触发 `testTimeoutFailure`。原测试与此前成功 `8b85b2a` 的 Git blob 和 SHA 完全相同，数量/事务/期限未改。本轮只读 observer 记录 mock listen 到首轮第 501 条实际 HTTP 之间 58.816311 秒，区间包括准备、安装和启动，不能猜成某一步 SQL 耗时。首轮 PNG/opaque 内容与权限请求及第二个实际 child 入口有记录；第二次监听、HTTP、SQLite/安全错误和精确等待点尚未取证，两个 child 最终均关闭。旧独审该项 38.527 秒的通过仍保留为原候选证据，不使本轮超时变绿；下一步只做阶段诊断，不延长期限或减少数据。

617 个实际 tracked 文件逐 Git blob/raw 绑定，Node/native/pnpm/批准 preload 和 runner 前后完全相同。Python 显式创建新环境块，初始 pnpm Node 的 16 个实际名字与传入集合相等；后代允许原 pnpm metadata 与合成夹具变量，只记录名字，不谎称全部仅 OS 名单。393 个有入口记录的 OS Node 身份均有守卫和版本信息，336 个 self-exit 加 57 个父句柄退出覆盖这些身份；另 13 个 worker thread 随所属 OS 进程退出。主 coordinator 没有 self-prelude，仍单独使用真实 argv/父句柄/OS 退出证据；两个复用为 Chrome/svchost 的 PID 没有被终止。旧 df0 环境资格说明不追溯升级。

新证据独立封存于 `H:/TEMP/novelai-root-full-postfix-20261115/`，没有覆盖旧运行：

| 文件                     | SHA256                                                             |
| ------------------------ | ------------------------------------------------------------------ |
| `REPORT.md`              | `741097e38a6d62d82b5b65d39f0499622d97ada29e322984d728693e69c687f0` |
| `FAILURES.md`            | `96ccf7b8619055383f935a8fe45cbabdf80da3f959a62981b51290f696a6b45a` |
| `evidence-manifest.json` | `c3b101792b6b1d04305337ec6b6a7d385b3af85e04c305dd0d425e683ef61cdc` |

静态准备的 before JSON 在正式 launch 按 runner 重写时仅发生 LF→CRLF 序列化变化，规范化后相同；实际运行 before/after 字节完全相同，原静态版本及 `metadata-serialization.json` 单列。以上只证明当前完整源码的实际失败范围、来源和退出，不替代发布物、迁移、实外端容量或生产切换。

## 后继 0094243 的第三次完整运行

固定 `00942434283ee18f2faf1e457d01d6f60e2050bb` 在另一真实 Git 隔离树运行原完整命令，仍为 44 unit、126 integration、并发 4 和原期限/断言/平台条件。一次 exit 1，TAP **346.717144 秒**、Popen 347.469 秒：**2,899 节点为 2,888 pass、6 fail、零 cancelled、5 原 skip**。2,079 个顶层中四个 `not ok`，另两个子级失败已计入六 fail；父汇总不另作业务根因。

| 实际入口 | 本轮失败阶段 |
| --- | --- |
| `failure-accounting` 原部分收费历史 | child 在启动等待时退出 1，尚未到领取及账务 oracle；原缺账户/时间/父关系等来源不能凭当前默认补齐 |
| `native-http` 原 Local 缺图子例 | 原期望 500，实际 503；该子例后面的安全正文断言未到；另含一个父汇总失败 |
| `r2-history-http` | 末尾原期望 500，实际 503；前面的字节/ACL、坏来源、正文、停止及公共回放断言已到 |
| `relational-sole-runtime` 的 `done` 子例 | 首次基线领取在原 `:1103` 比较失败：实际 887 B XML/SVG 超时提示，预期 34 B WebP；另含一个父汇总失败 |

sole 原 `DIRECT_GENERATE_TIMEOUT_MS=100` 未改。该次失败尚未进入 `:1106` arm-IO、`:1113` shutdown 或 `:1131` latewire 比较，因此没有观察到这些后继路径失败；原 TAP 只给出了响应字节和 stack，没有首次响应 header 或更深阶段取证。不能把提示内容直接推成关停重领缺陷、修改图片期望或延长期限来消红。Native/R2 状态码仍按[迁移决策材料](database-model-migration-decision.md)等待维护者选择，当前完整运行保持 NOTPASS。

原 503 条冷历史本轮实际通过，**54.9141193 秒**，仍在原 60 秒限制内；此前 b9 的取消和私有阶段观察记录保留。这一轮并发、夹具和产品并集均不同，不将通过单独归因于两个同步读取优化，也不宣称并发性能问题已普遍消失。

617 份实际 tracked 文件的 Git/blob/raw、Node/native/pnpm/批准 preload 和 runner 前后相同，初始 pnpm Node 的 16 个实际环境变量名与 Python 显式传入块相等。本轮 before-source 静态与实际运行原字节完全相同，没有 b9 的 metadata 换行差别。411 个入口记录对应 398 个 OS Node 身份和 13 个线程；340 个 self-exit 加 58 个实际父句柄退出覆盖这些 OS 身份，线程随已退出 owner 关闭。主 coordinator 仍单列真实父句柄/OS 退出证据，不冒 self-prelude；两个复用为 svchost/Firefox 的 PID 没有被终止。所有已记录原测试和子进程均关闭后释放运行槽，没有自动重跑。

本轮证据独立封存于 `H:/TEMP/novelai-root-full-integrated-0094243-20261115/`。本记录只覆盖固定源码的完整原测试，不代替实际 emitted JS 包、Node24、三包/Nuxt、Docker、真实对象端点、迁移、同规模恢复或切换。

| 文件 | SHA256 |
| --- | --- |
| `REPORT.md` | `ec888e967807cdefe6602a15821208770753c87a5e6e5751ce9b70f8c4fcfafe` |
| `FAILURES.md` | `b27f92032826125e74b25ffc4520b82dd393d9a9cff90140825d83762145cb35` |
| `EVIDENCE-MANIFEST.json` | `fbfa1e11a404a9880849606db22cd2efe7e0fe5bc67f7d98a44dc90a15fd9ad7` |

## 后继 147aabd 的第四次完整运行

固定 `147aabd6f7996442d5c8d242379e080ff27afe19` 在另一真实 Git 隔离树运行一次原完整命令，仍为 44 unit、126 integration、并发 4 和原期限/断言/平台条件。实际 exit 1，TAP **343.8323078 秒**、Popen 344.407 秒：**2,917 节点为 2,908 pass、4 fail、零 cancelled、5 原 skip**。2,088 个顶层中三个 not-ok，另一个子级已计入四 fail；Native 父汇总不是独立业务根因。未过滤、重试或从旧结果扣减，完整后端仍未通过。

原部分收费历史仍在 child 启动等待时退出 1，本轮没有更深的 SQLite/安全错误原因，尚未到签名图片领取及账务 oracle。Native 原 Local 缺图仍期望 500 而实际 503，后续安全正文断言未到；R2 在前置正文隐私断言之后的末尾原状态断言同样收到 503。500/503 按原决策材料等待裁定，不翻断言消红。

本候选已包含 9.111 的两条 retained 产品路径和 9.112 的 done 前置修正，受影响源码逐字绑定到各自独审候选。原 sole queued/done 整例本轮 **14.9545624 秒通过**，仍保留 100 ms/40 s 及原断言；原 503 条冷历史 **55.6516313 秒通过**，仍在原 60 秒期限内。这些是本轮真实并发结果，不抹去早期失败，也不证明通用性能上界。

617 个实际 tracked 文件及额外路径空集、Node/native/pnpm/控制前后相同，实际 before/after 源快照 SHA `d9f823407e5e5c6723ac096a9e096c184b5da297c355c4761261498d3d436abe`。初始 pnpm Node 的 16 个实际环境变量名与 Python 显式传入块相等；后代保留原 pnpm/合成夹具行为，不冒称所有后代只有 OS 环境。400 个 OS Node 身份由 343 个 self-exit 和 57 个实际父句柄退出闭合，13 个线程随实际 owner 关闭；主编排仍单列真实 argv/OS 证据。373 个 PID 数字的终态核对中 371 个已不存在、两个已被 svchost/Firefox 复用，未终止这些无关进程。实际测试及进程世代全部关闭后才释放运行槽。

准备期曾在启动 Node 前遇到静态 matcher 错误，原记录保留；没有执行用例或在正式运行中修改控制。新证据独立封存于 `H:/TEMP/novelai-root-full-postclosure-147aabd-20261115/`，旧三轮原件不改。本轮不覆盖实际 emitted JS/Node24、Nitro/Web/Docker、真实云端、迁移或同规模恢复，后续按同候选实际产物另行验收。

| 文件 | SHA256 |
| --- | --- |
| `REPORT.md` | `b138f969bdc80bff535aad8fd6dc2b9d36ad985c81ec69668d4e27a25ee6c6a5` |
| `FAILURES.md` | `82d0deb2afd144f7638d83fb1be6aa912062fc09a4d9013ba686c8c2f5bda9f3` |
| `EVIDENCE-MANIFEST.json` | `54915352223704110604165a1b3d948f5a5f990823bc68b84ffda47997a34af3` |
| `full.log` | `4c2502af8714f9dbe36d6528f1640e84b3c54aa15a7e4d238cb985928cc0b1e9` |
