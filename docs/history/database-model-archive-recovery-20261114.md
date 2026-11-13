# 固定旧工具的真实归档恢复验证

本记录接续 [#48 的源盘点](database-model-preflight.md)，对应 #50/#51 的恢复取证，测量日期为 2026-11-14（Asia/Taipei）。实际旧解释器固定在 `3caba3dfa0b5d9f582acabfc02e36e7db4af038f`，工具包固定在 [PR #62](https://github.com/Hime-Hina/novelai-shared-gateway/pull/62) 的 `d5a19f8`。此次取得真实封存源审计、从零事件重建及重复回放证据，也确认旧事件不足以恢复全部历史查询和审计来源。完整原始 SQLite 仍是归档权威，保留在私有证据目录；目标保留供独立审查，没有修改固定旧 owner、手补目标字段或操作生产。

[安全聚合 JSON](database-model-archive-recovery-20261114.json) 记录代码身份、逐表结果、资源采样与缺口，不含提示词、原成员/产物 ID、令牌、上下文或诊断原文。A/B 路线仍待维护者决定；缺字段不是删除历史的依据。

## 固定输入与实际入口

源库来自既有获准一致性副本：切点 `16509`，605,024,256 B，SHA-256 为 `47ea07f440d7652c11ebced52c17883ad821e12411d4dbd046bcc1ed319f4821`。源 header 为 DELETE 模式且无 WAL/SHM/journal，始终以 `readonly` 打开；没有对源调用 `JsonStore.init()`、迁移或 checkpoint。源文件摘要及四项递归目录元数据快照前后相同。

先核对原独立审查的实际 tar：SHA-256 `daf8b12fc7f863eff23fde6cc9b37fc6487567a749b04979e5a5cb447c4f1dde`；解包后 `--verify-package` 的 4027 项资产通过。实际审计器 SHA-256 为 `9ecff2111101ddb0bb282106df95c803c68aa42d0d17db0fcd6c68b0cfbb5e70`。未以开发工作树的依赖解析替代实包，也未输出原始内容。

审计入口完整核对 schema/原 SQL journal、连续提交和当前 clock/core 切点、流摘要、影子行、冷凭据及受保护摘要。它的 `verifyEventProjections()` 使用已保存 stream，不能称作从零重建。随后另从同一固定 Git 对象逐字节导出[原 `scripts/replay-events.mjs`](https://github.com/Hime-Hina/novelai-shared-gateway/blob/3caba3dfa0b5d9f582acabfc02e36e7db4af038f/scripts/replay-events.mjs)，入口 SHA-256 为 `f48d74f3897460b0a1ad5e6b78de8082713812b92c8a0eca9eb1d19f4e364359`；私有 runner 的 server/dependencies 解析到上述已验证包，未另写解释器。

本次从零运行在启动前确认目标不存在；原入口另支持同一历史的 checkpoint 续跑。`copyProtectedFacts()` 复制输入、上下文、产物输入、诊断、users/accounts/cards/member_sessions，以及不含 fairQuota 的 settings 和两项原 metadata。`replayDomainEvents()` 读取源完整提交，目标的 `appendDomainCommit()` → `reduceDomainEvent()` → `applyEventProjection()` 从事实建立流和查询投影；未复制源 `event_streams`、jobs、images、ledger 或 fairQuota。该路径没有网络、监听或派发，实际网络/监听阻断计数为零。

复现时沿用已验证固定包及上述原入口，在新的私有目标运行，stdout/stderr 只重定向到私有文件：

```sh
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --verify-package
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite
node /private/runner/scripts/replay-events.mjs --source=/sealed/library.sqlite --target=/private/new-target/rebuilt.sqlite
node /private/runner/scripts/replay-events.mjs --source=/sealed/library.sqlite --target=/private/new-target/rebuilt.sqlite
```

本次 runner 入口字节和实际引用的 server/dependencies 均已记录；测量脚本、只读逐表比较器、错误输出和目标保留在私有 checkpoint。首次测量启动器的 Windows `--import` 路径需改为 file URL，该预检失败发生在源打开前，未创建目标；没有为取得结果修改回放 owner 或受保护源。

## 实际结果与资源

本轮为 Windows x64 / Node 22.23.4 实测。工具包的 Linux 19/19 解包验收属于 [PR #62 的同候选 CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/43480703164)，不能将其写成本轮真实数据在 Linux 上的恢复测量。

| 阶段           | 实际结果                                                                                                                      |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| 真实源审计     | 16,509 连续完整提交、77,865 事件；9,690 输入共 171,047,481 B、40 上下文、8,830 产物输入、10 诊断、9,642 产物描述符；21.745 秒 |
| 从零回放       | 16,509 提交、77,865 事件到原切点；59.082 秒；目标主文件 602,951,680 B                                                         |
| 目标核对       | integrity 通过、外键缺口 0；原 `verifyEventProjections()` 通过；逐表核对 32.547 秒                                            |
| 重复原回放入口 | 2.059 秒，0 提交、0 事件，原切点不变；目标主文件 SHA-256 前后相同                                                             |
| 审计内存/TEMP  | 采样 OS 峰值工作集 370,548,736 B；专用 TEMP 未观察到落盘                                                                      |
| 回放内存       | 采样 OS 峰值工作集 318,832,640 B                                                                                              |
| 回放磁盘       | WAL 峰值 180,612,592 B；专用 TEMP 峰值 14,440,200 B；DB/WAL/SHM/TEMP 同刻合计峰值 783,650,484 B                               |
| 空间与保留     | 开始时 H 盘约 81 GB 可用，准备预算为 8 倍源文件约 4.84 GB；目标留存，最终有 0 B WAL 和 32,768 B SHM                           |

资源每 250ms 采样，可能遗漏更短或最后采样间隔内的峰值；不包括系统文件缓存、V8 独立堆、原图、备份总量或实际切换维护窗口。预算是开工余量检查，不是保证的空间上限。原回放器使用 WAL/FULL；只读验证目标可能留下 WAL/SHM，目标没有被封存安装。两库文件大小不同不证明去重收益；本次没有测量新格式同规模容量或备份/外部对象总量。

全部 event_commits、domain_events、39,036 个 event_streams、core projection/cut、受保护输入/上下文/产物输入/诊断、40 条幂等记录及 2,214 条免费使用凭据逐行摘要相同。9,652 条终态收费只有 JSON 序列化差异，规范内容相同。users、accounts、cards、member_sessions、settings 也相同，但这不等于已执行真实认证或恢复部署环境。

## 查询影响与必要审计来源

jobs、images、ledger 的稳定身份集合均完整，查询行并不全保真。以下缺口同时列明已测事实与沿原调用链的影响；没有运行恢复目标的真实 HTTP 或前端，不将推断写成端点验收。

| 已测差异                                                                                     | 用户可见查询或排查影响                                                                                                                                                   | 必须保留的来源                                                                          |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| 818 条 jobs 缺非空 `diagnosticId`                                                            | 原 Native HTTP 把同一相关号放入 `X-Nai-Diagnostic-Id` 并传给受理 job；缺失后历史 job 无法保持该响应与诊断日志的原关联。它与已恢复的十条 `diagnosticRef` 正文不是同一字段 | 原 jobs 保存关联；若要求联查原响应/日志，须另核其留存，不能用新随机号或十条诊断引用替代 |
| 820 条 jobs 缺 `queueTotal`、810 条缺 `generationProgress`、812 条缺 `sharedCooldown`        | 历史排队、进度和冷却展示/排查证据不完整；原 `publicErrorLog()` 使用 `queueTotal`，不可把缺失都当原值为零                                                                 | 原查询行及生成时证据；当前账户冷却不能反推每条历史任务的展示状态                        |
| 812 条 images 缺非空 `accountId`，对应 `account_id`/`search_text` 改变                       | 产物的原上游账户归属和按该关联的检索证据丢失；产物存在与原图引用存在不足以证明账户归属完整                                                                               | 原 image 行和可证明的任务/尝试关系；没有来源时保留未知，不套当前账户                    |
| 1,388 条 ledger 缺非空 `token` 引用，`token`/`user_token`/`search_text` 改变                 | 原记录的令牌关联检索与历史来源对账不能保持；不得将这些私有引用作为公开字段输出，也不能直接按当前 Key 替换                                                                | 原 ledger 记录、当时身份关联及原授权边界；迁移到稳定成员关联需有来源证明                |
| 812 条 ledger `order_value` 不同；相同 `ORDER BY order_value DESC` 的结果有 2,776 个位置不同 | 既有通用读取使用该排序，历史列表/分页和先后证据会变化；不能由 createdAt 或当前插入顺序猜原排序                                                                           | 原保存排序依据与稳定记录身份；本次 jobs/images 同一排序结果未变，不推广为普遍等价       |
| 383 条 `event_imports` 未复制；目标无 `event_authority`                                      | 开账分块摘要及其审计来源不完整，目标也未取得现行旧 writer 身份                                                                                                           | 原完整 SQLite、opening 元数据、导入分块及原 SQL/journal；不能手补成功/ready 标记        |

另有 9,681 条空 `errorDetail`、812 条空 `error`、820 条零 `payloadBytes` 在重建行中缺省；这些表示差异单独计数，不将它们混同于上面的非空信息丢失。jobs/images 的 `order_value` 均不同，但本次相同降序读取的稳定身份顺序相同；ledger 的排序变化已实际测得。

固定旧 owner 的影子核对只承诺其治理字段，因此通过不证明这些冷查询字段可被丢弃。源库仍完整，A 的映射或新 opening 需要从实际保存来源补充最小、有 origin 的诊断、展示、归属及必要顺序证据；B 也须保留所承诺在线薄历史和独立归档的完整来源，不把回放缺字段当作删减授权。本轮不同时扩展两套转换器。

## 本轮检查点与后续验收

已经完成：固定包与真实源的只读完整审计、原事件从零回放、目标真实 owner 核对、逐表对照、重复零写入、源不变及本机资源测量。保留目标与全部私有复现材料供独立审查，未修改原源、固定 3c 解释器或其 SQL/journal。

尚未完成：上述历史查询/审计来源的保真承接、外部原图与归档闭包、部署环境和真实认证恢复、A/B 正式转换、新格式合法 ready、同一发布候选的真实 HTTP/调度恢复、同规模备份/外部存储容量性能和生产切换。所选快照也不代表持续运行的生产已停写。后续证据应接着这些明确缺口验收，不将旧格式回放成功等同于新格式完成。
