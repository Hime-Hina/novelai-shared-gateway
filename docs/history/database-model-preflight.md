# 数据模型迁移前置盘点

本记录对应 [#48](https://github.com/Hime-Hina/novelai-shared-gateway/issues/48)，实现基线为 `main@1f60907eedc53726d28044000581f0c312d22f50`（与受审整合候选 `c6d6245` 同树），核对日期为 2026-11-14（Asia/Taipei）。这是只读工具、线上副本初次盘点和合成试验的交付记录，完整 A/B 选择报告仍待完成。规则继续归[设计第 6 节](../plans/database-model-redesign.md#6-一次迁移的两条路线)，不在本记录另定迁移政策。

## 远端基线与门禁

开始 fetch 时 `main` 为 `4161ea5`，PR #39/#40 已合并。核对 PR #43 固定 head `c6d6245`、独立复审与完整 CI #158 后，维护者明确授权按现有门禁合并；已通过带 expected SHA 的正常 merge 合入 `1f60907`，未使用 bypass。#44/#45 已自动关闭；确认 #46 的 `4741707` 是 main 祖先、契约与证据全部保留后，将其按已包含关闭。#30/#31/#32/#47–#53 继续开放，未把阶段实现标成总计划完成。

GitHub CLI 实际读取 main protection：必需检查为 `offline-validation`、`caddy-routing`、`node24-reader`，strict 和 enforce_admins 为 true，禁止 force push 和删除；没有配置 required_pull_request_reviews，rulesets 返回空列表。连接器对完整 protection 的读取为 403，CLI 的读取成功。没有修改任何保护设置。本批预检独立交付；账户命令和本地恢复从合入后的实际 main 分别建分支。

## 当前恢复能力的实际缺口

对事实字段表和四个实际业务回放入口逐项核对：74 类事实中，入口白名单并集为 47 类，另外 27 类尚无业务投影。这是源码覆盖统计，不是完整恢复通过率。

| 实际入口                                             | 已承担范围                                 | 尚不能据此声称完成的能力                                    |
| ---------------------------------------------------- | ------------------------------------------ | ----------------------------------------------------------- |
| `RelationalMemberStore.replayMemberCommit()`         | 稳定身份、状态、接单及资格历史             | 私有密码/Key/Discord/epoch/会话恢复；资格历史不恢复当前授权 |
| `RelationalInputStore.replayAcceptedInput()`         | 输入、BLOB、首次上下文、受理与 child       | 旧 opening 输入/任务解释；跨 owner 的同一完整提交           |
| `RelationalGenerationStore.replayGenerationCommit()` | 生成、预扣、扣退、终态、冷却及部分 opening | 副本、退休、旧请求/尝试 opening、薄键应用                   |
| `RelationalQuotaStore.replayQuotaCommit()`           | 观察、采纳、恢复、资金、核销及相关分配     | 政策/分组/权益转移、旧待到账 opening                        |

尚无投影的事实：

- sharing：`SharingConfigured`、`GroupConfigured`、`GroupMemberAssigned`、`GroupMemberRemoved`、`FallbackPolicyChanged`、`FreePolicyChanged`、`LossPolicyChanged`、`AccountBound`。
- account：`AccountRegistered`、`CredentialRotated`、`FundsTransferred`、`OpeningFundingRecorded`。
- request：`CachedResultLinked`、`JobHistoryRetired`、`RequestAliasRegistered`、`DiagnosticRecorded`、`OpeningRequestRecorded`、`OpeningJobRecorded`、`OpeningAttemptRecorded`。
- artifact/storage：`ArtifactReplicaRecorded`、`ArtifactHistoryRetired`、`OpeningArtifactRecorded`、`OpeningReplicaRecorded`、`StorageTargetRegistered`、`OpeningStorageTargetRecorded`。
- input/idempotency：`OpeningInputRecorded`、`OpeningIdempotencyRecorded`。

底层 `replayRelationalCommit()` 只保存完整提交、回执、流头和游标，不负责业务投影。现有混合历史用例按提交选择某一个业务入口，还不能处理同一提交跨多个存储 owner 的事实。未找到同切点私有快照导入/导出或合法 ready 安装入口；只读 `assertRelationalStorageReady()` 不能替代完整核对。后续覆盖清单随 #47/#49/#31 的命令更新，由 #50 汇总验证。

另有两个已确定的接入约束：账户启用旧 DTO 的 `{id, token}` 不足以表达凭据 A→B→A，契约与实现由 [#47](https://github.com/Hime-Hina/novelai-shared-gateway/issues/47) 第一片交付；新副本 `encoding` 当前只有 `identity/gzip`，但现行 PikPak 字节由 `PikPakStorage` 写为 Brotli `.br`。#49 必须保留实际编码，不能为适配字段而改标 gzip。

## 代表性试验与边界

只测试可证明的旧终态收费 opening 转换：任务已不存在，原结算凭据仍有效。原 opening 仍转换为 opening，不补造预扣/退款逐笔历史。试验只在测试内固定这一类别的映射，复用完整提交及现有业务回放，不提供另一套正式迁移产品。

薄终态能保存不等于薄键已接入受理防重；`OpeningIdempotencyRecorded` 仍缺业务投影和命令读取。两条路线都必须补齐它，不能通过伪造在线任务、关闭外键或新建成员身份回避。

### 真实源的困难类别与输入试验

对同一 `16509` 副本继续只读分类，前后数据库 SHA-256 均与下文一致。分析脚本及聚合保存在本地受保护的 `_private/database-model-20261114/difficulties.mjs` / `difficulties.json`；本次脚本 SHA-256 为 `b67f8d115e1dd96082f0355f116f977c45deedc851f7089752a60a348e1ded9f`。未改源、未发外部请求、未写入新库或伪造转换回执。

| 原来源与实际类别        | 已证明                                                                                                                         | 尚未证明或需要明确的处理                                                                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 9,690 份原输入          | 原事实摘要与成员归属全部匹配，旧恢复 owner 均可还原 wire                                                                       | wire 可读不代表首次规范化定义或原随机/固定意图已保存                                                                                                                      |
| 首次上下文              | 27 份有原 Native 定义，13 份只有网关默认值；另 9,650 份没有已存首次上下文                                                      | 不为 9,663 份缺完整定义的历史填当前默认；新输入恢复/复用应明确 partial 的真实能力，不能先宣称等价                                                                         |
| 27 份可证输入的实际往返 | 25 generate、1 upscale、1 encode-vibe；28 个实际 child 的完整出站参数和购买池许可相同，26 个生成 child 的实际 seed 相同        | 不外推至其余历史，未执行全库持久化迁移或数据库恢复                                                                                                                        |
| 对应附件                | 3 件共 871,872 B 拆出 BLOB 后恢复字节相同                                                                                      | 全源 80 个 wire 附件槽共 66,707,364 B 只完成句法 base64 检查，不等同完整语义验证                                                                                          |
| 旧键与批次              | 40 个旧键的原上下文均有事实对应；27 个利用原 Native 定义重算原 fingerprint 一致；25 个可证生成 parent 的首 seed/批次数往返相同 | 13 个仅默认值的键不能借当前定义证明；另 2 份生成输入没有显式 sampleCount，不能猜为单图                                                                                    |
| 9,652 份终态收费        | 每份均找到完全相同的原收费事实，身份、配置版本和原起止时间均存在；812 份后续收费为规范金额字符串                               | 8,840 份旧 opening 金额是 number，其中 6,481 份 amount 超过 12 位精度；这不是字段丢失。正式映射须复用已有旧金额 nearest 规则，原文/原摘要另存来源，不能原样冒充精确字符串 |
| 免费及 fallback         | 2,214 份 settled 免费收费各有独立成功凭据；8 份免费退款没有成功凭据。121 份 fallback 原两候选及归一后的等价量/预留量相符       | 不能把 resource=free 都重建成成功，不能把 fallback 未知消费改为单一已知实耗                                                                                               |
| 旧 opening 边界         | 28,764 条 opening 中包括 request 8,871、artifact 8,830、settlement 8,840                                                       | 保留 opening 边界，不补造开账前逐次生成/退款；真实源本切点无孤儿终态不替代合成薄凭据反例                                                                                  |

实际参数对照沿既有 `singleGenerationRequest()` / `buildNovelAiParameters()` 与新 `prepareRelationalInput()` → 拆附件 → `restoreRelationalWire()` / `relationalProviderRequest()` 执行，使用每个输入原保存的 Native 定义。27 份均完成对照，未利用当前定义补历史缺项。

这批证据支持继续优先评估 A：目前未发现原输入或终态收费事实丢失，尚无依据因缺首次定义就先选 B 缩小历史。上下文未知、数字金额和不可证明旧键是两条路线都必须处理的边界；B 也须保留这些来源及 2,214 个独立成功时间。完整转换覆盖、归档接口影响和成本尚未实测，路线仍交由后续具体报告决定。

## 线上一致性副本的首轮盘点

维护者授权从线上服务取证后，通过现有 SSH 管理入口确定实际运行目录仍为 `3caba3dfa0b5d9f582acabfc02e36e7db4af038f`。在 **2026-11-14 00:22:13（UTC+8）** 使用 SQLite 只读连接及 backup API 取得独立副本，包含已提交 WAL；没有停机、触发备份轮换、改变生产配置或调用真实生成。原库和配套清单保存在既有受限 release-prep 区域，未在用户家目录新建目录。

副本切点为 `16509`；原库 SHA-256 为 `47ea07f440d7652c11ebced52c17883ad821e12411d4dbd046bcc1ed319f4821`。传回后的压缩包、解压库及从同一副本读取的 9,642 条产物描述符分别校验摘要一致。描述符不是实际对象字节清单；Local/R2/PikPak 仍在运行，尚未验证对象闭包和归档恢复，未收集部署私有环境快照。原始库、描述符和秘密不提交仓库，仅提交[聚合 JSON](database-model-inventory-20261114.json)。

| 初次盘点                                                   | 实际结果                                                                 |
| ---------------------------------------------------------- | ------------------------------------------------------------------------ |
| 原库 / gzip                                                | 605,024,256 B / 147,445,430 B；gzip 是本次传输副本，不是日常加密备份大小 |
| 账户 / 成员 / 任务 / 产物                                  | 1 / 5 / 9,691 / 9,642                                                    |
| 提交 / 事件 / 流                                           | 16,509 / 77,865 / 39,036                                                 |
| integrity / 外键 / 提交事件数 / 未知事件版本               | 通过 / 0 / 0 缺口 / 0                                                    |
| 输入摘要/长度/归属、产物输入摘要、诊断摘要、流摘要         | 当前工具所检查的缺口均为 0；不代替全业务状态重建核对                     |
| 活动任务 / 未决核对 / 待发布结果                           | 0 / 0 / 0，仅代表该一致性切点                                            |
| 缺失目标的键/结算/免费凭据、未兑卡密记录、官方直连持久产物 | 本副本所查类别均为 0；仍保留合成反例，不能推断线上易失官方队列为空       |
| 已分配页 / 页内空洞 / 完全空闲页                           | 605,016,064 B / 69,337,476 B / 2 页；不将空洞计为已释放文件空间          |
| 盘点进程                                                   | 约 6.09 秒，进程峰值 RSS 196,517,888 B；不是迁移/恢复性能或文件缓存峰值  |

代表列的逻辑字节：`generation_requests.payload` 为 171,047,481 B，`jobs.data` 为 69,045,372 B，`images.data` 为 35,850,389 B，事件 payload 为 65,169,840 B，stream projection 为 48,341,381 B。JSON 的 `logicalContentBytes` 只计列出的代表列，排除其他列及索引；它们不能直接相加推算可释放空间。当前钱包三池总额、预扣及运营聚合值详见 JSON，只是已保存投影的读取结果，尚未与新基线/外部上游独立核对。

### 工具复现与输入边界

使用仓库 `.node-version` 的 Node 22.23.4 和锁定依赖：

```powershell
node scripts/database-model-inventory.mjs --source=H:\protected\snapshot.sqlite --source-commit=3caba3dfa0b5d9f582acabfc02e36e7db4af038f
node --test tests/integration/database-model-inventory.test.mjs tests/integration/database-model-migration-probe.test.mjs
```

`--source-commit` 是调用者声明的原程序版本，工具不会把声明当作已验证的 Git 证明；实际数据库及脚本字节分别计算 SHA-256。输入须为封存的单文件、无 WAL/SHM/journal、header 为 rollback-journal 的当前 v4 副本。原样打开 WAL header 的文件即使指定 readonly 也可能创建 sidecar，故入口在打开前拒绝。现有 SQLite backup 输出若仍为 WAL header，只在**另建的工作副本**上执行以下准备，不修改原归档或在线库：

```powershell
[System.IO.File]::Copy('H:\protected\original.sqlite', 'H:\protected\work.sqlite', $false)
node --input-type=module -e "import Database from 'better-sqlite3'; const db=new Database(process.argv[1]); db.pragma('journal_mode=DELETE'); db.close();" 'H:\protected\work.sqlite'
```

准备命令在目标已存在时拒绝，不覆盖既有证据；另记原归档和工作副本各自的摘要。有非空 WAL 的原材料须先用既有备份 API 形成包含已提交 WAL 的一致性副本，不能只复制主文件或删除 sidecar。工具本身不升级、checkpoint 或转换源库；成功/拒绝用例都验证源字节和目录未变化。缺列或未知版本拒绝；dbstat 不可用保留 NULL。JSON 无提示词、原成员/产物 ID、Key/PST 或认证材料。

## B 基线恢复闭包

以下列出取证位置和当前缺口，不能用它代替真实副本核对。

| 必须保留的事实                         | 旧来源与核对对象                                                                                                      | 后续责任                                                          |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| 稳定身份、当前凭据/会话、组/政策       | users、accounts、app_settings、member_sessions；业务事件与私有当前值分别取证                                          | #47 的唯一 owner；#50 同切点受保护快照；D6 仍需决定               |
| 三资源权益、预扣、终态、债务与未决核对 | sharing/account stream、fair_quota_settlements、jobs 的原收费/核对证据                                                | 原金额 owner 核对；#51 有来源 opening，不把未知实耗改为已知       |
| 观察、代次、游标、冷却、待到账         | 原账户/额度事实与运营资金收据，保留原时点和关联 ID                                                                    | #47/#50/#51；不得按迁移时间刷新                                   |
| 活动任务、排队/期限、批次原授权        | generation_requests、jobs、request stream、受保护输入/上下文                                                          | #31/#51/#52；缺失输入/完整 child 费用证据必须报告                 |
| 旧键、命令回执、退款、免费窗口         | generation_idempotency、event_commits、fair_quota_settlements、free_image_usage                                       | #50/#51 保留目标缺失的薄凭据；不能只复制 fingerprint 宣称精确重放 |
| 输入/附件/诊断、产物与原图             | generation_requests、generation_contexts、artifact_inputs、generation_diagnostics、images 与匹配 Local/R2/PikPak 清单 | #49/#51 核验原字节/归属/摘要；原图并未因业务备份成功而自动包含    |
| 卡密和官方直连历史                     | cards、旧 official 任务/产物及可证明归属                                                                              | #48 清点未兑责任/归属，维护者选具体去向；D4 退出不自动授权清零    |

若选 B，还需固定旧解释器及原 SQL/journal、完整原始备份、私有解密材料管理和可核验原图清单。归档恢复与新服务恢复各自独立验证；仅归档引用的对象不能进入日常七份轮换或孤儿清理。

## 能力保留与路线决策

| 能力                   | A 的目标                     | B 必须明确的取舍                                            |
| ---------------------- | ---------------------------- | ----------------------------------------------------------- |
| 历史列表/统计/损耗审计 | 在可证明的历史范围保留       | 哪些在线，哪些只在独立归档；缺历史不能显示完整零历史        |
| 旧请求查询/原回执      | 保留提交映射与原查询语义     | 原在线结果、归档查询与不可用状态的接口边界                  |
| 原图领取               | 保留真实归属及副本引用       | 归档原图的独立领取与引用保护，不能变成新服务的旧库 fallback |
| 输入复用               | 核验原语义、附件与首次上下文 | 在线范围外的输入是否仅归档，缺失时如何表达                  |
| 不可证明旧键           | 一律继续防重                 | 冲突/不可重放与原查询入口；不清空键空间                     |

当前已取得数据库副本并完成聚合盘点，建议接着以困难类别验证 A；仅一个合成终态类别不支持选择整条路线。匹配对象闭包、困难类别转换和同规模迁移/恢复资源测量仍缺，A/B 保持待决定，不同时实施两套转换器。待报告完成后集中决定：A 或 B、B 的逐接口历史范围、不可证明旧键行为、D4 未兑权益/历史产物去向及 D6 会话处理。D5 保留 Key 再次查看，D8 留存期限继续暂缓。

尚未测量生产副本的转换/恢复耗时、内存与文件缓存、临时磁盘、备份/归档/外部对象总量或查询性能；本记录不提供预计容量收益或维护窗口。真实端点、实际发布包恢复、HTTP/调度、完整私有恢复与切换仍分别属于 #49–#53 的验收。

## 本片验证与独立审查

Node 22.23.4 最终两文件 9/9、完整后端 1,001 项（996 通过、5 项既有条件跳过、0 失败）；隔离 Node 24.22.1 的相同脚本/测试 9/9、零跳过，使用既有 ABI137 依赖，未重编共享原生依赖。架构、后端类型、默认及实际 main `1f60907` 基线的旧迁移历史、129 文件语法、全库格式和差异检查通过。历史报告及 JSON 另外用同一固定 Prettier 显式检查；未调整 ignore 或 CI。

独立审查从畸形投影构造反例，发现直接输出资金列表 `.length` 会泄露非数组对象中的私有文本；修复在报告入口核对数组表示，原始攻击脚本现已拒绝且源字节/目录不变。两个同类反例（成员列表及待到账资金列表）在旧实现上都因错误接受而失败，当前通过。复审确认脚本 SHA-256 `0b633af2770d878adca0f0e3ca267f5e7a1805e0eac6ab46b64e891b45854546` 与本次线上聚合报告一致，本片无新增阻塞项。

主线合并后 [CI](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/43461555499) 已在 `1f60907` 成功；它证明基线检查通过，本片自身的同候选远端 CI 随独立 PR 更新。此次新增工具没有接入正常 HTTP/调度，也没有把工具自验当成新运行时的真实发布包恢复。
