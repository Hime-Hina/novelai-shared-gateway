# 真实 B 转换与联合恢复证据

本记录绑定 `6a3e192c716dbc3a8e9bfa148c5a77fa3bb28088` 的实际独立 Windows 迁移包及同一迁移结果。总体进度以 [#32](https://github.com/Hime-Hina/novelai-shared-gateway/issues/32) 顶部为准；本记录不宣称合法 ready、最终发行物、全部外存或生产切换通过。

## 固定输入和执行产物

| 对象         | 固定依据                                                                                                                                                                                      |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 原一致性库   | 旧发布 `3caba3dfa0b5d9f582acabfc02e36e7db4af038f`，cut `16509`，605,024,256 B，SHA `47ea07f440d7652c11ebced52c17883ad821e12411d4dbd046bcc1ed319f4821`                                         |
| 独立导出     | audit SHA `f16a8f4cef7654218201d20fca95db107fe8c1003e46a37a2824027e43f3c63d`，artifacts SHA `b5d7b3e4c75d56142a1a58fb1478219d408b7a039503f684f5325afcaf2b3d09`                                |
| 选定切点配置 | SHA `fa7276050a959a670e57bf4231215154e510bfbd5aca8dec3efaa53bec82c147`；原文件未修改                                                                                                          |
| 原图补充输入 | 229 份原 R2 实读/原字节校验及既有样本，346,781,252 B；manifest SHA `e9113f66f5aced44e5dd737c005b4d73a238078505ba6a4e38ffe767a2ec6d26`                                                         |
| 实际迁移包   | `novelai-migration-tools-6a3e192-win32-x64.tar.gz`，SHA `ebc37ed14cfada20885a2167d9202348cf8e1fc67a60ea714020aacd8406a423`；Git-only 编译、解包、manifest 校验及 no-strip 编译入口 46/46 通过 |
| 执行环境     | Node 22.23.4，pnpm 10.32.1，同版本已安装 SQLite native；未安装或重建共享 native；转换及联合恢复均禁止外部网络                                                                                 |

实际转换使用解包后的 `scripts/migrate-legacy-database.mjs`，传入原 source/SHA/commit/cut、audit/SHA、artifacts、全新 output、cut-config/SHA 与上述 rescued-originals/SHA。CLI 的 `building` 输出和退出码 2 是尚未安装 ready 的明确状态。

## 转换结果

来源核对为 `complete`，覆盖身份、实体集合、金额与责任、键和回执、输入、诊断及产物来源；不能仅由目标与自身 shadow 相等推导该结论。原源文件、sidecar 集合、Node 与 native 字节在执行前后不变。

| 结果                      |                                                                                实际值 |
| ------------------------- | ------------------------------------------------------------------------------------: |
| 成员 / 请求 / 任务 / 产物 |                                                             5 / 9,690 / 9,691 / 9,642 |
| 新完整提交 / 事件         |                                                                       10,173 / 91,381 |
| 收费 / 尝试 / 防重键      |                                                                    9,652 / 8,286 / 40 |
| 受保护输入 / 附件 / 诊断  |                                                                      18,520 / 80 / 10 |
| 目标数据库                | 311,853,056 B，SHA `858f19ce030d8a0580ea2a592ff07434e17e01f002cac7eb655f934305cac7a1` |
| 目标状态                  |                                          `building`，未调用 installer，未手工改 ready |

原 cut 与新提交数属于不同历史表示；B 保存有来源 opening，切点前逐时点审计继续由独立旧归档承接。报告仅剩 `REMAINING_PUBLIC_RECONCILIATION_AND_READY_PENDING` 待办；该旧标签仍保留，来源核对的独立字段已经是 `complete`，没有通过删除 finding 放行。

## 同一结果的联合恢复

使用同一实际包的公共 replayer、保护输入 owner 和选定私有快照 owner，从空库重建。恢复记录的 source SHA 为上述 `858f19ce…cac7a1`，history 与完整新 cut `10173` 一致。

- 第 7 笔提交的真实 `generation_inputs` AFTER INSERT 阶段注入原生 `SQLITE_CONSTRAINT_TRIGGER`。已到达 owner，外层捕获后全部业务表摘要不变，事实、回执、保护输入及游标一起回滚。
- 保留前 6 笔 checkpoint，关闭并重新打开数据库；续跑恢复其余 10,167 笔，原 6 笔识别为重复。
- 私有恢复前 27 张公共表相同且私有为空；选定快照恢复 21 行私有记录后，唯一 DDL 的全部 34 张表逐项一致，成员私有列另按原 owner 核对。
- 全部 10,173 笔再次回放及私有再次恢复均零新增，`total_changes()` 增量为 0，所有表和私有指纹不变。完整性、外键和最终重开通过，原 source SHA 不变。

这证明真实迁移结果的数据库联合恢复与断点/重复/回滚，不证明对象字节验证或唯一 installer 已完成。恢复目标保持 `building`，外部 IO 为 0。

## 容量和性能口径

| 测量                         | 实际值与边界                                                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| 转换耗时 / 进程 CPU          | 463.651 s / 446.703 s                                                                                               |
| 转换进程峰值工作集           | 1,550,675,968 B，Windows OS 进程指标；不是系统文件缓存                                                              |
| 联合恢复及完整重复耗时 / CPU | 562.015 s / 501.391 s，包含 SQL 故障、重开、全表核对和重复全历史                                                    |
| 联合恢复进程峰值工作集       | 280,072,192 B                                                                                                       |
| DB 页                        | 4,096 B × 76,136 页，freelist 0；不是 VACUUM 后释放量                                                               |
| 代表性受保护逻辑字节         | 输入正文 70,604,368 B；附件 66,707,364 B；事件 payload 51,687,965 B；提交回执 1,353,194 B。各列口径不相加推算节省量 |
| 转换全部输出                 | 233 文件，共 684,820,315 B，包含新库、原图副本、输入与核对报告                                                      |
| 临时空间                     | 5 s 采样曾读到 442,742,264 B，小于最终全部输出；它漏掉短阶段，不能作为峰值空间上限                                  |

新库小于原库属于该 B 结果的实际文件测量。旧完整归档仍必须保留；尚未完成同候选实际加密备份、独立归档、全外存、系统文件缓存和完整维护窗口的联合容量报告，不能只用两个数据库文件相减宣称总体节省。

原始 argv、程序、stdout/stderr、进程 wait、前后 SHA、逐文件清单、完整来源核对和联合恢复记录封存在本地受保护证据目录 `novelai-B-real-47ea-6a3e192-20261116`；不提交真实实体、提示词、图片或认证内容。最终验收仍按[离线恢复](../operations/relational-recovery.md)及[重构计划](../plans/database-model-redesign.md)完成真实对象、唯一安装、同候选 HTTP/调度与切换演练。

## 后继 Linux 包的两对象原图试验

本节单独绑定后继 `669fc14050a161a85f0539f23aa8b1bf884ebc3f` 的实际 Linux 迁移包，不改变上文固定 6a 的转换与联合恢复含义。[CI43795003961](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/43795003961) 的实际 checkout `fb48cbd2a917135568443b66b72b25ccf4f41755` 与 669 同 tree，编译迁移检查 53/53、零跳过后上传 [artifact14193879608](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/43795003961/artifacts/14193879608)。

| 固定对象或实测 | 依据 |
| --- | --- |
| 下载 ZIP | 9,786,431 B，SHA `2468fad937903dba46480a449689093cdaa0d22524ba4bfb1203408195f875cb`，匹配 API 和上传记录 |
| 包内 tar | 9,807,391 B，SHA `9925ffc7b86672eccd0524db16e411064ddd3dfe9ffde6d0dcbc8b48a0cb6717`；4,085 条 manifest 与实际文件/链接完整一致，Linux x64 / Node 22.23.4 / ABI 127 |
| 选定范围 | 两个不同的原对象：一个当前候选、一个原 Pik 声明；原成员、输入、导出行及含 LF 摘要绑定，原 fa727 配置和证明字节不改 |
| 实际读取 | 原服务主机身份和 13 个代码 pin 前后不变；实际编译 materializer 复用默认 Pik owner，顺序两次逻辑 get，均成功；没有重试、R2 回退或外部写入/删除 |
| 字节与耗时 | 原图合计 541,941 B；工具 12.742 s，外层 SSH 17.487 s，Node 自然退出 0；10 秒 RSS 样本 120,041,472 B，不是峰值 |
| 返包 | 563,200 B，SHA `354663f538227c73bf3c77770fdb312dca18c72e88f15640797a0378599b0fd1`；固定六个普通文件，无链接或路径逃逸 |
| 独立接收核验 | 两 PNG 的 MIME、全部 chunk CRC/IEND、SHA、长度、稳定归属、原导出行及 receipt 的 tool/selection/method 全部一致；新 Local 复用 manifest SHA `6f89e927e6ace75fbc5a1eaa756263bd74b771f2124d3cea9cee236d3001b6d6`，仅重绑传输路径，原 receipt 字节/SHA/方法保留 |

五个本地 SSH/SCP 进程均记录实际 PID、开始时间及同句柄 wait，正常退出；远端 Node 也实际 wait 到退出。外层 110 秒、16 KiB 输出及 4 MiB 返包上限由独占控制执行；正常、输出超限与超时三项自有子进程反例通过。私有控制、原 stdout/stderr、返包与复用输入分别封存在 `novelai-pik-2item-pilot-control-669-20261116` 和 `novelai-pik-pilot-root-supervisor-20261116`。

这两次实际云读取只证明所选字节及接收闭包。物理 HTTP 请求数、状态码和 wire 字节未单独观测，仍未知；其余原图、全外存容量、最终同候选 ready 与发布验收继续独立完成。原 229 份 R2 输入 e911 保持不变，本次没有生产迁移或切换。
