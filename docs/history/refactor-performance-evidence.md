# 事件重构性能验证记录

状态：2026-11-01，测量工具的本地小样本及冻结 Linux 镜像的有界诊断已通过；R12 十万历史正式测量和年度容量尚未开始。小样本、时间外推和容量外推都不能用于宣布性能验收通过。

验收范围及回归阈值沿用[当前重构计划 §11.2](../plans/architecture-refactor-plan.md#112-性能验收)。本记录保存运行方式、环境、原始结果位置及解释，不另设性能门槛。

## 正式运行口径

- 基线为 `deab4db`。候选必须从最终冻结提交提取到独立源码目录，使用同一 Node 22.23.4、相同已安装依赖和 SQLite 版本。
- 数据全部生成于专属 Linux volume，基线和候选都写入该 volume；不使用 Windows bind mount 承担 SQLite fsync。其他构建和测试停止后，串行运行测量子进程。
- 1,000 与 100,000 条旧历史分别测基线/候选各三轮，每轮 100 次完整成功生成、固定 8 个活动任务；覆盖 20 名成员、4 个分组及历史中的批次、成功/失败、生成/放大/Vibe 分布。
- 候选 100,000 历史的第一轮完成后，固定同一份已 checkpoint 数据库。三个独立子进程各从 opening 全重建一次，再在另一空库重建至半程、关闭连接、重新打开并续跑；保留完整提交游标、源库 SHA-256、投影 SHA-256、SQL 投影校验和 integrity 结果。
- 年度容量单独运行一次：525,600 次官方分钟观测及 36,500 个批次，批次大小循环为 `1/4/1/2`，共 73,000 张图。年度库再执行一次完整重建、一次关闭重开后的断点恢复和一次备份。年度结果明确标作单次容量样本。
- 财务源库与重建目标保持 `synchronous=FULL`。不模拟上游等待；逻辑时钟为生成耗时推进 2 秒，并遵守现有账号冷却后再派发下一张。

正式命令在候选源码根目录执行，路径由协调者冻结并记录：

```sh
node --expose-gc scripts/benchmark-refactor.mjs \
  --baseline /bench/baseline-deab4db \
  --candidate /bench/candidate \
  --output /bench/results/refactor-final \
  --history 100000 --rounds 3 --samples 100 --active 8 \
  --observations 525600 --annual-batches 36500 --owner-days 7
```

输出目录必须尚不存在。`report.json` 汇总源码摘要、版本、机器/文件系统、各轮分位数、内存、读取行数代理、重建、磁盘和年度预算；每个子进程另外保留配置、完整结果和日志。执行结束发现源码内容变化时，正式模式拒绝将结果作为固定候选对照。

正式候选提交、Linux 镜像、volume、命令及结果：待最终候选全量验证完成、获得空载窗口后补充。

## 计时和解释边界

受理通过实际 `/api/jobs` handler，分开记录最终 `JsonStore.update` 与含准入准备的整个受理耗时。候选派发和完成直接调用生产导出的 `reserveQueuedGeneration`、`completeGenerationCommand`。旧版对应命令是私有函数，适配器使用原公共领域函数及 `JsonStore.update` 实现相同成功路径；保留账号计数、冷却、产物描述、最终结算、任务/图片记录及下一子任务推进，不截取运行时源码。

派发、产物准备、最终结算各自提交。两版都调用真实 PNG 发布与 fsync，但文件发布时间置于 SQL 事务计时之外。年度容量通过公共领域函数、事件提交与真实 projector 保存同样的完整成功事实，逐项检查预留、派发、准备、发布、结算、完成、冷却和下一子任务事件数；仅保存合成 PNG 的描述与引用，不写年度图片字节，也不代表上游端到端延迟。

每轮使用新的子进程和 SQLite 连接，单独记录首次查询；操作系统页缓存未强制清空。读取行数为物化 SQL/DTO 行数及匹配源行数，`better-sqlite3` 未暴露物理页读取计数，不能将这些代理指标解释为磁盘 I/O。旧版没有事件重放功能，三轮重建数据仅比较候选的重复性和规模成本。

## 2026-11-01 工具小样本

命令：

```powershell
& 'C:\Users\HH\AppData\Roaming\fnm\node-versions\v22.23.4\installation\node.exe' --expose-gc scripts/benchmark-refactor.mjs `
  --baseline 'H:\TEMP\nai-refactor-acceptance-20261101-87cc125a90d04581b43070eb5d9df881\baseline-deab4db' `
  --candidate 'H:\WorkingSpace\novelai-shared-gateway' `
  --output 'H:\TEMP\nai-benchmark-command-replay-smoke-20261101-1' --smoke
```

该轮使用 Windows、Node 22.23.4、SQLite 3.53.1、i5-12500H，20/120 条历史，每版各一轮、3 次成功生成、固定 2 个活动任务。源工作区仍在实施，结果如实记录 `sourceChangedDuringRun=true`，仅证明测量流程能够完成，不能视为固定版本的性能对照。

固定 120 历史库的独立重建和断点恢复均完成，源库共 125 个任务、171 个事件、21 个提交。全重建约 308.1ms；断点前约 186.6ms，重新打开后续跑约 64.7ms。源/重建投影摘要一致，SQL 投影验证及 SQLite integrity 通过，重建目标为 FULL。

年度缩小样本包含 12 次观测、4 个批次、8 张图，产生 104 个新事件和 40 次业务提交：12 个观测提交、4 个受理提交，以及各 8 个派发、准备、完成提交。追加约 345.0ms；全重建约 314.6ms；断点重建两段合计约 359.7ms。数据库由 405,504 字节增长至 573,440 字节，约 1,615 字节/新增事件。原始文件位于上述输出目录，所有重放摘要、投影及 integrity 检查通过。

脚本最终小样本复跑位于 `H:\TEMP\nai-benchmark-command-replay-smoke-20261101-2\report.json`，总耗时约 10.4 秒，`sourceChangedDuringRun=false`，包含新增的环境一致性和重建汇总检查。另在 `H:\TEMP\nai-benchmark-command-refresh-smoke-20261101-1\candidate.json` 用同一模板执行 7 次候选成功生成，覆盖一次分钟额度刷新且活动集合仍为 2。两项仍是工具验证，未达到正式规模或三轮要求。

## 空载窗口与磁盘准备估计

按本次实际业务频率，完整年度预期约 2,197,300 个新增事件、781,100 次业务提交。由极小 Windows 样本按提交数直线外推，年度追加、全重建及断点重建合计约 5.3 小时；该估计包含明显平台和小样本不确定性，不能作为 Linux 耗时承诺。应先为完整任务准备约 4–8 小时级的专属窗口，再以实际 Linux 进度修正预期，不能据外推宣布验收完成。

同样的粗略容量外推为年度源库约 3.3GiB，源库、备份和两份重建库合计约 13.2GiB；旧历史模板、三轮负载库和六份十万历史重建目标另约 5GiB。建议专属 volume 至少保留 30GiB 可用空间，40GiB 更便于保留日志和规模偏差。正式容量预算必须改用完整年度结果中的实际数据库、WAL、索引和备份字节。

## 冻结 Linux 候选的有界诊断

诊断使用 `novelai-refactor-validation:performance-f781263`，镜像摘要为 `sha256:a6dc40e3145dd11ff6a8d81099025eb7b985e36a8b17603baf989a5e5918a44d`。基线位于 `/baseline`，候选 `f781263` 位于 `/app`。容器禁止网络、限制 2 CPU / 4GiB，SQLite 数据写入 `nai-refactor-performance-20261101-87cc125a` 原生 volume。没有修改镜像或业务源码。

首次 1,000 历史运行暴露脚本错误：摘要测量试图克隆已经只保留热集合的 `jobs`，存储层按设计拒绝。修正脚本改为调用两版实际 `/api/me/quota` GET，并将只读 runner 的 SHA-256 纳入结果；不扩热缓存、不改业务读取规则。

修正后的命令参数为 `--history 1000 --rounds 3 --samples 100 --active 8 --skip-annual`，运行约 57 秒，`sourceChangedDuringRun=false`。100 次样本显著降低了首次编译对尾延迟的影响；仍不是十万历史验收。三轮 P95 的中位数如下：

| 指标 | 基线 ms | 候选 ms | 差值 ms |
| --- | ---: | ---: | ---: |
| 受理最终事务 | 3.99 | 9.88 | +5.90 |
| 受理全过程 | 8.06 | 19.48 | +11.41 |
| 派发事务 | 3.83 | 12.46 | +8.63 |
| 产物准备事务 | 3.09 | 9.44 | +6.35 |
| 最终结算事务 | 3.56 | 12.74 | +9.18 |
| 成员摘要 | 0.59 | 1.09 | +0.50 |
| 首页历史 | 0.56 | 0.66 | +0.10 |
| 全年热力图 | 10.61 | 10.94 | +0.33 |
| Owner 统计 | 1.54 | 1.68 | +0.13 |

受理全过程在此规模触发原计划的联合阈值。其余上述指标没有同时越过两项条件。热 RSS 增加约 4.88%；三轮全重建中位约 3.00 秒，断点前约 1.76 秒、续跑约 1.26 秒。投影摘要及完整性检查均通过。

独立诊断进程随后以相同模板再执行两版各 100 次成功生成，并对候选做全量/断点重建。探针计时 SQLite statement 的准备与执行、COMMIT 原生调用边界，并保存 Node CPU profile；探针自身开销和启动成本包含其中，所以这些总耗时只用于归因，不替代上表的未插桩延迟。

| 插桩结果 | 基线负载 | 候选负载 | 候选两份重建 |
| --- | ---: | ---: | ---: |
| 进程墙钟 ms | 3,512 | 6,810 | 6,991 |
| SQL 准备 ms | 51 | 662 | 724 |
| SQL 执行 ms，包含 COMMIT | 1,144 | 1,889 | 2,905 |
| COMMIT 边界 ms | 956 | 1,275 | 2,193 |
| COMMIT 中进程 CPU ms | 188 | 240 | 373 |
| 用户投影 upsert 次数 | 0 | 10,140 | 40 |

两版业务写事务的 COMMIT 都是 507 次；另各有 102 次只读/初始化事务提交。因此“候选提交次数翻倍”不成立。候选每个写事务都包含共享流核对，`relatedStreams` 无条件加入 `sharing:default`，`applyEventProjection` 随后把 20 名未变更成员再次写入：`20 × 507 = 10,140`。产物准备这样只变更任务的命令也重复准备用户 upsert SQL。CPU profile 中 `applyEventProjection`、快照/投影规范化和 `canonicalJson` 也占用计算时间。

离线重建的 COMMIT 边界约占本轮总墙钟 31%，支持减少离线物理提交次数的优化方向；COMMIT 时间包含 SQLite 工作、文件系统同步和调度等待，不能直接称为 fsync 系统调用耗时。进程 CPU 包含工作线程，不能将墙钟减 SQL 时间直接等同于 JavaScript CPU。

交给存储负责人验证的最小优化建议：

1. 保留候选/事件投影一致性检查，在验证完成后跳过与原 SQL `data`、`order_value` 相同的 upsert 及无变化缓存刷新，避免仅因参与核对而重写全部成员。
2. 复用固定集合的预编译写语句，避免每条成员/任务投影重复编译同形 SQL；优先利用现有 statements 结构，不添加无界查询缓存。
3. 离线重放可将有界数量的完整逻辑 commit 放入一个外层 FULL 事务；保持各事件批次、回执和提交游标，异常整批回滚。生产命令仍逐次 FULL。此项本轮仅提出，尚未实现或验证收益。

原始证据在宿主目录 `H:\TEMP\nai-refactor-acceptance-20261101-87cc125a90d04581b43070eb5d9df881\performance-profile-f781263`，包括 `warm-report.json`、`profile-summary.json`、三组 `*-profile.json` / `*.cpuprofile` 及诊断探针。volume 内对应 `/bench/profile-f781263-warm-v2` 与 `/bench/profile-f781263-instrumented-v2`。runner 摘要为 `4d483dd9f04cdba190eb2b72d3a8937812bdf2b60f9328429a87925edeaaa3e7`，候选源码摘要为 `04f6e1de3e20488640ce199e760530c79ac398802f414d72ca35992534a8a75f`。窗口结束后容器已退出，没有启动年度任务。
