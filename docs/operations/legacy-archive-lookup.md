# 独立旧归档的精确查询

本入口属于 [固定旧工具包](../plans/database-model-redesign.md#918-t7-前置固定旧格式的独立只读工具包)，只解释该包固定 `3caba3d` owner 支持的旧 v4 封存源。正常网关不调用它，不启动 HTTP、调度或网络，不选择 A/B 路线，不转换新模型或安装 ready。原始 SQLite 继续保留有效权益、未决责任、旧键依据和审计来源；可查询不代表可删源。

先核发布 tar 的外部 SHA-256，再解包并运行 `--verify-package`。运行平台、Node 22.23.4、ABI 和逐文件摘要必须匹配；包内原 server、SQL/journal 和 lock 来自固定 Git 对象。使用 [既有封存要求](../plans/database-model-redesign.md#918-t7-前置固定旧格式的独立只读工具包)：只接受无 WAL/SHM/journal 的 DELETE 模式一致性单文件，不对原源调用初始化、迁移、checkpoint 或升级。需要整理 WAL 时另建工作副本，保留原备份。

每次精确查询都复用完整的 schema、连续提交、clock/core 切点、影子行和保护摘要审计。`--source-sha256` 是此前已选封存来源的完整文件摘要，精确查询必须提供；匹配的 ID 不能代替匹配的历史。查询中及关闭后仍核源摘要和无 sidecar。整库审计本身的耗时仍在，不把本入口视为在线分页服务。

```sh
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --verify-package
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite --source-sha256=SELECTED_SOURCE_SHA256 --job=ORIGINAL_JOB_ID
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite --source-sha256=SELECTED_SOURCE_SHA256 --request=ORIGINAL_INPUT_REF
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite --source-sha256=SELECTED_SOURCE_SHA256 --command=ORIGINAL_COMMAND_ID
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite --source-sha256=SELECTED_SOURCE_SHA256 --artifact=ORIGINAL_ARTIFACT_ID
```

四个 selector 互斥，也不与批量 `--export` 混用。未知 ID、格式或必要来源、错误摘要、坏关系和不支持的输出均非零退出且 stdout 为空；错误消息不包含源内容。默认 stdout 只有完整审计统计和 `lookup` 的静态白名单：

- `job` 读取原 job 行的身份、状态、时间、诊断相关号和原保存排序。`originalOrder` 保留 SQLite 的十进制文本，不按当前插入顺序或时间重排；`streamLastCommitSeq/streamLastEventId` 明确是该流最后来源，不冒充首次受理时刻。成员取保存流中的稳定事实，不按当前 Key 反查；缺值保持 NULL。`inputRef` 若存在，须指向原 Input 及其原 RequestAccepted/Opening 来源。
- `request` 核原输入引用、稳定成员、保存字节数和原事实 SHA，返回这些元数据及原 commit/event/index，不输出正文、提示词、附件或规范化 context。缺首次完整定义不会补当前默认，也不据此宣称可执行恢复。
- `command` 复用原 `readCommandReceipt()`，返回原完整提交序号、时间、事件数及原 receipt 字节 SHA/长度。它不是从当前 job 或新 import command 重造的回执。
- `artifact` 复用原清单映射，保留原 Local 文件声明、R2 bucket/key 和 PikPak 目录/上传声明及已保存 SHA/长度。原稳定成员取保存 artifact 流；不猜当前账户或部署 namespace。未明确实读时 `localContentVerified=false`，云端始终 `externalBytesChecked=false`。

只有显式 `--output` 才写原保护内容，目标必须是已存在私有目录下的全新普通文件；不能覆盖，也不能写进源库目录、选定原图目录或工具包。所有原文、JSONL 和 audit.json 共用创建 owner：Windows 普通 drive/UNC 根保留，根后任意 NTFS stream 冒号、保留设备名及非法组件在创建前拒绝；`wx` 后继续核对普通文件、单链接及 named path/fd 身份。批量导出目录也使用同一预检，不经另一发射路径绕过。先设置该目录的本机私有 ACL；POSIX 文件模式使用 0600。正文和回执不进入 stdout，返回的 output 元数据仅包含写入字节数与 SHA。

```sh
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite --source-sha256=SELECTED_SOURCE_SHA256 --request=ORIGINAL_INPUT_REF --output=/private/new-original-input.json
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite --source-sha256=SELECTED_SOURCE_SHA256 --command=ORIGINAL_COMMAND_ID --output=/private/new-original-receipt.json
node /verified/legacy-tools/scripts/audit-legacy-archive.mjs --source=/sealed/library.sqlite --source-sha256=SELECTED_SOURCE_SHA256 --artifact=ORIGINAL_ARTIFACT_ID --data-dir=/sealed/original-files --output=/private/new-original-image.webp
```

输入文件保留原 JSON 字节，原提示词/附件只进入该明确私有文件；认证角色字段被拒绝，不静默删字段后称为原件。回执原文仅支持闭合的生成回执字段 `id/requestId/jobIds/count/cacheHit` 并继续由原公开事实 owner 拒绝认证内容；其他回执可查原 SHA/提交审计依据，原文导出明确拒绝，完整来源仍留原 SQLite。

`--data-dir` 只用于 artifact，按原显式 file 在该物理根目录内用 `r` 句柄读取；拒绝越界、软链接/junction 逃逸、文件身份变化、缺基线 SHA/长度、缺原件和坏内容。打开前后及读完后均核选定物理根的目录身份和路径归属，并同时核 named path/fd；同内容不能替代原 namespace。复用固定 owner 的纯 `describeStoredResult()` 核内容；不调用会 `r+`、sync 或 rename 的发布 owner。已核原字节时才将 `localContentVerified` 设为 true，省略 `--output` 也可仅验证。不会 GET R2/PikPak 或尝试兼容定位后重发布，单张本地图通过不代表全量外部存储恢复闭包。R2/PikPak 描述符必须是真正对象，缺字段才保留 NULL；primitive、array 或 false 不解释成空对象。

## 2026-11-14 局部验收

初始 `7197b7b` 的 Windows x64 / Node 22.23.4 tar SHA-256 为 `1d80e85bbc35ed288c37ebc32b5896f97eea5d9e28cb93d687edaa1e59b387c7`，解包后 4,027 项清单验证通过；入口 SHA-256 为 `6736a5d317dd55a7eb54337f7b43df7bb1395f29fd2d86ecb4e1c3939637ca10`。该候选随后独审确认三个 P2，不能因这些初始检查接收。原固定 server/SQL/journal 未改，未安装或重建 native。初始打包专项 19/19，新增 lookup 断言与原包边界反例同次运行，包含原请求/回执原字节、无认证内容的 metadata、必需历史摘要、未知 ID/格式、禁止覆盖/写源、保护输入的 Discord grant 拒绝、祖先 junction 逃逸、坏图，以及源 DB/sidecar/原图前后不变。19 是该文件的实际测试总数，不表示 19 个全新 lookup 用例。

对既有获准封存源 `47ea07f440d7652c11ebced52c17883ad821e12411d4dbd046bcc1ed319f4821`（605,024,256 B），从上述实际解包目录各执行一次 job/request/command/artifact 精确查询。四次均通过原 16,509 提交、77,865 事件、cut 16,509 的完整审计，耗时分别 22.625/25.843/27.032/22.187 秒；实际网络及 listen 阻断计数均零。原保护输入 4,108 B、原生成回执 117 B 只写入显式私有文件，未输出提示词、原 ID 或秘密到本轮公开证据。源目录四个文件（DB、gzip、描述符和 snapshot）的 SHA、长度、mtime 前后均相同，无 sidecar；输入/原回执仍由同一 DB 摘要覆盖。新建证据目录和文件已核只允许当前用户、System、Administrators。

本次真实 artifact 只验证来源及位置声明，`localContentVerified=false`、`externalBytesChecked=false`；封存描述符的 operation/MIME 未知仍保留 NULL，未补当前默认或重新抓取原图。真实本地图读取仅有独立包合成控制，不写成真实旧图或云端端点验收。可复现选择和命令 runner、原始 stdout/stderr、阻断计数、保护输出及前后清单保存在私有 `database-model-20261114/archive-lookup-20261114`，安全聚合核对见该处 `checkpoint.json`。本片未执行 full、CI 大改、迁移、ready 或切换；独立审查须绑定实际候选及该 tar，不以这些局部证据替代全量归档原图、同规模容量或生产恢复验收。

独审原红均来自上述真实解包 CLI：外部 hardlink 的 `:stream` 输出可给原 source 建 129 B ADS 并改变 mtime；合法旧 owner 封存的 R2 primitive 被报告成七字段全 NULL；realpath 通过后置换祖先 junction，可读取 root 外同字节并虚称本地已核。原脚本、正确 file-URL/Node 22 的红日志与早期未进入 CLI 的加载错误分别保留在 `H:\TEMP\novelai-archive-lookup-independent-20261114`，不把加载错误当产品反例。三项修复归上述共同 owner，不修改业务事实或放宽信任边界；新增全部 output/export 角色的 ADS/设备名、硬链接源 mtime/stream 不变、错误 R2/PikPak 容器及合法对象对照、同字节祖先置换。修复后的作者实际 pack/unpack 专项仍 19/19，176.011 秒；新固定源码、实际新 tar 与独立复审结论继续绑定登记，旧 `7197b7b` 保持 NOT PASS。

固定修复 `621b512bee7bee666ee7a9e53f7f0fca1188e5b0` 的新 tar SHA-256 为 `9339b655d4164d3140d5784d0fd16d8f70b4e7f953f1635c9944612db8cf5faf`，实际解包 4,027 项验证通过，入口 SHA-256 `fb6ef6ad8afe52189bc8e4e6955a0cf34a0ed10456fca9a2ee0f4ed368f8ddb2` 与 fixed 源逐字相同。类型、架构、默认与可信 `88d2c26` 旧迁移历史、改动格式和语法检查通过，未执行 full、CI 改动或 native 安装/重建。

该新实际解包目录在同一获准封存源分别执行 job/request/command/artifact，28.172/26.781/28.000/24.516 秒；四次均核 16,509 提交/77,865 事件/cut16,509，网络及 listen 阻断计数均0。保护输入4,108 B及原生成receipt117 B只写新私有文件，源目录四文件 SHA/长度/mtime 前后全同且无 sidecar；图仍 `localContentVerified=false/externalBytesChecked=false`，operation/MIME NULL 不补猜。作者证据在私有 `database-model-20261114/archive-lookup-fixed-621b512/checkpoint.json`；本轮写前已设置/核新目录 ACL，写后21项无额外Allow。最初子WindowsPowerShell5文件策略预检失败发生在CLI/源打开前，未改或绕策略；实际在既有授权父shell完成只读ACL检查再运行取证runner，该设置失败不计为产品反例或成功证据。

root 未参与实现，对固定621补丁的 lookup、privacy、read-only owner及四份源 SHA/Gitblob作独立审阅，并单独核实际保留新tar、CLI及 `--verify-package` 的4,027项。随后在同一固定树独立执行 `node --test --test-concurrency=1 tests/deployment/legacy-tools.test.mjs`：19/19、零跳过，195735.6279毫秒，实际pack/unpack及已入库的三个回归和原包边界均通过。Scoped技术PASS见 `H:\TEMP\novelai-archive-lookup-independent-20261114\INDEPENDENT-REVIEW-621-primary.md`，日志为 `H:\TEMP\novelai-archive-lookup-621-primary-independent-package.log`。该结论属于root本轮独立审查；此前子审查被平台自动审查阻断的记录保留，不称子审查已完成，也不称root重跑了原standalone脚本。真实封存四次查询仍为上述单独作者证据，root未读取私有payload或访问实际存储。未验证真实云端、Linux/POSIX特有路径、同规模恢复容量、全后端或最终在线装配；该PASS不授权合并或生产迁移/删除/切换。
