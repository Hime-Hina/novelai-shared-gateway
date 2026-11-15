# B 迁移的选定原图准备

`materialize-legacy-originals.mjs` 为明确选定的恢复输入准备本地原图；`migrate-legacy-database.mjs` 仍是唯一、纯离线的 B 转换入口。领取成功不证明旧库曾拥有 R2 承诺，也不安装 ready。生产读取范围与成本须先获授权，生产迁移、删除和切换另行授权。

## 来源与不变量

选择清单是私有 JSON，格式 `novelai-legacy-rescued-originals-v1`，顶层字段均必填：`sourceSha256`、`sourceCommit`、`cut`、`auditSha256`、`cutConfigurationSha256`、`namespace`、`namespaceReceipt`、`originals`。`namespaceReceipt` 是 `{file, sha256}`，必须与同一个原 cut-config 的 namespace receipt 摘要相同，且实际文件摘要匹配；namespace 必须等于其 `r2://accountId/bucket` 或非 NULL 的 `pikpakUrl`，需要远端领取的项只使用这一处来源。已有本地项的取得来源保留在各自 receipt，不由该 namespace 追认。读取类别由同一校验入口临时推导，不写入清单；未知来源不得从当前环境补全。不得覆盖原 cut-config 或用当前环境推定历史 namespace。

`originals` 每项均包含：

- `artifactId`、`memberId`、`inputId`：原实体及本次 source plan 确定的稳定归属。B 再核对实际计划，不采纳任意调用者 Map，同 SHA 的不同实体仍分别复制。
- `sha256`、`byteLength` 和 `sourceArtifactRowPointer: {file, fileSha256, line, lineSha256}`：绑定实际审计出口的原行；line 从 1 起，行摘要包含终止 LF。选择清单与出口均按文件一次索引，不逐图重扫全出口。
- `file`：已取得的本地普通文件，或尚待领取的 `null`。相对路径以选择清单目录为准。已取样本沿同一 MIME/长度/SHA owner 复核后复用。
- `r2`：明确选定的逻辑 archive 描述符 `{bucket, key}`，或仅有本地来源时为 `null`。需要领取时必须存在；R2 沿其 bucket/key，Pik 原 owner 沿其合法逻辑 key 及原 SHA 派生选定 namespace 下的 `SHA.br`。它是本次精确 allowlist，不是旧承诺；不注入声称历史上传的 `pikpak` 属性。对象 SHA/长度复用该项原描述符。
- `downloadReceipt`：既有私有领取记录的 `{file, sha256}` 或 `null`；存在时必须能实际核对文件。记录不能代替内容校验。

所有输出必须是独占管理的新私有目录。文件申请 `0600`、目录申请 `0700`；Windows 须预先限制父目录 ACL，Node mode 不能代替 ACL。不要在运行中替换输入、目录或 namespace 证据。

## 准备与接入

独立迁移发布包包含两个入口，均使用 `--name=value`，参数不得重复：

```sh
node scripts/materialize-legacy-originals.mjs --selection=/private/selection.json --selection-sha256=<SHA256> --cut-config=/private/original-cut-config.json --cut-config-sha256=<SHA256> --output=/private/new-rescued-inputs
```

需要 R2 领取时只从当前进程的 `R2_ACCOUNT_ID`、`R2_BUCKET`、`R2_ACCESS_KEY_ID`、`R2_SECRET_ACCESS_KEY` 交原 `r2Config()`；Pik 则只将 `PIKPAK_WEBDAV_URL`、`PIKPAK_WEBDAV_USERNAME`、`PIKPAK_WEBDAV_PASSWORD` 交原 `pikpakConfig()`。两者都须与选定 namespace 相等，凭据不写清单、receipt 或 stdout。全为现有本地项时不读取这两种配置、不发请求。需要领取的项串行复用一个原存储实例，首错后不切换来源。

R2 保持原 `R2Storage.getFile` 默认 fetch、30 秒、redirect:error 与 WX 流写。Pik 调用原 `PikPakStorage.get`/WebDAV 默认 transport、原 30 秒和 Brotli/SHA/长度校验，完整 Buffer 交现 WX/fsync owner，随后与所有本地复用项共同核 MIME/长度/SHA。Pik 默认 SDK 可跟随重定向，一次逻辑调用不证明物理请求数；原压缩累计阈值 65 MiB 在整 chunk 到达后检查，非硬 wire 或 RSS 上限。原图 Buffer 受原描述符长度约束，处理后不保留在成功清单内。没有 HEAD/LIST/PUT/DELETE、Range、HTTP/Response 包装、替代 transport 或重试。

每成功一项，以 WX 写入并 fsync 独立 receipt。首错即停止，保留已成功的文件与记录；正常收束或首错时均生成新的 `rescued-originals.private.json`，只含已成功子集。失败项或未列出的 BIN 不构成输入证明；零成功清单不能用于迁移。全新输出要求意味着本工具没有原地 resume 或覆盖机制。stdout 只给成功计数、字节数和新清单摘要；错误只给固定代码与已成功数，不打印 ID、key、签名、URL 或认证。

receipt 的 method 区分 `R2Storage.getFile`、`PikPakStorage.get` 和 `selected-local-reuse`；历史保留字段 `isOriginalR2Promise:false` 不授予任何新远端承诺。原 Pik 声明仍由原归档/事实保留，当前同名候选只在真实取得并校验后成为本次新的 Local 恢复输入，不补原上传/发布时间或历史 verified。跨主机取回文件时新建指向实际本地文件/receipt SHA 的选择清单并走既有 local-reuse，不直接将另一主机的绝对路径当成本地输入；原 source/config 和稳定 member/input 关系不得改变。合并已取 R2/Pik 文件时必须全为本地项并逐项保留实际下载 receipt，不能重写原 receipt 的 namespace 或冒称所有文件来自同一远端。

保留原十个必填参数，仅在同一个 B CLI 末尾增加以下可选参数对；只给一个必须拒绝：

```text
--rescued-originals=/private/new-rescued-inputs/rescued-originals.private.json
--rescued-originals-sha256=<该文件SHA256>
```

B 仍完整核对原 source/audit/出口，核对新增清单与原 cut-config，再在稳定 input/member 计划完成后物化。原 cut-config 内的单图 `localOriginal` 继续有效，不因新清单替换或改写；重复选择同一实体拒绝。物化沿原 WX canonical copy → 实际 common reader → 当前真实 `verifiedAt` → exact8 tuple/proofRef → 新 `migration-inputs.private.json` 摘要 → `ImportSourceRegistered`。复制与清单同步后才开新目标 SQLite，失败不留下伪 verified proof。原 publication、retiredAt 和归档审计不变；不从旧 tombstone 在线复活。后续仍由唯一 installer 验证实际字节并安装 ready。

专项在 `tests/integration/migrate-legacy-database.test.mjs`，覆盖实体/来源错配、错误字节、同 SHA 双实体、公共回放、当前旧单图入口及 mock 原 getFile 的串行首错保留；Pik 沿原 decoder 的 mock body 核错 namespace/坏压缩内容/独立 MIME 校验、串行首错前缀、本地零 IO 和 B Local intake。实际包测试的 mock 绑定 entry-relative 编译 owner；mock 结果不是实际 R2/Pik 端点验收。退休与 SQL 尾错边界继续由 `migrate-legacy-artifacts.test.mjs` 的真实 owner 反例验证。发布包入口清单由原 `package-migration-tools.mjs` 和部署专项维护。
