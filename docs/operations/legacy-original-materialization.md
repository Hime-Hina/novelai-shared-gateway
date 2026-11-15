# B 迁移的选定原图准备

`materialize-legacy-originals.mjs` 为明确选定的恢复输入准备本地原图；`migrate-legacy-database.mjs` 仍是唯一、纯离线的 B 转换入口。领取成功不证明旧库曾拥有 R2 承诺，也不安装 ready。生产读取范围与成本须先获授权，生产迁移、删除和切换另行授权。

## 来源与不变量

选择清单是私有 JSON，格式 `novelai-legacy-rescued-originals-v1`，顶层字段均必填：`sourceSha256`、`sourceCommit`、`cut`、`auditSha256`、`cutConfigurationSha256`、`namespace`、`namespaceReceipt`、`originals`。`namespaceReceipt` 是 `{file, sha256}`，必须与同一个原 cut-config 的 namespace receipt 摘要相同，且实际文件摘要匹配；namespace 必须等于其 `r2://accountId/bucket`。不得覆盖原 cut-config 或用当前环境推定历史 namespace。

`originals` 每项均包含：

- `artifactId`、`memberId`、`inputId`：原实体及本次 source plan 确定的稳定归属。B 再核对实际计划，不采纳任意调用者 Map，同 SHA 的不同实体仍分别复制。
- `sha256`、`byteLength` 和 `sourceArtifactRowPointer: {file, fileSha256, line, lineSha256}`：绑定实际审计出口的原行；line 从 1 起，行摘要包含终止 LF。选择清单与出口均按文件一次索引，不逐图重扫全出口。
- `file`：已取得的本地普通文件，或尚待领取的 `null`。相对路径以选择清单目录为准。已取样本沿同一 MIME/长度/SHA owner 复核后复用。
- `r2`：明确选定的 `{bucket, key}`，或仅有本地来源时为 `null`。需要领取时必须存在；它是本次精确 allowlist，不是旧承诺。对象 SHA/长度复用该项原描述符。
- `downloadReceipt`：既有私有领取记录的 `{file, sha256}` 或 `null`；存在时必须能实际核对文件。记录不能代替内容校验。

所有输出必须是独占管理的新私有目录。文件申请 `0600`、目录申请 `0700`；Windows 须预先限制父目录 ACL，Node mode 不能代替 ACL。不要在运行中替换输入、目录或 namespace 证据。

## 准备与接入

独立迁移发布包包含两个入口，均使用 `--name=value`，参数不得重复：

```sh
node scripts/materialize-legacy-originals.mjs --selection=/private/selection.json --selection-sha256=<SHA256> --cut-config=/private/original-cut-config.json --cut-config-sha256=<SHA256> --output=/private/new-rescued-inputs
```

只从当前进程的 `R2_ACCOUNT_ID`、`R2_BUCKET`、`R2_ACCESS_KEY_ID`、`R2_SECRET_ACCESS_KEY` 交原 `r2Config()`；凭据不写清单、receipt 或 stdout。现有本地项不发请求；其余项串行调用原 `R2Storage.getFile`，使用默认 fetch、原 30 秒超时、redirect:error、WX 流写、长度/SHA 与 fsync。没有 HEAD/LIST/PUT/DELETE、Range、替代 transport 或重试。

每成功一项，以 WX 写入并 fsync 独立 receipt。首错即停止，保留已成功的文件与记录；正常收束或首错时均生成新的 `rescued-originals.private.json`，只含已成功子集。失败项或未列出的 BIN 不构成输入证明；零成功清单不能用于迁移。全新输出要求意味着本工具没有原地 resume 或覆盖机制。stdout 只给成功计数、字节数和新清单摘要；错误只给固定代码与已成功数，不打印 ID、key、签名、URL 或认证。

保留原十个必填参数，仅在同一个 B CLI 末尾增加以下可选参数对；只给一个必须拒绝：

```text
--rescued-originals=/private/new-rescued-inputs/rescued-originals.private.json
--rescued-originals-sha256=<该文件SHA256>
```

B 仍完整核对原 source/audit/出口，核对新增清单与原 cut-config，再在稳定 input/member 计划完成后物化。原 cut-config 内的单图 `localOriginal` 继续有效，不因新清单替换或改写；重复选择同一实体拒绝。物化沿原 WX canonical copy → 实际 common reader → 当前真实 `verifiedAt` → exact8 tuple/proofRef → 新 `migration-inputs.private.json` 摘要 → `ImportSourceRegistered`。复制与清单同步后才开新目标 SQLite，失败不留下伪 verified proof。原 publication、retiredAt 和归档审计不变；不从旧 tombstone 在线复活。后续仍由唯一 installer 验证实际字节并安装 ready。

专项在 `tests/integration/migrate-legacy-database.test.mjs`，覆盖实体/来源错配、错误字节、同 SHA 双实体、公共回放、当前旧单图入口及 mock 原 getFile 的串行首错保留；mock 结果不是实际 R2 端点验收。退休与 SQL 尾错边界继续由 `migrate-legacy-artifacts.test.mjs` 的真实 owner 反例验证。发布包入口清单由原 `package-migration-tools.mjs` 和部署专项维护。
