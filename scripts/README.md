# 初始化与测试脚本

首次启动前在项目根目录运行 `pnpm run bootstrap`。先使用 `pnpm install --frozen-lockfile` 安装锁定依赖；只使用新数据目录，不会覆盖现有数据库，详见仓库根目录 README.md。

`pnpm run build` 将后端源码编译到 `.output/backend/`，再运行 `package-backend.mjs` 写入使用 JavaScript 入口的发布包元数据；Docker 和执行器复用这一文件。源码的 `pnpm start`/`dev` 使用 Node 原生 TypeScript 支持，发布产物仍直接运行编译后的 JavaScript。

离线重放使用 `node scripts/replay-events.mjs --source=/private/backup/library.sqlite --target=/private/rebuild/library.sqlite [--max-commits=100] [--through=COMMIT]`，参数使用等号。源库只读，目标须为新的隔离库或同一历史的续跑目标；游标停在完整提交边界，完成后重复运行不新增业务效果。CLI 不生成图片、不请求上游、不复制本地/R2 产物；必须同时保留完整输入/上下文、inline 产物、私有诊断、认证和实际文件/R2 清单。版本、opening、退休与恢复限制见[结果恢复](../docs/architecture/result-recovery.md)。发布包工具仍位于 `executor/.output/server/node_modules/novelai-shared-gateway/scripts/`，已有 `package-runtime.mjs` 复制编译脚本及完整 SQL/journal，CI 对解包结果运行离线重放检查。

隔离测试上游使用 `node scripts/mock-novelai.mjs --port 19081 --delay-ms 1000`。只绑定回环地址，接受固定合成凭据，返回固定图片与余额，不访问真实 NAI。支持范围、凭据和独立 systemd 配置见 [测试部署说明](../docs/operations/test-deployment.md#常驻合成上游)；不能用这些响应证明官方计费或生成质量。

离线测试副本使用 `node scripts/create-test-copy.mjs --source /private/snapshot/library.sqlite --target /private/test-copy --images-dir /private/snapshot/images`。目标必须不存在，其父目录须为受保护目录；图片目录可省略。工具只读一致性备份源库，轮换凭据，隔离活动任务和预留，并脱敏受保护的请求、首次规范化上下文、inline 产物/诊断及冷幂等/结算记录；受保护摘要先校验，脱敏后重新生成摘要和对应引用。不启动服务或访问上游。成员在副本中停止接单，上游凭据仅匹配本地合成服务，原图和业务输入仍属于须保护的私人数据。

测试副本是一段独立历史。工具仅在新建的私有副本中移除旧事件、提交、投影、导入/重放标记，按脱敏并终止任务后的事实建立新的 opening baseline，经影子核对后启用事件权威。新 historyId 与原 historyId、原提交游标及原事件历史 SHA-256 摘要写入 `app_meta.event_test_copy_provenance` 和命令摘要；不会篡改源事件、沿用旧事件哈希或把被隔离任务重新排队。数据库会重新压实并扫描原凭据残留字节；未知表仍拒绝交付，失败仅留下受保护的工作副本。支持范围、失败处理与隔离前置见 [离线测试副本](../docs/operations/test-deployment.md#离线测试副本工具)。

旧幂等键保留到原请求/任务的归属，并以独立域摘要改为隔离阻断记录；提交旧键会得到 409，不能借删除映射重新创建原任务。历史状态与已脱敏输入仍可读取，复用参数开始测试必须使用新键。原密码、成员 Key、会话与上游凭据均不可用于副本。
