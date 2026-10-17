# 初始化与测试脚本

首次启动前在项目根目录运行 `pnpm run bootstrap`。先使用 `pnpm install --frozen-lockfile` 安装锁定依赖；只使用新数据目录，不会覆盖现有数据库，详见仓库根目录 README.md。

隔离测试上游使用 `node scripts/mock-novelai.mjs --port 19081 --delay-ms 1000`。只绑定回环地址，接受固定合成凭据，返回固定图片与余额，不访问真实 NAI。支持范围、凭据和独立 systemd 配置见 [测试部署说明](../docs/test-deployment.md#常驻合成上游)；不能用这些响应证明官方计费或生成质量。

离线测试副本使用 `node scripts/create-test-copy.mjs --source /private/snapshot/library.sqlite --target /private/test-copy --images-dir /private/snapshot/images`。目标必须不存在，其父目录须为受保护目录；图片目录可省略。工具只读备份源库，轮换凭据、隔离活动任务和预留，输出新的数据库与私有测试凭据；不启动服务或修改源库。支持范围、失败处理与隔离前置见 [离线测试副本](../docs/test-deployment.md#离线测试副本工具)。
