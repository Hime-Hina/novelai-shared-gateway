# 开发与验证

先读 [文档导航](docs/README.md)、[模块归属](server/README.md)和所改接口的契约。业务规则在已有 owner 中修改；查询不提交钱包，reducer 只消费已保存事实。保持单执行器、单共享 Opus、SQLite 的当前边界。

## 本地命令

使用 `.node-version` 的 Node 和根 `package.json` 固定的 pnpm：

```sh
corepack pnpm install --frozen-lockfile
pnpm run format:check
pnpm run architecture:check
pnpm run migration:check
pnpm run typecheck
pnpm run check
pnpm test
pnpm --filter @novelai-gateway/web typecheck
pnpm --filter @novelai-gateway/web test
pnpm --filter @novelai-gateway/executor typecheck
pnpm --filter @novelai-gateway/executor test
pnpm --filter @novelai-gateway/executor build
pnpm --filter @novelai-gateway/web test:executor
```

先执行有区分力的受影响用例，再做完整检查。后端测试文件默认并发为 4；不要通过延长等待、删断言或放宽检查来掩盖失败。Node 24 的读取 worker 兼容检查也进入 CI。发布包、Docker、恢复与迁移门禁继续使用现有 workflow；本地某项通过不代表远端或生产已经通过。

## 格式与结构

全 workspace 共用根 Prettier 精确版本与 `.prettierrc.json`。`pnpm run format` 写入格式，`format:check` 只检查；`check` 仍是 JS/MJS 语法检查。编辑器使用项目本地 Prettier 和根配置，VS Code 配置只是可选入口。不要使用全局版本或第二套冲突的缩进规则。

- 每行最多一条有副作用的语句；相关步骤之间留空行。
- `if`、`else` 和循环使用显式大括号；`else if` 保持可读的连续分支。
- 不使用嵌套三元；在原 owner 中展开多层决定，保留求值顺序和短路行为。
- 100 列是排版目标，不强行拆 URL、字符串或正则。保留 Markdown 段落换行和 Vue 模板的空白语义。

Prettier 不检查大括号、嵌套三元或业务保证，这些由结构 review 与已有行为测试验证。纯机械格式提交与行为修复、控制流展开分开，避免把金额、事件、请求指纹或测试期望的变化藏在排版中。

`.prettierignore` 保留迁移 SQL/元数据、历史证据、精确字节 fixtures、独立打包的外部 MessagePack 测试解码器、patch、生成的 notices 和 lockfile 的工具格式；不按“legacy”排除核心源码。锁文件只由包管理器修改，历史 migration 只允许追加，不能改旧字节。

## 注释与交付证据

注释优先解释中文业务约束：事实源、事务归属、先后顺序、重试/回放、异常和恢复边界。金额说明单位与舍入；结果说明发布与结算的崩溃窗口；异步 UI 说明旧响应失效和草稿保护。类型已表达的参数、简单赋值和 getter 无需逐行翻译。不要把尚未修复的缺陷写成已成立的保证。

每次改动说明受影响契约、规则 owner、修复前能失败的反例、实际运行命令和结果，以及旧历史兼容策略。变更事件/reducer/金额/政策必须链接对应契约与回放验证；不要求局部改动新建一套设计文档。

架构守卫、可信 migration 基线、关键行为断言与 CI 的修改须有明确需求和维护者复核。仓库中的类型和检查用于防止正常误用，不是开发 agent 的权限沙箱；远端 required checks、规则修改和绕过权由仓库维护者管理，未做真实阻断演练不能宣称强制合并门禁已完成。
