# 独立执行服务（F11，实施中）

目标是让 Nuxt 网页/API 与任务执行拥有独立进程生命周期。执行服务独占 SQLite
写入、任务调度、上游连接、结果文件、会话和 Discord 令牌刷新；Nuxt 不另建
数据库缓存或调度器。断开 HTTP 等待不取消持久任务，结果仍可按成员权限领取。

## 已抽取的运行时

`server/gateway-runtime.js` 保留现有业务与文件布局，仅提供两个入口：

- `handleGatewayRequest(req, res)`：完整的原 Node handler，包含访问守卫、
  各协议错误响应及载荷预算的 finally 释放，不只导出内部 router。
- `startGateway(handler)`：初始化数据库，先恢复完整结果再清理中断任务，随后
  监听端口；监听成功才启动额度刷新、未知核对和队列派发，并安装统一关闭流程。

`server/index.js` 仍是兼容启动入口，调用同一运行时。导入运行时本身不会建库、
监听或启动后台任务；它只能由执行服务的进程入口启动，不能在 Nuxt 请求、SSR
或热更新插件中调用。旧 CLI 与 Nitro 是同一数据目录的替代启动方式，不可并行启动。

关闭流程先结束成员状态和任务预览订阅，随后允许在途任务持久化及响应完成，
最后关库。关闭订阅不发会话失效通知，不提前退款或取消等待任务。
相关修复和真实 Linux 信号验证见[结果恢复](result-recovery.md)。

抽取后 Windows 完整后端 476 项中 472 通过、4 项平台跳过；随后新增的导入
无副作用检查单独通过，未冒充已包含在前一轮全量中。Linux 镜像定向验证
导入、双 SSE 退出、在途 PNG/ZIP 完整交付与超宽限恢复，3/3 通过。
此时尚未经过 Nitro handler，因此不能据此宣布 Nitro 契约或 F11 已验收。

## Nitro 接入边界

采用当前 Nuxt 已锁定的 Nitro 2.13.4、H3 1.15.11 与 Node 22.23.3。
使用官方 `node-listener` 与 `fromNodeMiddleware` 接入原始 req/res，保留小型
Node 进程入口复用上述恢复、监听和关闭顺序；不使用 Nitro Tasks 代替持久队列。

选择 listener 是因为该版本默认 node-server 的 close hook 在 HTTP 排空后调用，
不能用它作为首次停止派发的时机；默认插件调用也不等待异步数据库恢复。
版本依据为已安装产物及官方发布提交
[`039b841`](https://github.com/nitrojs/nitro/blob/039b841669cd0641e54b443cba875b6552b24e05/src/presets/node/runtime/node-listener.ts)，
不能套用当前 Nitro 3 网站示例推断该版本生命周期。

业务代码暂不打平为 Nitro chunk：SQLite 读 worker、Drizzle SQL/journal 与
public 资源依赖真实相对文件路径。构建产物须保留文件树与 SQL 精确字节，原生
SQLite 依赖按目标平台安装；不能将 Windows 的二进制直接部署到 bob。

## 尚未完成的验收

运行时抽取及关闭修复不等于 F11 完成。内部入口密钥校验、实际 Nitro 构建与
产物中的 worker/迁移验证仍在实施，随后还须通过真实 Nuxt 与执行器双进程验证：

1. 只强杀 Nuxt，上游生成继续、结果落盘；Nuxt 恢复后可查询及领取。
2. 内部受理回执丢失后，以同标识重试不新增任务、预扣或上游调用；成员作用域不串用。
3. 单独强杀执行器，完整产物恢复为原结果，重复启动不重复结算或重发。

N01–N03 再按查询、认证管理、生图与长连接的顺序迁移网页 API；内部 SSE
重连必须让仍在线的浏览器重新校准。最终仍需独立 systemd 部署和 D01 隔离验收。
当前生产未切换，本文不作为部署完成或任意故障都能找回上游结果的声明。
