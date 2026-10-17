# 调用热力图（U03）

热力图展示当前保留的任务记录，供成员查看本人活动，供 owner／私有管理员查看
总体及指定成员活动。数据库仍由现有网关单写，Nuxt 负责展示与授权后的查询。

## 统计口径

- 窗口为最近一个日历年，包含今天，共 365 或 366 个台北日期；去年同日为
  2 月 29 日而不存在时夹到 2 月 28 日，再从次日起算。
- 按任务 `createdAt` 的 **Asia/Taipei 提交日**分桶，不按完成时间或图片日期。
  仅接受带 `T`、秒及 `Z`／显式 `±HH:mm` 的有效 ISO 日期时间，小数秒可选；
  非法日期、无时区值和纯日期不猜测归属。查询时间边界为 UTC 左闭右开。
- 每个 child 任务计一次；一次四张计四次，尚待激活的 child 也已受理。
  免费和缓存任务均计入；同一任务的幂等回放不增加计数，因此不是实际 NAI 请求次数。
- 默认 `generate` 包含普通生图、`img2img` 和 `infill`；`upscale` 与
  `encode-vibe` 分别查看，不混入默认生图。
- `done`、`failed`、`cancelled`、`queued`、`pending`、`running`、`other`
  互斥且合计为 `total`。取消使用 failed 任务的 `cancelledAt` 标记，不能仅凭
  HTTP 409 或错误文本判断；排队与等待前序分开。迟到图片保留原失败分类。

旧版本曾裁剪历史，现有手动日志清理也可删除任务。缺日表示无保留记录或覆盖
未知，不能断言零次；已有正数也不保证该日完整。响应固定标记
`coverage: "retained-records"`。最早任务日期、数据库迁移时间均不是可靠的
全量留存起点；不会用父请求或结算凭据猜测已删除任务的数量与状态。

## 接口与权限

| 入口 | 权限与成员范围 |
| --- | --- |
| `GET /api/member/heatmap` | 沿用成员 Cookie 和资格检查，固定本人；任何 `memberId` 参数返回 400 |
| `GET /api/member/owner/member-management?view=heatmap` | 沿用 owner Cookie 权限，默认总体，可指定历史成员 ID |
| `GET /api/admin/member-management?view=heatmap` | 沿用私有管理入口和管理员 Token 权限，范围同管理统计 |

查询参数 `operation` 默认为 `generate`，另支持 `upscale`、`encode-vibe`。
`dates` 为逗号分隔的台北 `YYYY-MM-DD` 日期，必须位于当前窗口，最多 366 天；
不传时查询整个窗口，传入时用于增量校准。重复参数、非法操作或日期返回 400。
管理入口允许 `memberId` 查询已经离开当前成员列表的历史归属，不借此开放成员权限。

DTO 包含 `timezone`、`from`、`to`、`asOf`、`memberId`、`operation`、
`coverage`、`days`。其中 `from/to` 为窗口首尾台北日期，`asOf` 为查询时间；
`days` 是稀疏数组，每项为：

```json
{"date":"2026-10-16","total":4,"done":1,"failed":0,"cancelled":1,"queued":1,"pending":1,"running":0,"other":0}
```

所有计数为整数。没有保留记录的日期不补造零行；响应不包含提示词、Key、原任务或上游私密数据。

## 查询与更新

`store.readActivityDays` 等待串行写入并 flush 后，调用现有 SQLite 只读 worker。
worker 直接 SQL 按日聚合全部持久历史，覆盖运行时缓存之外的记录，不热载全年任务。
成员归属依次采用父请求的稳定 `member_id`、旧任务 attempt 成员 ID、结算凭据
成员 ID，最后才匹配当前用户 token；无法确认归属的任务只计入管理总体。

查询复用 `idx_jobs_created_stats`：日期文本先粗筛两侧各两日，再通过复用严格
日期校验的 SQLite UDF 精确比较毫秒，避免合法时区偏移被文本排序漏掉。
指定 `dates` 时粗筛范围缩到所选日期首尾。没有新增 schema、汇总表或全年汇总缓存。

提交成功且缓存更新完成后，store 的 `activityChanges` 保留 `{memberId, date}`
配对并去重。新建、删除或统计投影变化才产生日期；只改图片引用、错误详情或
`updatedAt` 不触发热力图重算。回滚不发送通知，延迟写待 flush 成功才发送。
R01 合并通知时保留配对，按权限过滤后只向浏览器发送近一年内的 `activityDates`；
未知归属只供 owner 总览，不把全体成员 ID 与全部日期做交叉广播。

前端合并日期刷新，单次查询期间的后续变化补读；普通资料更新不重算全年。
首次加载、resync／断连恢复、台北跨日及回到可见页面时校准窗口，避免遗漏离线变化。

## 展示与兼容

复用 Naive UI `NHeatmap`，按浏览器本地日历坐标映射 API 的日期标签，避免
浏览器时区让格子错位；统计时区始终是台北。除悬停提示外，提供可搜索的
`NSelect` 日期选择和文字明细，触屏与键盘无需依赖悬停。

Naive UI 2.45.3 的小补丁修正稀疏数据的月份位置，短月份保留列宽并省略标签。
补丁同时覆盖 ES module／CommonJS，通过 pnpm `patchedDependencies` 安装；
`.gitattributes` 固定补丁 LF，锁文件记录精确字节 hash，避免 Windows／Linux
安装不一致。许可见 `web/THIRD_PARTY_NOTICES.md`；上游无需补丁即可通过月份回归时移除。

## 当前验证记录

- Nuxt SSR 会将页面动态依赖写成 HTML `prefetch`，匿名页面即使未挂载热力图仍会
  提前下载其资源。现通过 `build:manifest` 关闭这类预取，保留 `preload` 与动态导入。
  重建后的 `/`、`/owner`、`/admin` 均无 prefetch；首页 9 个 modulepreload 与修复前完全一致。
  Chrome 匿名冷缓存传输从 242,755 B 降至 184,387 B；就绪时间仍有波动，不能把全部延迟归因于预取。
- 存储相关定向 36 项通过；新增 HTTP 2 项及既有 HTTP 16 项通过，分别记录，未合并冒充全量。
- 前端 80 项、typecheck 和两项 Docker 检查通过；最终 CI 与首屏矩阵待补。
- 首轮后端全量 474 项中 470 通过、1 项旧 SSE“五字段”断言失败、3 项 Windows 跳过。
  该断言已适配新增日期字段，随后 16 项事件回归通过；最终全量结果待主验收补充。
- 本机 12,000 条冷历史、365 日聚合约 459 ms（含 worker 启动），重启结果一致，
  热缓存保持 2 条，EXPLAIN 确认现有日期索引参与范围查询。这不是生产性能承诺。

真实 Chrome 合成验收已确认：洛杉矶浏览器本地10月16日仍显示台北10月17日，
最后一天的格子与 tooltip 日期一致；缺失的1月1日明确显示未知，放大空历史
不补成零。owner 全局当天22条可切换到指定成员7条；390px视口无整页横向
溢出，图内可横滚，日期选择与文字明细可用于触屏。

实际提交两条测试任务，一条模拟V4.5免费成功、另一条排队后取消；成员当天
自动从总计5/成功4/失败1/取消0更新为7/5/1/1。网络只新增两次
`dates=2026-10-17` 的日桶查询，没有重读全年。模拟上游生成1次，新增外联0；
没有真实NAI/Discord调用。截图与请求记录保留在私有测试目录，不含真实凭据。

U03 尚待最终性能和 CI 验收收口；本阶段未替换或操作生产服务。
