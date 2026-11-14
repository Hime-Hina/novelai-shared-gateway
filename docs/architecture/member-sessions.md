# 持久网页登录会话（A01）

T3 新格式接入继续复用 `members/authentication.ts` 的规则。存储通过 `memberSessionTransaction()`
提供当前成员、会话和 owner 配置的窄事务视图；旧运行时仅在存储边界映射原 SQL 字段。
新格式将凭据、会话和 `member_auth_state` 作为私有当前状态恢复，业务事实不能复活这些认证材料。
真实登录/退出共用原 HTTP handler；T4 的受理回调要求同一真实 SQLite 事务，不接受旧串行队列替代。
Discord 的邀请、登录和刷新继续由 `members/discord-auth.ts` 决定，经 `discordAuthenticationTransaction()`
保存私有绑定/授权与非秘密资格事实。网络验证在 SQL 事务外完成，受理事务按同一五分钟规则复核当前资格。
本段不改变下面已发布接口的期限、Cookie、Discord 或撤销规则；逐阶段实现与验证见
[数据库计划第 9.6 节](../plans/database-model-redesign.md#96-t3t4稳定身份私有凭据和会话事务)。

关系成员注册和状态命令接受当前已认证 owner 的稳定成员及认证版本，复用
`assertCurrentOwnerCommand()`，在同一 Key/Cookie 事务内、原回执之前复核当前
owner 配置、启用状态及数据库中的认证版本；元数据不能自证身份。Discord 当前
资格和 Cookie/Key 仍由原认证事务 owner 校验，传入成员快照不是独立的 HTTP 授权。
owner 不得停用本人；本机 admin 和内部 system 的既有应急入口保留。停用的私有
撤销与业务事实同笔提交，重新启用、重启及重领原回执均不恢复旧 Cookie 或旧认证版本。
公共提交回放只恢复稳定身份与状态，不恢复私有凭据，也不重做在线授权或网络调用。
这一存储接缝不表示管理 POST 已接入；范围和反例见数据库计划第 9.65 节。

owner 连续 1 天未访问后过期，普通成员连续 7 天未访问后过期。有效会话的
可见网页访问将截止时间重设为当前时间加各自期限，不在旧截止上累加天数。
页面显示服务器返回的当前截止；服务器每次鉴权重新判断有效期，Cookie 本身不是权限证明。

Nuxt 与保留的旧网页只在可见时，为个人 `me` 或 owner 的管理摘要请求附加
`X-Member-Activity: 1`。现有可见页面 30 秒补读也算访问；隐藏后停止补读。
服务端仅在这两个 GET 入口完成同源检查、会话鉴权和 Discord 成员验证后续期。
管理摘要特指精确 `/api/member/owner/member-management`，且 URL 无任何 query；
字面 `/api/member/owner/summary`、静态 `/api/admin/member-management`、普通 GET，
以及带 query 的 heatmap/losses 等读取都不属于此续期入口。续期继续复用原会话 owner，
不自行计算期限；Discord 和服务资源 IO 后仍复核当前会话、owner/epoch 与 ready 才返回管理数据。
普通 API 查询、客户端生图、SSE 心跳、后台标签页和 Discord token 刷新不续期。
不使用鼠标轨迹判断活动，也没有另加心跳请求或浏览器存储中的登录令牌。

## 保存、签发与撤销

新增 0010 的 member_sessions 是独立冷表，不加入热业务集合、成员 DTO
或普通导出。字段为 token_hash、user_id、auth_version、created_at、
expires_at、owner_binding；两个时间是 Unix 毫秒，索引用于按用户撤销和
期限查询。浏览器收到 32 字节随机令牌，数据库只保存 SHA-256 摘要。

签发事务重新核对已提交的成员状态、认证版本及 owner 绑定，防止密码或
OAuth 校验完成后成员已被停用/换绑的旧副本继续获得会话。期限从该串行
事务实际执行时计算；插入、过期/旧认证版本会话清理和 owner 配置协调
同一事务提交，失败不发成功 Cookie。保留最多 10000 个有效会话的原限制，
满额返回 503，不用认证缓存数量限制正常生图。

会话鉴权核对当前启用状态、认证版本、签发角色/绑定及当前到期时间。
退出先完成 SQLite 删除，再清 Cookie；重复退出幂等。密码重置、停用、
删除、Discord 重新登录和确认不再具备成员资格继续使用原认证版本撤销
规则。停止接单不撤销会话，退出网页也不撤销成员 API Key。

owner 会话绑定内部成员 ID、Discord ID 与 guild。已有 app_meta 保存上次
owner 配置，启动和鉴权协调配置变化：撤销前任/新任 owner 及旧 owner
会话，保留其他普通成员会话。A→B→A 不恢复旧 Cookie；普通 7 天会话不
能因成员刚升为 owner 而直接获得管理权限，必须重新认证并签发 1 天
会话。数据库失败时不能先放行未完成撤销的旧权限。

Cookie 继续为 HttpOnly、SameSite=Strict、Path=/api/member，公网 Secure；
Max-Age 和 Expires 对应服务器截止。写请求仍须本站 Origin，只有 OAuth
GET callback 使用原一次性 state/浏览器 Cookie/Host 保护。不扩大 Cookie
范围，不把登录令牌或上游凭据放进 localStorage。

续期复用现有表和串行写事务，仅 UPDATE expires_at，保留原始 created_at、认证版本
与 owner 绑定；不会插入或复活已删除会话。事务内重新检查到期、权限及撤销状态，
写入成功后才更新 Cookie 的 Max-Age/Expires。时钟回拨不缩短已有期限；续期不广播
业务变更，避免“续期→推送→查询→再续期”的循环。没有新增 schema 或数据库迁移。
旧的未过期 owner 会话在新页面首次访问后可延长为一天；已过期的会话仍须重新登录。

## 页面与兼容边界

`/api/member/login` 成功增加 expiresAt，`/api/member/me` 增加
sessionExpiresAt；均为整数毫秒，不返回令牌/摘要。到期或撤销时页面关闭
状态连接、清空已展示 API Key、回登录入口；迟到的额度/密钥响应或旧会话
401 不能覆盖新登录页面。SSE 已连接时发送 auth-expired；初次连接收到
401 而无法收到 SSE 事件时，仅对永久关闭的连接补一次鉴权检查。

Discord 临时故障或普通断网不会伪装成会话过期，503 不清登录视图。现有
Discord 五分钟成员资格缓存、token 刷新去重、同源限制和 owner 权限检查
保持；A01 当时先在原入口实现，后续 N02 已接入 Nuxt，执行器仍是唯一会话写入方。

旧内存 Map 从未持久化，单靠浏览器 Cookie 无法证明身份，因此默认首次升级需
重新登录。2026-10-19 正式切换仅对当前 owner 做了[一次性验证桥接](../operations/production-release.md#当前-owner-会话沿用)：
停止旧服务前由旧服务验证原 Cookie，身份与版本不变且凭据未过期才离线导入摘要，
不签发新令牌、不放宽权限；该 owner 已在正式页面沿用并正常续期。其他旧会话
仍需登录，新版有效持久会话在重启后保留。OAuth 尚未完成的一次性登录
流程仍是原机制，重启期间未完成的授权需重新发起，不宣称所有临时认证
状态都已持久化。不要把新旧运行版本同时接到同一共享数据库。

## 验证记录（2026-10-17）

Windows 完整回归 430 项：427 通过，3 项 Linux 专属关闭/目录同步测试跳过；
101 个文件语法检查通过。Linux 会话、权限、持久化与页面定向 40/40 通过，
容器内 11 条迁移、WAL/FULL、完整性检查、冷任务读取与 HTTP 启动通过。
镜像 `novelai-shared-gateway:a01-check` 的 ID 为
`sha256:71c7df94dd5d41e2247f65907f49a23aef94f7b838cf96c7ad45b0d579edc216`。

真实 HTTP 覆盖固定期限、强杀重启、退出及 SSE 撤销、数据库签发失败不发
Cookie、认证版本变化和 owner A→B→A；单元覆盖迟到响应不能覆盖新登录。
Chrome 的隔离合成成员页在 390px 宽度显示七天截止且无横向溢出；展示 API
Key 后推进测试时钟，收到服务器 auth-expired 即回登录并清除 Key。
合成上游生图调用为 0，浏览器测试实例已关闭。

[托管 CI 38984338010](https://github.com/Hime-Hina/novelai-shared-gateway/actions/runs/38984338010)
在 536dd58 通过 430/430、101 文件语法及 Docker 构建。此前两次既有
Native 多图 HTTP 超时仍未解释，此轮通过不等于修复该问题。未使用真实
NAI、未迁移生产数据库或替换生产服务。
