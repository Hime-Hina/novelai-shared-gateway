# 持久请求与排队生命周期（F06-1 / F06-2 / F06-3 / F06-4）

当前已接入网页 `POST /api/jobs`、Native 普通/stream 多图和辅助操作、
direct `/generate`、OpenAI JSON/SSE。各入口均使用持久父参数与统一调度，
已受理任务独立于 HTTP 连接。父请求查询、结果领取和取消未派发任务已经
接入；可选幂等仍留下一片，不能将当前版本视为 F06 全部完成。

## 受理与载荷

生图受理时，在同一个同步 SQLite 事务中写入不可变
`generation_requests` 父记录、所有 `jobs` 子记录和本次账本变化。任一 SQL
写入失败，父记录、任务和账本一起回滚。父记录保存稳定成员 ID、来源、
操作、提交时间、完整规范化参数及 UTF-8 字节数，不保存 API Key 或原始 URL。

父表不加入 JsonStore 的通用集合。历史参数留在 SQLite；热 job 只保留
requestId、模型、尺寸、步数、种子等小字段，不复制 tag、完整提示词、
负面提示词或输入图片。执行时按 requestId 冷读，按 sampleIndex 浅构造
单张种子和 `n_samples: 1`，不为每张深拷贝 base64。单任务回包和错误详情
按需冷读取完整提示词，不截断原 API 字段；错误种子对应实际失败子任务。
Native、direct 和 OpenAI 受理函数返回后结束完整正文/解析参数的作用域，
同步 handler 只持有任务 ID、回包模型等元数据和已完成的输出图片。SSE
进度只读取 child 摘要，不为反复展示队列状态冷读完整提示词；普通任务查询
仍保留完整 prompt 契约。

网页和 OpenAI 正文上限保持 **2 MiB**，Native wire 上限保持 **32 MiB**。规范化请求
默认上限 **32 MiB**（`REQUEST_MAX_PAYLOAD_BYTES`），全局活动载荷默认
**64 MiB**（`QUEUE_MAX_PAYLOAD_BYTES`）。按含 base64 的 JSON UTF-8 字节
计量，不按解码图片大小或 JavaScript 字符数。两个配置均可下调验证。

路由先规范化路径，再在读正文前创建临时占用；direct 的查询串也先计临时
字节，规范化后只存执行参数，不把含 Key 的完整 URL 写入父记录。每个正文 chunk 在保留前
检查预算；multipart 展开附件前按预期 base64 字节检查，最后按规范化 JSON
重新校准。提交成功后，在下一项串行受理开始前把 claim 移交为持久字节，
携带 claim 的更新不会延迟写入；解析失败、读流断线和写入失败由 HTTP
finally 释放临时占用。没有通用提交回调机制。

持久计量从小型活动 jobs 索引取去重 requestId，再按父表主键读字节数，
不扫描历史父记录，也不读取 payload。queued/pending/running 均占预算；
同一父批次只计一次。旧无 requestId 的活动记录继续使用内联参数，启动时
仅为这些活动任务回填 payloadBytes，后续新 inline 任务在受理事务内计量。
不会为了兼容旧记录热载所有历史参数。

执行控制表中的未结束尝试也计入字节数，并按 requestId/jobId 与 SQL 去重。
即使 timeout/cleanup 已把任务标 failed，只要运行控制尚未 finish，仍占
载荷和上游并发槽；执行 finally 完成后释放。重启时旧进程不再持有输入，
在途任务按现有未知消费/退款证据处理，不重新派发原尝试。终态释放预算，
父参数和已保存结果继续保留；显式全库替换才原子移除无子任务引用的父记录。

32/64 MiB 初值来自 Windows 合成实验，不是 bob 的内存峰值保证。图片解码、
解析过程中的中间对象和输出缓冲仍不能由这份输入字节预算推导出完整内存上界。

## 批次、队列与连接

Native 一次受理全部 1–4 张意图：首张 queued，其余 pending。全部子任务
原子计入成员 4 / 全局 16 上限；pending 不预扣、不占上游槽、不领取队列
序号。当前入口固定使用公平额度模式，Native 在实际派发时才预扣。

首张成功保存图片、结算和激活下一张在同一事务中提交。下一张领取新
queueSeq 加入队尾，因此 A1 运行期间 B 已入队时，顺序为 A1 → B → A2。
激活沿用整批最初提交的 queueExpiresAt，不重新获得十分钟等待期限。
失败、未知结果、过期或停止接单会在同一事务终止未派发的 siblings；迟到
结果只找回已失败任务的图片，不复活剩余批次。停止接单不取消在途执行，
恢复接单也不复活原意图。

pending 同时进入活动 SQL 索引、启动加载、容量、过期清理、日志保留及
成员停用/份额调整保护。暂停接单仅持久化受影响成员、取消任务、退款账本
和设置；即使热缓存只加载部分历史，也不会尝试全量覆盖 jobs。公共共享
状态的 queued 仍表示已进入派发队列，新增 pending 表示待前图完成的意图；
成员近期活动的等待数包括两者。pending 没有虚构的排队位置。

客户端断线仅移除 HTTP 等待者，不取消已受理任务。同步 HTTP 等待超时也
不修改任务：direct 保留超时图片，OpenAI JSON 返回 504，SSE 保留超时文字
与 DONE 结束标记。响应发送失败不会把已完成任务改成失败或触发退款；缓存
产物缺失仅在受理前回落到重新生成，已受理结果发送失败不再次入队。SSE
close 会清除自己的轮询定时器和等待者，不改变任务。

新的 direct/OpenAI 不再把 HTTP 等待截止写入任务 deadlineAt，队列仍使用
独立业务等待期限，上游执行仍保留原执行超时。各来源尚未派发的 queued/pending
在重启后继续执行，旧 inline direct/OpenAI 也按已保存的业务期限判断；
已发送但结果不明的旧 running 按现有故障规则结案并终止剩余意图。旧版本
从未持久化的后续图片无法凭空恢复。原同步 ZIP、MessagePack final/error
帧、部分成功清单与辅助二进制响应保持；当前没有新增客户端幂等要求。

## 持久排队序号

实际新入队均分配独立 queueSeq；缓存命中直接完成的记录不占号。任务影子
列与 `app_meta.queue_sequence` 最高号在同一事务提交，写失败一起回滚，
最高号不随历史清理或重启归零。

旧活任务首次启动按原调度实际使用的已加载数组过滤后 reverse 顺序编号，
保留历史 order_value，包括并列排序时原活动索引枚举的影响。此后派发
直接读取 queued 部分索引并按 queue_seq 排序，公开排队进度使用同一序号。

## 查询、领取与取消

Native、direct、OpenAI 在受理后通过已有 CORS 暴露的 `X-Nai-Request-Id`
返回父 ID，成功响应体保持原格式，超时/部分失败响应也保留该 ID。网页
job 回包和成员 `me.jobs[]` 增加 requestId；旧 inline 任务为 null，继续
使用原 job 查询接口，不为历史任务批量补造父记录。

Bearer 入口 `GET /api/requests/:id` 和成员 Cookie 入口
`GET /api/member/requests/:id` 复用同一业务查询。父归属按不可变 member_id
判断，因此本人换 Key 后仍可找回；他人成员或未知 ID 返回 404。查询仅
返回来源、操作、时间、派生状态/计数及小型 child 信息，不读取父 payload，
也不把冷历史重新热载。已有 request_id/status 索引满足单父查询，无新增索引。

每张图的 imageUrl 都是既有签名链接；签发前必须确认 image.token 与历史
child.userToken 一致，不能只因父请求属于本人就签发任意关联图片。失败任务
的迟到完整图片也可领取。旧 Key 与当前成员 Key 不同不影响父 ID 归属判断。

两种鉴权路径都提供 `POST .../:id/cancel`。Cookie 请求沿用同源 Origin/CSRF
检查；成员页通过原生详情展开查询，显示结果及“取消未派发任务”。取消与
派发使用同一串行存储事务，仅将 queued/pending 置 failed 并标记 cancelled，
沿用幂等退款；running 继续。任务、退款账本和设置一起提交，失败回滚且不
提前通知等待者。重复取消返回 cancelledCount: 0；取消后的余图不会被随后
成功或迟到结果重新激活。父状态由子任务派生，不增加可漂移的父状态副本。

查询返回 counts.failed 包含取消项，child.cancelled 单独区分取消。父状态
按 running/queued/pending 优先；全成功为 done，部分成功为 partial，全取消
为 cancelled，其余失败为 failed，无保留子记录为 empty。cancellable 仅由
是否仍有 queued/pending 决定。取消回包复用同一 DTO，额外返回本次取消数。

## 验证边界

已有测试覆盖父/子/账本写失败回滚、临时/持久字节交接、活动索引 EXPLAIN、
大参数不入热子记录、旧活动 inline 字节回填、partial 历史下停止/恢复、
pending 重启加载、激活顺序/期限、物理在途字节/槽位、迟到结果不复活批次
和 multipart 扩张。真实 HTTP 使用独立临时数据与 localhost 合成上游，
检查原响应、批次公平性、部分成功、断线继续、完成激活后强杀和重启续跑。

F06-2 Linux 镜像使用 Node 22.23.3，在 UID 1000、断网、只读根目录加临时
可写数据卷下通过七次迁移、106 条任务冷缓存重开保留三条 pending、旧 inline
字节回填、父载荷去重、激活顺序/原期限、逻辑失败后的物理在途占用、SQLite
integrity 与真实 HTTP 启动。镜像为 `novelai-shared-gateway:f06-native-check`
（sha256:36a05264935e42d7fec2620dcc3a965bf44ab29054d4208f5de810346781b99e）。
没有请求真实 NAI。Linux 检查验证功能与权限，内存峰值证据仍仅来自 Windows
合成实验。

F06-3 的独立 localhost HTTP 检查使用 100 ms 同步等待，覆盖 direct 错误
图片、OpenAI JSON/SSE 超时后继续、三个连接形态的主动断线、全部已受理父
请求强杀后恢复、旧 inline 队列兼容、完整大提示词冷存及 direct 缓存命中。
小型回归检查 SSE close 清理定时器/等待者、进度不解析父 payload，以及
结果发送失败保留任务终态和结算。

本片 Linux 镜像 `novelai-shared-gateway:f06-connection-check`
（sha256:58a67867cf44c011fa806fe42a41557b57ca2ac209b10af4f7e72d324d6ae294）
在 UID 1000、断网、只读根目录下再次通过七迁移、冷 pending、载荷预算与
HTTP 启动检查。完整连接行为由上述独立 localhost 合成 HTTP 用例验证。

F06-4 回归覆盖真实 SQLite 冷查询、稳定成员归属、图片所有权、取消 SQL
失败整体回滚/零通知、重复取消、部分历史持久化，以及实际 reserveQueuedJob
和 completeGeneration 与取消的双向竞争。独立 HTTP 检查覆盖两种鉴权的
同 DTO、Cookie CSRF、签名图片领取、跨成员拒绝和新入口请求 ID；没有真实
NAI 请求。

F06-4 全套 328/328、84 文件语法通过；Chrome 合成成员页完成批次取消、
重复读取与真实签名图片领取，390px 视口详情宽 322px、无详情横向溢出。
测试期间上游生图调用为零。Linux 镜像 `novelai-shared-gateway:f06-request-check`
（sha256:cab02058050f4c77a35475068d9a5a450613641df604ebee98b355078ea1b64b）
在 UID 1000、断网、只读根目录下通过七迁移、冷 pending、预算、SQLite
integrity 和真实 HTTP 启动检查。未替换生产服务。
