<script setup lang="ts">
import { computed, h, onBeforeUnmount, ref } from 'vue'
import { NAlert, NButton, NCard, NInput, NInputNumber, NModal, NSelect, NDataTable, NDropdown, NTag, type DataTableColumns } from 'naive-ui'
import type { ManagementApi, ManagementGroup, ManagementMemberChange, ManagementMode, ManagementResource, ManagementRun, ManagementState, ManagementUser } from '../types/management'
import { date, number } from '../utils/member-display'

const props = defineProps<{ state: ManagementState; mode: ManagementMode; api: ManagementApi; run: ManagementRun; busy: boolean; groupsDirty: boolean }>()
const emit = defineEmits<{ changed: [message: string]; notice: [message: string, error?: boolean] }>()
type MemberIdentity = Pick<ManagementUser, 'id' | 'username'>
type Dialog = 'credentials' | 'delivery' | 'confirm' | 'transfer' | 'delete'
const search = ref(''), filterGroup = ref('')
const dialog = ref<Dialog | null>(null)
let active = true, dialogVersion = 0, deleteReadVersion = 0
const dialogIds: Record<Dialog, string> = { credentials: 'memberDialog', delivery: 'deliveryDialog', confirm: 'confirmDialog', transfer: 'transferDialog', delete: 'deleteDialog' }
const percent = (value?: number) => value == null ? '—' : number(value * 100) + '%'
const name = (user: MemberIdentity) => user.username || user.id
const groupOf = (id: string) => props.state.groups.find(group => group.members.includes(id))
const protectedOwner = (user: MemberIdentity) => props.mode === 'owner' && user.id === props.state.ownerUserId
const copyUser = (user: ManagementUser) => ({ ...user, quota: user.quota ? { ...user.quota } : null })
const users = computed(() => props.state.users.filter(user =>
  (user.username || user.note || user.id).toLowerCase().includes(search.value.trim().toLowerCase())
  && (!filterGroup.value || (filterGroup.value === 'unassigned' ? !groupOf(user.id) : groupOf(user.id)?.id === filterGroup.value))))
const groupFilters = computed(() => [{ label: '全部分组', value: '' }, { label: '未分组', value: 'unassigned' }, ...props.state.groups.map(group => ({ label: group.name, value: group.id }))])
function requireClean() { if (props.groupsDirty) throw new Error('分组有未保存修改，请先预览保存或撤销修改。') }
function current(version: number, kind: Dialog) { return active && dialogVersion === version && dialog.value === kind }
const manage = <T>(body: unknown) => props.api<T>('member-management', body)

const editingId = ref(''), username = ref(''), password = ref(''), showPassword = ref(false), createGroup = ref('')
const credentialDiscord = ref(false), createGroups = ref<{ label: string; value: string; disabled?: boolean }[]>([])
const deliveryText = ref(''), deliveryLoading = ref(false)
const confirmation = ref<{ title: string; description: string; body: { action: 'member-status' | 'member-admission'; userId: string; enabled?: boolean; acceptingJobs?: boolean } } | null>(null)
const transferUsers = ref<ManagementUser[]>([]), transferFrom = ref(''), transferTo = ref(''), transferResource = ref<ManagementResource>('v5'), transferAmount = ref<number | null>(null)
let transferConfigVersion = 0
const resourceOptions = [{ label: 'V5 余量（百分点）', value: 'v5' }, { label: '订阅 Anlas', value: 'anlasFixed' }, { label: '购买 Anlas', value: 'anlasPurchased' }]
const resourceNames = { v5: 'V5 余量', anlasFixed: '订阅 Anlas', anlasPurchased: '购买 Anlas' }
const transferOptions = computed(() => transferUsers.value.map(user => ({ label: name(user), value: user.id })))
const transferHint = computed(() => {
  const from = transferUsers.value.find(user => user.id === transferFrom.value), to = transferUsers.value.find(user => user.id === transferTo.value)
  const key = `${transferResource.value}Available` as const, before = from?.quota?.[key], after = to?.quota?.[key], amount = transferAmount.value || 0
  return `${from ? name(from) : '转出者'}：${number(before)} → ${number(before == null ? null : before - amount)}；${to ? name(to) : '接收者'}：${number(after)} → ${number(after == null ? null : after + amount)}${transferResource.value === 'v5' ? ' / 容量 ' + number(to?.quota?.v5Capacity) : ''}。总余额保持不变；有排队或运行任务时暂不可操作。`
})
const deleteUser = ref<ManagementUser | null>(null), deleteUsers = ref<ManagementUser[]>([]), deleteRecipient = ref(''), deleteConfirmName = ref('')
let deleteConfigVersion = 0
const deleteGroups = ref<ManagementGroup[]>([])
const deletePreview = ref<{ result: ManagementMemberChange; payload: { action: 'delete-member'; userId: string; transferToUserId: string; expectedConfigVersion: number } } | null>(null)
const deleteOptions = computed(() => deleteUsers.value.filter(user => user.id !== deleteUser.value?.id && (user.quota?.member || (deleteUser.value?.quota?.member && user.enabled)))
  .map(user => ({ label: name(user) + (user.quota?.member ? '' : '（未分组，将接替原份额）'), value: user.id })))
const deleteName = computed(() => deleteUser.value ? name(deleteUser.value) : '')
const canDelete = computed(() => Boolean(deletePreview.value && deleteConfirmName.value === deleteName.value))
const deleteSummary = computed(() => {
  const preview = deletePreview.value
  if (!preview) return '选择接收账户后预览。余额和原份额全部转交指定成员，其他成员份额不变。未分组的新账户会自动加入原分组。'
  const to = deleteUsers.value.find(user => user.id === preview.payload.transferToUserId), q = preview.result.transferred
  let message = `将删除成员「${deleteName.value}」，把全部剩余余额和原份额交给「${to ? name(to) : preview.payload.transferToUserId}」：V5 ${number(q.v5)}，订阅 Anlas ${number(q.anlasFixed)}，购买 Anlas ${number(q.anlasPurchased)}。其他成员份额不变。登录与 API Key 将失效，历史记录保留。接收者 V5 如超过容量，将暂停恢复直到低于容量。`
  if (to && !to.quota?.member) message += ' 新账户将自动加入原分组并继承被删除成员的份额，其他成员的份额不变。'
  const changes = preview.result.groups.filter(group => group.share !== deleteGroups.value.find(old => old.id === group.id)?.share)
  if (changes.length) message += ' 分组份额调整：' + changes.map(group => `${group.name} ${percent(deleteGroups.value.find(old => old.id === group.id)?.share)} → ${percent(group.share)}`).join('；') + '。'
  return message
})
const dialogTitle = computed(() => !dialog.value ? '' : dialog.value === 'credentials' ? (editingId.value ? '重置 ' + username.value + ' 的密码' : '创建成员')
  : dialog.value === 'delivery' ? '成员分发信息' : dialog.value === 'confirm' ? confirmation.value?.title || '确认操作' : dialog.value === 'transfer' ? '调整成员额度' : '删除成员')
const dialogTitleId = computed(() => dialog.value === 'credentials' ? 'memberDialogTitle' : dialog.value === 'confirm' ? 'confirmTitle' : 'memberOperationTitle')

function closeDialog() {
  dialogVersion++; deleteReadVersion++; dialog.value = null
  password.value = ''; deliveryText.value = ''; showPassword.value = false; deliveryLoading.value = false
  confirmation.value = null; deletePreview.value = null; deleteConfirmName.value = ''
}
function openDialog(kind: Dialog) { closeDialog(); dialog.value = kind; return dialogVersion }
function updateDialog(show: boolean) { if (!show) closeDialog() }
function portal() { return (props.state.publicBaseUrl || location.origin).replace(/\/+$/, '') }
function openCredentials(user?: ManagementUser) {
  requireClean(); openDialog('credentials')
  editingId.value = user?.id || ''; username.value = user?.username || ''; createGroup.value = ''; credentialDiscord.value = props.state.discordLogin
  createGroups.value = [{ label: '暂不入组，稍后分配', value: '' }, ...props.state.groups.map(group => ({ value: group.id, label: group.name + (group.members.length ? '' : '（先在分组页分配份额）'), disabled: !group.members.length }))]
}
function randomPassword() {
  password.value = Array.from(crypto.getRandomValues(new Uint8Array(24)), byte => byte.toString(16).padStart(2, '0')).join('')
  emit('notice', '随机密码已生成，保存后可复制分发信息。')
}
async function delivery(user: MemberIdentity, savedPassword = '') {
  const version = openDialog('delivery'), discord = props.state.discordLogin, base = portal()
  deliveryLoading.value = true
  try {
    const key = await manage<{ apiKey: string }>({ action: 'member-key', userId: user.id })
    if (current(version, 'delivery')) deliveryText.value = [`成员入口：${base}/`, `用户名：${name(user)}`,
      discord ? '登录方式：Discord（首次登录需要管理员邀请码）' : '密码：' + (savedPassword || '使用此前分配的密码；忘记请联系管理员重置'), `API Base：${base}/v1`, `API Key：${key.apiKey}`].join('\n')
  } finally { if (current(version, 'delivery')) deliveryLoading.value = false }
}
async function inviteMember(user: MemberIdentity) {
  requireClean()
  const version = openDialog('delivery'), base = portal()
  deliveryLoading.value = true
  try {
    const invite = await manage<{ code: string; expiresAt: number }>({ action: 'discord-invite', userId: user.id })
    if (!active) return
    emit('changed', '邀请码已生成；重新生成会撤销旧邀请码。')
    if (current(version, 'delivery')) deliveryText.value = [`成员入口：${base}/`, `成员：${name(user)}`, `邀请码：${invite.code}`, `有效期：${date(invite.expiresAt)}`,
      '首次登录填写邀请码，再通过 Discord 验证指定服务器成员身份。', '邀请码仅可使用一次；重新生成会撤销旧邀请码。绑定后直接使用 Discord 登录。'].join('\n')
  } finally { if (current(version, 'delivery')) deliveryLoading.value = false }
}
async function saveCredentials() {
  requireClean()
  const version = dialogVersion, id = editingId.value, savedPassword = password.value, discord = credentialDiscord.value, groupId = createGroup.value
  const user = await manage<MemberIdentity>({ action: 'credentials', userId: id, username: username.value, password: savedPassword, ...(!id && groupId ? { groupId } : {}) })
  if (!active) return
  emit('changed', id ? '新密码已保存；原 API Key 不变。' : groupId ? '成员已创建并加入分组。' : '成员已创建，尚未入组，请到分组页设置份额。')
  if (!current(version, 'credentials')) return
  closeDialog()
  try { if (discord) await inviteMember(user); else await delivery(user, savedPassword) }
  catch (failure) {
    if (active && !(failure as { stale?: boolean } | null)?.stale) emit('notice', '账户已保存，但分发信息读取失败。请勿重复创建；在该成员行重新生成邀请码或打开分发信息。', true)
  }
}
async function copyDelivery() {
  const version = dialogVersion
  await navigator.clipboard.writeText(deliveryText.value)
  if (current(version, 'delivery')) emit('notice', '已复制这一位成员的登录与 API 配置。')
}
function confirmStatus(user: ManagementUser) {
  requireClean()
  if (protectedOwner(user)) throw new Error('不能停用当前 owner。')
  openDialog('confirm')
  confirmation.value = { title: (user.enabled ? '停用 ' : '恢复 ') + name(user), body: { action: 'member-status', userId: user.id, enabled: !user.enabled },
    description: user.enabled ? '将禁止该成员登录和新的 API 调用。余额、分组、份额保留，不会分给其他成员。' : '恢复该成员登录及 API 调用，继续使用原 Key 和保留额度。' }
}
function confirmAdmission(user: ManagementUser) {
  requireClean(); openDialog('confirm')
  const acceptingJobs = user.acceptingJobs === false
  confirmation.value = { title: (acceptingJobs ? '恢复接单：' : '停止接单：') + name(user), body: { action: 'member-admission', userId: user.id, acceptingJobs },
    description: acceptingJobs ? '允许提交新任务。此前取消的等待任务和旧批次剩余图片不会恢复，请重新提交。' : '将立即禁止新任务并取消未派发任务、退回其预留。在途生成正常完成和结算；成员仍可登录、查询及下载已有结果。' }
}
async function confirmAction() {
  requireClean()
  const task = confirmation.value, version = dialogVersion
  if (!task) return
  const result = await manage<{ cancelledCount?: number }>(task.body)
  if (!active) return
  if (current(version, 'confirm')) closeDialog()
  emit('changed', task.body.action === 'member-status' ? '成员状态已更新。' : task.body.acceptingJobs ? '已恢复接单。' : `已停止接单，取消 ${number(result.cancelledCount ?? 0)} 个未派发任务。`)
}
function openTransfer(user: ManagementUser) {
  requireClean()
  const members = props.state.users.filter(item => item.quota?.member)
  if (!members.some(item => item.id === user.id)) throw new Error('请先将该成员加入共享分组。')
  if (members.length < 2) throw new Error('至少需要两位共享成员才能转移额度。')
  openDialog('transfer'); transferUsers.value = members.map(copyUser); transferConfigVersion = props.state.configVersion
  transferFrom.value = user.id; transferTo.value = members.find(item => item.id !== user.id)!.id; transferResource.value = 'v5'; transferAmount.value = null
}
async function transfer() {
  requireClean()
  const version = dialogVersion, resource = transferResource.value, amount = transferAmount.value
  if (transferFrom.value === transferTo.value) throw new Error('转出与接收成员不能相同。')
  if (amount == null || !Number.isFinite(amount) || amount <= 0) throw new Error('请输入大于 0 的转移额度。')
  await manage({ action: 'transfer-quota', fromUserId: transferFrom.value, toUserId: transferTo.value, resource, amount, expectedConfigVersion: transferConfigVersion })
  if (!active) return
  if (current(version, 'transfer')) closeDialog()
  emit('changed', resourceNames[resource] + ' 已转移。')
}
function resetDeletePreview() { deleteReadVersion++; deletePreview.value = null; deleteConfirmName.value = '' }
function changeRecipient(value: string) { deleteRecipient.value = value; resetDeletePreview() }
function openDelete(user: ManagementUser) {
  requireClean()
  if (protectedOwner(user)) throw new Error('不能删除当前 owner。')
  const candidates = props.state.users.filter(item => item.id !== user.id && (item.quota?.member || (user.quota?.member && item.enabled)))
  if (!candidates.length) throw new Error('没有可接收的其他账户。请创建并启用一个新账户；未分组的新账户也可接替当前共享成员。')
  openDialog('delete'); deleteUser.value = copyUser(user); deleteUsers.value = props.state.users.map(copyUser)
  deleteGroups.value = props.state.groups.map(group => ({ ...group, members: [...group.members], memberWeights: { ...group.memberWeights } }))
  deleteConfigVersion = props.state.configVersion; deleteRecipient.value = candidates[0]!.id
}
async function previewDelete() {
  requireClean()
  if (!deleteUser.value) return
  const version = dialogVersion, read = ++deleteReadVersion
  const body = { action: 'preview-delete-member', userId: deleteUser.value.id, transferToUserId: deleteRecipient.value, expectedConfigVersion: deleteConfigVersion }
  deletePreview.value = null; deleteConfirmName.value = ''
  const result = await manage<ManagementMemberChange>(body)
  if (current(version, 'delete') && read === deleteReadVersion) deletePreview.value = { result, payload: { ...body, action: 'delete-member', expectedConfigVersion: result.configVersion } }
}
async function confirmDelete() {
  requireClean()
  if (!deletePreview.value || !canDelete.value) throw new Error('请输入被删除成员的完整用户名确认。')
  const version = dialogVersion, payload = { ...deletePreview.value.payload }
  await manage(payload)
  if (!active) return
  if (current(version, 'delete')) closeDialog()
  emit('changed', '成员已删除，全部剩余余额已转交，历史记录已保留。')
}
function changeLabel(userId: string) {
  const user = deleteUsers.value.find(item => item.id === userId)
  return (user ? name(user) : userId) + (userId === deleteUser.value?.id ? '（删除）' : userId === deletePreview.value?.payload.transferToUserId ? '（接收）' : '')
}
onBeforeUnmount(() => { active = false; closeDialog() })

const pagination = { pageSize: 10, showSizePicker: true, pageSizes: [10, 25, 50] }
function memberActions(user: ManagementUser) {
  return [
    props.state.discordLogin ? { label: '邀请码', key: 'invite', disabled: !user.enabled || !!user.discordId } : { label: '重置密码', key: 'credentials', disabled: !user.enabled },
    { label: '调整额度', key: 'transfer' },
    { label: user.acceptingJobs === false ? '恢复接单' : '停止接单', key: 'admission' },
    { label: user.enabled ? '停用' : '恢复', key: 'status', disabled: protectedOwner(user) },
    { type: 'divider' as const, key: 'divider' },
    { label: '删除', key: 'delete', disabled: protectedOwner(user) }
  ]
}
function memberAction(key: string, user: ManagementUser) {
  const actions: Record<string, () => void | Promise<void>> = { invite: () => inviteMember(user), credentials: () => openCredentials(user),
    transfer: () => openTransfer(user), admission: () => confirmAdmission(user), status: () => confirmStatus(user), delete: () => openDelete(user) }
  if (actions[key]) void props.run(actions[key])
}
const memberColumns = computed<DataTableColumns<ManagementUser>>(() => [
  { title: '成员', key: 'username', sorter: 'default', width: 220, render: user => h('div', [h('strong', user.username || user.note || user.id),
    h('small', props.state.discordLogin ? user.discordId ? '已绑定 Discord' : '尚未绑定 Discord' : '密码登录')]) },
  { title: '状态', key: 'enabled', width: 130, filterOptions: [{ label: '已启用', value: 'true' }, { label: '已停用', value: 'false' }], filter: (value, user) => String(user.enabled) === value,
    render: user => h('div', [h(NTag, { size: 'small', bordered: false, type: user.enabled ? 'success' : 'default' }, () => user.enabled ? '已启用' : '已停用'), user.acceptingJobs === false ? h('small', '已停止接单') : null]) },
  { title: '分组 / 份额', key: 'share', width: 150, sorter: (a, b) => (a.quota?.share || 0) - (b.quota?.share || 0), render: user => h('div', [groupOf(user.id)?.name || '未分组', h('small', percent(user.quota?.share))]) },
  ...(['v5Available', 'anlasFixedAvailable', 'anlasPurchasedAvailable'] as const).map((key, index) => ({ title: ['V5 余量', '订阅 Anlas', '购买 Anlas'][index], key, width: 125, align: 'right' as const,
    sorter: (a: ManagementUser, b: ManagementUser) => (a.quota?.[key] || 0) - (b.quota?.[key] || 0), render: (user: ManagementUser) => number(user.quota?.[key]) })),
  { title: '免费图 / 24 小时', key: 'freeImages', width: 220, render: user => {
    const usage = user.quota?.freeImages
    return usage ? h('div', [h('span', `${usage.successful} 张成功 · ${usage.reserved} 张在途`),
      h('small', usage.enabled ? `提醒 ${usage.warningThreshold ?? '无'} / 上限 ${usage.hardLimit ?? '无'}` : '策略关闭'),
      usage.warning ? h(NTag, { type: 'warning', size: 'small' }, () => '用量达到阈值') : null,
      usage.warning && usage.warnedAt ? h('small', '触发于 ' + date(usage.warnedAt)) : null]) : '—'
  } },
  { title: '操作', key: 'actions', width: 200, render: user => h('div', { class: 'row-actions' }, [
    h(NButton, { size: 'small', secondary: true, disabled: props.busy, onClick: () => props.run(() => delivery(user)) }, () => '分发信息'),
    h(NDropdown, { trigger: 'click', options: memberActions(user), onSelect: key => memberAction(key, user) }, { default: () => h(NButton, { size: 'small', disabled: props.busy }, () => '更多') })
  ]) }
])
type ShareChange = ManagementMemberChange['shareChanges'][number]
const shareColumns = computed<DataTableColumns<ShareChange>>(() => [
  { title: '成员', key: 'userId', sorter: 'default', render: row => changeLabel(row.userId), filterOptions: (deletePreview.value?.result.shareChanges || []).map(row => ({ label: changeLabel(row.userId), value: row.userId })), filter: (value, row) => row.userId === value },
  { title: '当前全局份额', key: 'beforeShare', sorter: 'default', render: row => percent(row.beforeShare) },
  { title: '删除后全局份额', key: 'afterShare', sorter: 'default', render: row => percent(row.afterShare) }
])
</script>

<template>
  <ManagementFreeImages :policy="state.freeImagePolicy" :api="api" :run="run" :busy="busy" @changed="message => emit('changed', message)" />
  <NCard id="membersPanel" title="成员与分发" size="small">
    <template #header-extra><NButton id="newMember" :disabled="busy" @click="run(() => openCredentials())">＋ 创建成员</NButton></template>
    <p id="memberHelp">{{ state.discordLogin ? '首次登录发放一次性邀请码，成员通过 Discord 领取自己的额度。' : '创建、分发、重置密码，都在成员这一行完成。' }}</p>
    <div class="member-filters">
      <TableSearch v-model="search" placeholder="搜索用户名" label="搜索用户名" />
      <NSelect id="filterGroup" v-model:value="filterGroup" :options="groupFilters" aria-label="按分组筛选" />
      <span id="memberCount">{{ users.length }} / {{ state.users.length }} 位成员</span>
    </div>
    <div class="members-scroll">
      <NDataTable id="members" :columns="memberColumns" :data="users" :row-key="(user: ManagementUser) => user.id" :row-props="(user: ManagementUser) => ({ class: 'member-row', 'data-member-id': user.id })" :pagination="pagination" :scroll-x="1075" size="small"><template #empty>没有符合条件的成员</template></NDataTable>
    </div>
    <p class="muted">停用会阻止登录和新请求，但不回收余额、份额或恢复权重。登录方式由站点配置决定。</p>
  </NCard>

  <NModal :show="dialog !== null" @update:show="updateDialog">
    <NCard :id="dialog ? dialogIds[dialog] : undefined" class="member-modal" role="dialog" aria-modal="true" :aria-labelledby="dialogTitleId">
      <template #header><span :id="dialogTitleId">{{ dialogTitle }}</span></template>
      <template #header-extra><NButton id="closeMemberOperation" secondary @click="closeDialog">关闭</NButton></template>
      <form v-if="dialog === 'credentials'" id="credentials" @submit.prevent="run(saveCredentials)">
        <input id="editingId" type="hidden" :value="editingId">
        <label for="username">用户名</label>
        <NInput v-model:value="username" :readonly="!!editingId" :disabled="busy" :maxlength="40" :input-props="{ id: 'username', required: true, pattern: '[A-Za-z0-9][A-Za-z0-9_.-]{2,39}', autocomplete: 'off' }" />
        <template v-if="!credentialDiscord">
          <label id="passwordLabel" for="password">登录密码（至少 12 位）</label>
          <NInput v-model:value="password" :type="showPassword ? 'text' : 'password'" :disabled="busy" :maxlength="128" :input-props="{ id: 'password', required: true, minlength: 12, autocomplete: 'new-password' }" />
          <div class="row-actions"><NButton id="generatePassword" secondary :disabled="busy" @click="randomPassword">随机生成</NButton><NButton id="showPassword" secondary :disabled="busy" @click="showPassword = !showPassword">{{ showPassword ? '隐藏' : '显示' }}</NButton></div>
        </template>
        <template v-if="!editingId"><label id="createGroupLabel">加入分组</label><NSelect id="createGroup" v-model:value="createGroup" :options="createGroups" :disabled="busy" aria-label="加入分组" /></template>
        <p id="credentialHint" class="muted">{{ editingId ? '原 API Key 保持不变，旧登录会话将失效。' : credentialDiscord ? '创建后生成一次性 Discord 邀请码；新成员余额为 0，份额由管理员分配。' : '可加入已有分组，或暂不入组。新成员当前余额为 0；要启用空组，请在分组页同时分配份额。' }}</p>
        <NButton id="saveMember" attr-type="submit" type="primary" :disabled="busy" :loading="busy">{{ editingId ? '保存新密码' : credentialDiscord ? '创建成员并生成邀请码' : '创建并生成分发信息' }}</NButton>
      </form>

      <div v-else-if="dialog === 'delivery'">
        <p class="muted">只包含这一位成员的信息，请发送给对应成员。</p>
        <p v-if="deliveryLoading" role="status">正在读取分发信息…</p>
        <p v-else-if="!deliveryText" role="status">未取得分发信息，请关闭后在成员行重试。</p>
        <NInput type="textarea" :value="deliveryText" readonly :autosize="{ minRows: 6 }" :input-props="{ id: 'deliveryText', 'aria-label': '成员分发信息' }" />
        <div class="row-actions modal-actions"><NButton id="copyDelivery" :disabled="busy || !deliveryText" @click="run(copyDelivery)">复制全部信息</NButton><NButton secondary @click="closeDialog">完成</NButton></div>
      </div>

      <div v-else-if="dialog === 'confirm' && confirmation">
        <p id="confirmDescription">{{ confirmation.description }}</p>
        <div class="row-actions"><NButton id="confirmAction" type="primary" :disabled="busy" :loading="busy" @click="run(confirmAction)">确认</NButton><NButton secondary @click="closeDialog">取消</NButton></div>
      </div>

      <form v-else-if="dialog === 'transfer'" id="transferForm" @submit.prevent="run(transfer)">
        <label>转出成员</label><NSelect id="transferFrom" v-model:value="transferFrom" :options="transferOptions" :disabled="busy" aria-label="转出成员" />
        <label>接收成员</label><NSelect id="transferTo" v-model:value="transferTo" :options="transferOptions" :disabled="busy" aria-label="接收成员" />
        <label>额度类型</label><NSelect id="transferResource" v-model:value="transferResource" :options="resourceOptions" :disabled="busy" aria-label="额度类型" />
        <label for="transferAmount">转移数量</label><NInputNumber v-model:value="transferAmount" :min="0.000001" :disabled="busy" :input-props="{ id: 'transferAmount', required: true }" />
        <NAlert id="transferHint" type="info" :show-icon="false">{{ transferHint }}</NAlert>
        <NButton attr-type="submit" type="primary" :disabled="busy" :loading="busy">确认转移额度</NButton>
      </form>

      <div v-else-if="dialog === 'delete'">
        <input id="deleteUserId" type="hidden" :value="deleteUser?.id"><p>被删除成员：<strong id="deleteName">{{ deleteName }}</strong></p>
        <label>全部剩余余额与原份额转交给</label><NSelect id="deleteRecipient" :value="deleteRecipient" :options="deleteOptions" :disabled="busy" aria-label="余额与份额接收者" @update:value="changeRecipient" />
        <NButton id="previewDelete" secondary class="modal-actions" :disabled="busy" @click="run(previewDelete)">预览删除与余额转移</NButton>
        <NAlert id="deleteSummary" type="warning" :show-icon="false" class="modal-actions">{{ deleteSummary }}</NAlert>
        <div v-if="deletePreview?.result.shareChanges.length" id="deleteSharePreview" class="members-scroll modal-actions">
          <NDataTable id="deleteShareRows" :columns="shareColumns" :data="deletePreview.result.shareChanges" :row-key="(row: ShareChange) => row.userId" :pagination="pagination" size="small" />
        </div>
        <div v-if="deletePreview" id="deleteConfirmSection" class="modal-actions">
          <label for="deleteConfirmName">输入被删除成员的完整用户名确认</label><NInput v-model:value="deleteConfirmName" :disabled="busy" :input-props="{ id: 'deleteConfirmName', autocomplete: 'off' }" />
          <NButton id="confirmDelete" type="error" class="modal-actions" :disabled="busy || !canDelete" :loading="busy" @click="run(confirmDelete)">确认删除并转交余额</NButton>
        </div>
      </div>
    </NCard>
  </NModal>
</template>

<style scoped>
.member-filters { display: grid; grid-template-columns: minmax(140px, 1fr) minmax(140px, 220px) auto; gap: 12px; align-items: center; margin-bottom: 16px; }
.members-scroll { max-width: 100%; overflow-x: auto; }
.members-table { min-width: 760px; }
.row-actions, :deep(.row-actions) { display: flex; gap: 8px; flex-wrap: wrap; }
:deep(small) { display: block; margin-top: 4px; color: #adb7c6; }
.muted { color: #adb7c6; }
.member-modal { width: min(780px, calc(100vw - 32px)); max-height: 85vh; overflow: auto; }
.member-modal form { display: grid; gap: 12px; }
.member-modal label { display: block; margin-bottom: 4px; }
.member-modal p { line-height: 1.7; overflow-wrap: anywhere; }
.modal-actions { margin-top: 14px; }
@media (max-width: 700px) {
  .member-filters { grid-template-columns: 1fr; }
}
</style>
