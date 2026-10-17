<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { NAlert, NButton, NCard, NCheckbox, NInput, NInputNumber, NModal, NSelect, NTable } from 'naive-ui'
import type { ManagementApi, ManagementGroup, ManagementMemberChange, ManagementRun, ManagementState } from '../types/management'
import { number } from '../utils/member-display'

const props = defineProps<{ state: ManagementState; api: ManagementApi; run: ManagementRun; busy: boolean }>()
const emit = defineEmits<{ changed: [message: string]; notice: [message: string, error?: boolean]; dirty: [dirty: boolean] }>()
type Mode = 'direct' | 'advanced'
type GroupPayload = { action: 'preview-groups' | 'groups'; groups: ManagementGroup[]; redistributeRemaining: boolean; expectedConfigVersion: number }
type GroupResult = Pick<ManagementMemberChange, 'groups' | 'users' | 'configVersion'>
const groups = ref<ManagementGroup[]>([]), configVersion = ref(0), dirty = ref(false), pending = ref(false)
const mode = ref<Mode>('direct'), directShares = ref<Record<string, number>>({})
const redistribute = ref({ direct: true, advanced: false })
const preview = ref<{ payload: GroupPayload; rows: { id: string; name: string; before: ManagementState['users'][number]['quota']; after: ManagementState['users'][number]['quota'] }[] } | null>(null)
let active = true, editVersion = 0
const locked = computed(() => props.busy || pending.value)
const modeOptions = [{ label: '按成员直接分配（推荐）', value: 'direct' }, { label: '高级：分组与权重', value: 'advanced' }]
const groupOptions = computed(() => [{ label: '不在任何组', value: '' }, ...groups.value.map(group => ({ label: group.name, value: group.id }))])
const total = computed(() => mode.value === 'direct' ? Object.values(directShares.value).reduce((sum, value) => sum + value, 0) : groups.value.reduce((sum, group) => sum + group.share * 100, 0))
const validTotal = computed(() => Number.isFinite(total.value) && Math.abs(total.value - 100) <= 1e-6)
const percent = (value: number | null | undefined) => value == null ? '未知' : Number.isFinite(value) ? `${number(value * 100)}%` : '待填写'
const inputNumber = (value: number) => Number.isFinite(value) ? value : null
const copyGroups = (values: ManagementGroup[]) => values.map(group => ({ ...group, share: group.share ?? .25, members: [...group.members], memberWeights: { ...group.memberWeights } }))
const groupOf = (id: string) => groups.value.find(group => group.members.includes(id))

function draftShare(id: string) {
  const group = groupOf(id)
  if (!group) return 0
  const sum = group.members.reduce((value, member) => value + (group.memberWeights[member] ?? 1), 0)
  if (!Number.isFinite(group.share) || !Number.isFinite(sum)) return NaN
  return sum > 0 ? group.share * (group.memberWeights[id] ?? 1) / sum : group.share === 0 ? 0 : NaN
}
function readDirectShares() {
  directShares.value = Object.fromEntries(props.state.users.map(user => [user.id, Number((draftShare(user.id) * 100).toFixed(10))]))
}
function setDirty(value: boolean) { dirty.value = value; emit('dirty', value) }
function edited(markDirty = true) {
  editVersion++
  preview.value = null
  if (markDirty) setDirty(true)
}
function loadSnapshot() {
  groups.value = copyGroups(props.state.groups)
  configVersion.value = props.state.configVersion
  edited(false); setDirty(false); readDirectShares()
}
function resetDraft() {
  if (locked.value) return
  if (props.state.configVersion < configVersion.value) {
    emit('notice', '分组已保存，但页面配置尚未刷新；请先刷新管理摘要再撤销修改。', true)
    return
  }
  mode.value = 'direct'; redistribute.value = { direct: true, advanced: false }
  loadSnapshot()
  emit('notice', '未保存的分组修改已撤销，已读取最新配置。')
}
function switchMode(value: Mode) {
  mode.value = value
  if (value === 'direct') readDirectShares()
  edited(false)
}
function applyDirectShares() {
  for (const user of props.state.users) if (user.enabled && !groupOf(user.id)) groups.value[0]?.members.push(user.id)
  for (const group of groups.value) {
    group.memberWeights = Object.fromEntries(group.members.map(id => [id, directShares.value[id] ?? 0]))
    group.share = Object.values(group.memberWeights).reduce((sum, value) => sum + value, 0) / 100
  }
}
function changeDirect(id: string, value: number | null) {
  directShares.value[id] = value ?? NaN
  applyDirectShares(); edited()
}
function changeGroup(id: string, targetId: string) {
  const weight = groupOf(id)?.memberWeights[id] ?? 1
  for (const group of groups.value) {
    group.members = group.members.filter(member => member !== id)
    delete group.memberWeights[id]
  }
  const target = groups.value.find(group => group.id === targetId)
  if (target) { target.members.push(id); target.memberWeights[id] = weight }
  edited()
}
function changeWeight(id: string, value: number | null) {
  const group = groupOf(id)
  if (group) { group.memberWeights[id] = value ?? NaN; edited() }
}
function validate() {
  if (mode.value === 'direct') {
    const values = Object.values(directShares.value)
    if (values.some(value => !Number.isFinite(value) || value < 0 || value > 100)) throw new Error('每位成员的份额必须在 0% 到 100% 之间。')
    if (Math.abs(values.reduce((sum, value) => sum + value, 0) - 100) > 1e-6) throw new Error('成员份额合计必须为 100%，请调整后再预览。')
  }
  if (groups.value.some(group => !Number.isFinite(group.share) || group.share < 0 || group.share > 1) || Math.abs(groups.value.reduce((sum, group) => sum + group.share, 0) - 1) > 1e-8) throw new Error('四组份额须合计 100%。')
  if (groups.value.some(group => !group.members.length && group.share !== 0)) throw new Error('空组份额须为 0%。')
  if (!groups.value.some(group => group.members.length)) throw new Error('至少保留一位已分组成员。')
  for (const group of groups.value) {
    const weights = group.members.map(id => group.memberWeights[id] ?? 1)
    if (weights.some(weight => !Number.isFinite(weight) || weight < 0 || weight > 1e6)) throw new Error('组内权重须在 0 到 1000000 之间。')
    if (group.share > 0 && weights.reduce((sum, weight) => sum + weight, 0) <= 0) throw new Error('有份额的组至少一位成员权重须大于 0。')
  }
}
function previewGroups() {
  if (locked.value) return
  void props.run(async () => {
    pending.value = true
    preview.value = null
    try {
      if (mode.value === 'direct') applyDirectShares()
      validate()
      const revision = editVersion
      const payload: GroupPayload = { action: 'preview-groups', groups: copyGroups(groups.value), redistributeRemaining: redistribute.value[mode.value], expectedConfigVersion: configVersion.value }
      const before = props.state.users.map(user => ({ id: user.id, name: user.username || user.id, before: user.quota ? { ...user.quota } : null }))
      const result = await props.api<GroupResult>('member-management', payload)
      if (!active) return
      if (revision !== editVersion) { emit('notice', '预览期间草稿已修改，请重新预览。'); return }
      preview.value = { payload: { ...payload, action: 'groups' }, rows: before.map(user => ({ ...user, after: result.users.find(item => item.id === user.id)?.quota ?? null })) }
    } finally { pending.value = false }
  })
}
function saveGroups() {
  if (locked.value || !preview.value) return
  const payload = { ...preview.value.payload, groups: copyGroups(preview.value.payload.groups) }, revision = editVersion
  void props.run(async () => {
    pending.value = true
    try {
      const result = await props.api<Pick<GroupResult, 'groups' | 'configVersion'>>('member-management', payload)
      if (!active) return
      preview.value = null
      // 只有本次保存的成功响应能推进仍在编辑的草稿版本；后台快照不能替它换版本。
      configVersion.value = result.configVersion
      if (revision === editVersion) {
        groups.value = copyGroups(result.groups); readDirectShares(); setDirty(false)
        emit('changed', '分组与额度分配已保存。')
      } else {
        setDirty(true)
        emit('changed', '刚才预览已保存，后续修改仍未保存，请重新预览。')
      }
    } finally { pending.value = false }
  })
}
watch(() => props.state, () => {
  if (!dirty.value && !pending.value && props.state.configVersion >= configVersion.value) loadSnapshot()
}, { immediate: true })
onBeforeUnmount(() => { active = false; preview.value = null })
</script>

<template>
  <section id="groupsPanel" aria-labelledby="groupsTitle">
    <header class="toolbar"><div><h2 id="groupsTitle">分组与额度</h2><p class="muted">填写每位成员的百分比，合计 100% 后预览并保存。</p></div><NButton id="resetDraft" :disabled="locked" @click="resetDraft">撤销未保存修改</NButton></header>
    <label id="allocationModeLabel">分配方式</label>
    <NSelect id="allocationMode" :value="mode" :options="modeOptions" aria-labelledby="allocationModeLabel" @update:value="switchMode" />
    <NAlert v-if="dirty && state.configVersion !== configVersion" type="warning" class="notice" role="status">后台分组配置已有更新。草稿仍基于版本 {{ configVersion }}；可保留查看，或撤销后按最新配置重新编辑。</NAlert>
    <div v-if="mode === 'direct'" id="directAllocation">
      <h3>按成员直接分配</h3>
      <p class="muted">填写每个人占全部额度的百分比，例如 70% 与 30%。0% 保留成员但不分配未来额度。未分组的启用成员保存时自动加入第一组。</p>
      <div class="table-scroll" tabindex="0" aria-label="成员具体份额表，可横向滚动"><NTable :single-line="false"><thead><tr><th>成员</th><th>当前份额</th><th>分配具体份额（%）</th><th>当前 V5 余额</th></tr></thead><tbody id="directRows">
        <tr v-for="user in state.users" :key="user.id"><td>{{ user.username || user.id }}{{ user.enabled ? '' : '（停用）' }}</td><td>{{ percent(user.quota?.share) }}</td><td><NInputNumber :value="inputNumber(directShares[user.id] ?? 0)" :min="0" :max="100" :show-button="false" :disabled="!user.enabled && !groupOf(user.id)" :input-props="{ id: 'direct-' + user.id, 'aria-label': (user.username || user.id) + ' 的具体总份额百分比' }" @update:value="changeDirect(user.id, $event)" /><small v-if="!user.enabled && !groupOf(user.id)">先启用再分配</small></td><td>{{ number(user.quota?.v5Available) }}</td></tr>
      </tbody></NTable></div>
    </div>
    <div v-else id="advancedAllocation">
      <div id="groupCards" class="group-grid"><NCard v-for="group in groups" :key="group.id" size="small">
        <label :for="'group-name-' + group.id">组名 · {{ group.members.length }} 人</label><NInput :value="group.name" :maxlength="60" :input-props="{ id: 'group-name-' + group.id }" @update:value="group.name = $event; edited()" />
        <label :for="'group-share-' + group.id">总份额（%）</label><NInputNumber :value="inputNumber(group.share * 100)" :min="0" :max="100" :show-button="false" :input-props="{ id: 'group-share-' + group.id }" @update:value="group.share = $event == null ? NaN : $event / 100; edited()" />
      </NCard></div>
      <p class="muted">空组份额须为 0%。成员权重和非空组份额允许为 0；有份额的组至少一位成员权重大于 0。权重 2 的成员获得同组权重 1 成员的两倍份额。</p>
      <div class="table-scroll" tabindex="0" aria-label="分组权重表，可横向滚动"><NTable :single-line="false"><thead><tr><th>成员</th><th>目标分组</th><th>组内权重</th><th>当前总份额</th><th>调整后总份额</th></tr></thead><tbody id="assignments">
        <tr v-for="user in state.users" :key="user.id"><td>{{ user.username || user.id }}{{ user.enabled ? '' : '（停用）' }}</td><td><NSelect :value="groupOf(user.id)?.id || ''" :options="groupOptions" :disabled="!user.enabled" :aria-label="(user.username || user.id) + ' 的目标分组'" @update:value="changeGroup(user.id, $event)" /></td><td><NInputNumber :value="inputNumber(groupOf(user.id)?.memberWeights[user.id] ?? 1)" :min="0" :max="1000000" :show-button="false" :disabled="!groupOf(user.id)" :input-props="{ id: 'weight-' + user.id, 'aria-label': (user.username || user.id) + ' 的组内权重' }" @update:value="changeWeight(user.id, $event)" /></td><td>{{ percent(user.quota?.share) }}</td><td>{{ percent(draftShare(user.id)) }} · V5 容量 {{ Number.isFinite(draftShare(user.id)) ? number(100 * draftShare(user.id)) : '待填写' }}</td></tr>
      </tbody></NTable></div>
    </div>
    <p :id="mode === 'direct' ? 'directTotal' : 'shareTotal'" :class="{ danger: !validTotal }" role="status">{{ mode === 'direct' ? '已分配' : '四组总份额' }} {{ Number.isFinite(total) ? number(total) + '% / 100%' : '待填写有效数字' }}</p>
    <NCheckbox id="redistribute" :checked="redistribute[mode]" @update:checked="redistribute[mode] = $event; edited()">同步分配当前剩余余额（推荐）</NCheckbox>
    <p class="muted">勾选后保留各组总余额，按组内新权重重新分配；不会自动跨组转移。跨组调额请用成员行的「调整额度」。</p>
    <NAlert v-if="!redistribute[mode]" id="balancePreserveWarning" type="warning" role="status">仅以后生效：当前余额保持不变。已有额度仍留在原成员账户。</NAlert>
    <div class="toolbar actions"><span id="draftHint" role="status">{{ dirty ? '有未保存的修改 · 请先预览' : '尚无未保存修改' }} · 版本 {{ configVersion }}</span><NButton id="previewGroups" type="primary" :disabled="locked" :loading="pending" @click="previewGroups">预览并保存分配</NButton></div>
    <NModal :show="Boolean(preview)" preset="card" title="确认分配结果" class="group-preview" :style="{ width: 'min(1080px, calc(100vw - 32px))' }" :mask-closable="!pending" :closable="!pending" @update:show="value => { if (!value && !pending) preview = null }">
      <template v-if="preview"><NAlert id="previewDescription" type="info" :show-icon="false">{{ preview.payload.redistributeRemaining ? '将保留每组总余额，按组内权重重新分配剩余余额；跨组转移请使用调整额度。' : '仅以后生效：只调整分组、未来恢复与新增额度分配；当前余额保持原样。' }}</NAlert>
        <div class="table-scroll preview-table" tabindex="0" aria-label="分配前后对照表，可横向滚动"><NTable :single-line="false"><thead><tr><th>成员</th><th>份额：当前 → 保存后</th><th>V5 余额 / 容量：当前 → 保存后</th><th>订阅：当前 → 保存后</th><th>购买：当前 → 保存后</th></tr></thead><tbody id="previewRows"><tr v-for="row in preview.rows" :key="row.id"><td>{{ row.name }}</td><td>{{ percent(row.before?.share) }} → {{ percent(row.after?.share) }}</td><td>{{ number(row.before?.v5Available) }} / {{ number(row.before?.v5Capacity) }} → {{ number(row.after?.v5Available) }} / {{ number(row.after?.v5Capacity) }}</td><td>{{ number(row.before?.anlasFixedAvailable) }} → {{ number(row.after?.anlasFixedAvailable) }}</td><td>{{ number(row.before?.anlasPurchasedAvailable) }} → {{ number(row.after?.anlasPurchasedAvailable) }}</td></tr></tbody></NTable></div>
        <p class="muted">预览由后端账本计算。余额会随生成与恢复变化，保存按当时可用余额计算；分组配置被其他操作修改时需重新编辑。</p>
        <div class="toolbar"><NButton id="confirmGroups" type="primary" :disabled="locked" :loading="pending" @click="saveGroups">确认保存分配</NButton><NButton :disabled="pending" @click="preview = null">返回修改</NButton></div>
      </template>
    </NModal>
  </section>
</template>

<style scoped>
section { min-width: 0; }
h2 { margin: 0; font-size: 1.3rem; }
p { line-height: 1.7; }
label { display: block; margin: 10px 0 8px; }
.toolbar { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; justify-content: space-between; }
.actions, .notice { margin-top: 20px; }
.muted, small { color: #adb7c6; }
.danger { color: #ff9d9d; }
.group-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 20px; }
.table-scroll { max-width: 100%; overflow-x: auto; margin-top: 16px; }
.table-scroll :deep(table) { min-width: 650px; }
.table-scroll :deep(.n-input-number) { min-width: 120px; }
.table-scroll :deep(.n-select) { min-width: 150px; }
.preview-table :deep(table) { min-width: 840px; }
@media (max-width: 760px) { .group-grid { grid-template-columns: 1fr; } }
</style>
