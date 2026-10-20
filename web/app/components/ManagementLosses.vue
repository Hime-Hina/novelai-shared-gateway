<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, reactive, ref, shallowRef, watch } from 'vue'
import { NAlert, NButton, NCard, NCheckbox, NForm, NFormItem, NInput, NInputNumber, NSelect, NSpace, NDataTable, type DataTableColumns } from 'naive-ui'
import type { ManagementAlert, ManagementApi, ManagementLossEvent, ManagementLosses, ManagementLossPolicy, ManagementResource, ManagementResources, ManagementRun, ManagementState } from '../types/management'
import { date, number } from '../utils/member-display'

const props = defineProps<{ state: ManagementState; api: ManagementApi; run: ManagementRun; busy: boolean; refreshVersion: number; active: boolean }>()
const emit = defineEmits<{ changed: [message: string]; notice: [message: string, error?: boolean]; dirty: [value: boolean]; alerts: [value: ManagementAlert[]] }>()
const losses = shallowRef<ManagementLosses | null>(null), history = shallowRef<ManagementLossEvent[]>([])
const nextCursor = ref<string | null>(null), paged = ref(false), loading = ref(false), readError = ref('')
const policyVersion = ref<number | null>(null), policyDirty = ref(false), policySaving = ref(false), operatingSaving = ref(false)
const policyFields = [
  { key: 'v5Points24h', label: 'V5 候选风险或容量差额 / 24 小时', integer: false },
  { key: 'fixedAnlas24h', label: '订阅 Anlas / 24 小时', integer: true },
  { key: 'fixedAnlas30d', label: '订阅 Anlas / 30 天', integer: true },
  { key: 'memberAttempts24h', label: '同一成员异常尝试 / 24 小时', integer: true },
] as const
type PolicyField = typeof policyFields[number]['key']
const policyDraft = reactive<Record<PolicyField, number | null>>({ v5Points24h: null, fixedAnlas24h: null, fixedAnlas30d: null, memberAttempts24h: null })
const resources: ManagementResource[] = ['v5', 'anlasFixed', 'anlasPurchased']
const resourceNames: Record<ManagementResource, string> = { v5: 'V5 百分点', anlasFixed: '订阅 Anlas', anlasPurchased: '购买 Anlas' }
type OperatingAction = 'operating-contribute' | 'operating-register-funding' | 'operating-cover-loss' | 'operating-reconcile-return' | 'operating-confirm-debits'
const actionOptions: { value: OperatingAction; label: string }[] = [
  { value: 'operating-contribute', label: '从运营者本人份额划入' },
  { value: 'operating-register-funding', label: '登记新增外部资金' },
  { value: 'operating-cover-loss', label: '用运营余额承担差额' },
  { value: 'operating-reconcile-return', label: '确认快照回跳' },
  { value: 'operating-confirm-debits', label: '确认普通扣款已反映在快照' },
]
const action = ref<OperatingAction>('operating-contribute'), resource = ref<ManagementResource>('anlasFixed')
const amount = ref<number | null>(null), receipt = ref(''), lossEventId = ref('')
const destination = ref<'operating' | 'members'>('operating'), fromUnattributed = ref(false)
const register = computed(() => action.value === 'operating-register-funding')
const linked = computed(() => ['operating-cover-loss', 'operating-reconcile-return'].includes(action.value))
const anlasOnly = computed(() => register.value || ['operating-reconcile-return', 'operating-confirm-debits'].includes(action.value))
const resourceOptions = computed(() => resources.map(value => ({ value, label: resourceNames[value], disabled: value === 'v5' && anlasOnly.value })))
const hints: Record<OperatingAction, string> = {
  'operating-contribute': '仅从配置的运营者本人可用份额划入，不改变其他成员钱包。',
  'operating-register-funding': '登记已实际新增的资金凭据，本页面不会向 NAI 付款。默认等待官方余额增加后入账；只有确认资金来源时，才选择待归属入账。',
  'operating-cover-loss': '用已有运营余额承担指定的当前聚合差额，不再扣成员余额，也不累计任务疑似或估计金额。',
  'operating-reconcile-return': '确认待归属入账来自官方快照回跳，只修正尚未承担的关联差额，不算新增注资，也不回冲已承担的损耗。',
  'operating-confirm-debits': '仅在核实普通扣款已包含于官方快照、并与充值相抵时使用。确认数量会解除待核对扣款，先匹配已登记的资金归属，其余按成员份额补发。不能用等待时间代替核实，请提供唯一凭据。',
}
const eventNames: Record<string, string> = {
  generation_reserved: '任务预扣', generation_refunded: '本站退款', generation_loss_suspected: '疑似上游损耗',
  generation_loss_estimated: '估计结案', generation_result_recovered: '迟到结果找回', resource_gap_observed: '观察到资源差额',
  resource_gap_reconciled: '确认快照回跳', resource_return_unattributed: '待归属入账', v5_capacity_observed: '观察到 V5 容量差额',
  v5_capacity_recovered: 'V5 容量恢复', operating_funding_registered: '登记运营资金', operating_funding_received: '资金入账',
  operating_loss_covered: '运营承担差额', loss_alert_policy_updated: '调整软告警阈值', operating_debits_confirmed: '确认普通扣款',
}
const evidenceNames: Record<string, string> = { aggregate_gap: '官方余额聚合差额', aggregate_capacity: '共享容量差额',
  recovery_rate_estimate: '恢复速率估计', unclassified_balance_increase: '来源待核实的余额增加',
  operator_attested_aggregate_correction: '运营者确认的快照修正', operator_attested_debit_reconciliation: '运营者确认的扣款核对', local_refund: '本地退款凭据',
  unconfirmed: '上游消费未经确认', unconfirmed_estimate: '未确认消费的估计结案', complete_local_result: '完整本地产物' }
let disposed = false, readVersion = 0, editVersion = 0
type ReadMode = 'refresh' | 'append' | 'reset'
let reading: Promise<void> | null = null, pending: ReadMode | null = null
const isStale = (failure: unknown) => failure && typeof failure === 'object'
  && (('stale' in failure && failure.stale === true) || ('code' in failure && failure.code === 'STALE_RESPONSE'))
const message = (failure: unknown) => failure instanceof Error ? failure.message : '请求失败，请重试。'

function adoptPolicy(policy: ManagementLossPolicy) {
  policyVersion.value = policy.version; policyDirty.value = false
  for (const { key } of policyFields) policyDraft[key] = policy[key]
}
function editPolicy(key: PolicyField, value: number | null) { policyDraft[key] = value; policyDirty.value = true; editVersion++ }
function load(mode: ReadMode = 'refresh'): Promise<void> {
  if (disposed) return Promise.resolve()
  if (pending !== 'reset' && (mode !== 'refresh' || !pending)) pending = mode
  if (reading) return reading
  reading = (async () => {
    do {
      const next: ReadMode = pending || 'refresh'; pending = null
      const cursor = next === 'append' ? nextCursor.value : null
      if (next === 'append' && !cursor) continue
      const version = ++readVersion; loading.value = true
      try {
        const result = await props.api<ManagementLosses>('member-management?view=losses' + (cursor ? '&before=' + encodeURIComponent(cursor) : ''))
        if (disposed || version !== readVersion) continue
        // 翻页后刷新统计与告警，但不插入新第一页，避免破坏已读历史的连续范围。
        if (next === 'reset') paged.value = false
        if (next === 'append' || !paged.value) {
          const events = next === 'append' ? [...history.value, ...result.events] : result.events
          history.value = [...new Map(events.map(event => [event.id, event])).values()]
          nextCursor.value = result.nextCursor
        }
        if (next === 'append') paged.value = true
        losses.value = result; readError.value = ''
        if (!policyDirty.value && !policySaving.value) adoptPolicy(result.policy)
        emit('alerts', result.alerts)
      } catch (failure) {
        if (!disposed && version === readVersion && !isStale(failure)) {
          readError.value = '损耗读取失败，当前内容可能已过期：' + message(failure)
          emit('notice', readError.value, true)
        }
      }
    } while (pending && !disposed && !document.hidden)
  })().finally(() => { reading = null; if (!disposed) loading.value = false })
  return reading
}
async function savePolicy() {
  if (props.busy || policySaving.value) return
  policySaving.value = true
  try {
    await props.run(async () => {
      if (policyVersion.value === null) throw new Error('请先读取告警阈值。')
      const policy = {} as Omit<ManagementLossPolicy, 'version'>
      for (const { key, integer } of policyFields) {
        const value = policyDraft[key]
        if (value === null || !Number.isFinite(value) || value < 0 || integer && !Number.isSafeInteger(value)) throw new Error('阈值须为非负数字；Anlas 和次数须为安全整数。')
        policy[key] = value
      }
      const edit = editVersion; readVersion++
      const result = await props.api<{ policy: ManagementLossPolicy }>('member-management', { action: 'loss-alert-policy', expectedVersion: policyVersion.value, policy })
      if (disposed) return
      policyVersion.value = result.policy.version
      if (edit === editVersion) adoptPolicy(result.policy)
      emit('changed', '软告警阈值已保存并记录审计；调用权限不变。' + (policyDirty.value ? '后续编辑仍未保存。' : ''))
      await load()
    })
  } finally { if (!disposed) policySaving.value = false }
}
async function resetPolicy() {
  if (props.busy || policySaving.value) return
  const edit = editVersion
  await props.run(async () => {
    await load()
    if (!disposed && !readError.value && losses.value && edit === editVersion) adoptPolicy(losses.value.policy)
  })
}
async function saveOperating() {
  if (props.busy || operatingSaving.value) return
  operatingSaving.value = true
  try {
    await props.run(async () => {
      const value = amount.value, receiptId = receipt.value.trim()
      if (value === null || !Number.isFinite(value) || value <= 0 || resource.value !== 'v5' && !Number.isSafeInteger(value)) throw new Error('数量须大于 0；Anlas 须为安全整数。')
      if (!/^[A-Za-z0-9_-]{8,100}$/.test(receiptId)) throw new Error('请填写 8–100 位唯一收据标识，只含字母、数字、下划线或连字符。')
      if (linked.value && !lossEventId.value.trim()) throw new Error('请提供当前聚合差额记录 ID。')
      const result = await props.api<{ eventId: string; duplicate: boolean }>('member-management', {
        action: action.value, resource: resource.value, amount: value, receipt: receiptId,
        ...(linked.value ? { lossEventId: lossEventId.value.trim() } : {}),
        ...(register.value ? { destination: destination.value, fromUnattributed: fromUnattributed.value } : {}),
      })
      if (disposed) return
      amount.value = null; receipt.value = ''
      emit('changed', result.duplicate ? '该收据已处理，未重复入账。' : '账务操作已保存并记录收据。下一笔操作请使用新的收据标识。')
      await load()
    })
  } finally { if (!disposed) operatingSaving.value = false }
}
function useCurrentGap() { lossEventId.value = losses.value?.operating.gapEventIds[resource.value] || '' }
watch([action, resource], () => { if (anlasOnly.value && resource.value === 'v5') resource.value = 'anlasFixed'; if (linked.value) useCurrentGap(); else lossEventId.value = '' })
watch(() => props.refreshVersion, () => { void load() })
watch(() => policyDirty.value || amount.value !== null || Boolean(receipt.value || lossEventId.value), value => emit('dirty', value))
onMounted(() => { void load() })
onBeforeUnmount(() => { disposed = true; readVersion++; pending = null })
const resourceText = (value: Partial<ManagementResources>) => Object.entries(value).map(([key, amount]) => `${resourceNames[key as ManagementResource] || key} ${number(amount)}`).join(' / ') || '未提供资源金额'
const policyText = (value: ManagementLossPolicy) => policyFields.map(field => `${field.label} ${number(value[field.key])}`).join('；')
function evidence(event: ManagementLossEvent) {
  const parts: string[] = []
  if (event.upstreamCandidates?.length) parts.push('上游候选（互斥，不相加）：' + event.upstreamCandidates.map(candidate => resourceText(candidate.resources)).join(' 或 '))
  if (event.resources) parts.push('本地账务：' + resourceText(event.resources))
  if (event.resource && event.amount !== undefined) parts.push(`记录数量：${resourceNames[event.resource]} ${number(event.amount)}`)
  if (event.observedAmount !== undefined) parts.push('本次新增差额 ' + number(event.observedAmount))
  if (event.delta !== undefined) parts.push('本次变化 ' + number(event.delta))
  if (event.estimateAmountStatus) parts.push(event.estimateAmountStatus === 'quoted_candidates' ? '提交前报价候选，实耗未确认' : '金额未知，不能视为零消费')
  if (event.evidenceLevel) parts.push('证据：' + (evidenceNames[event.evidenceLevel] || event.evidenceLevel))
  if (event.refundRetained) parts.push('退款保留')
  if (event.before && event.after) parts.push(`版本 ${event.before.version} → ${event.after.version}；原阈值 ${policyText(event.before)}；新阈值 ${policyText(event.after)}`)
  if (event.receipt) parts.push('收据 ' + event.receipt)
  if (event.lossEventId) parts.push('关联差额 ' + event.lossEventId)
  return parts.join('；') || '未提供金额，不推算实耗'
}

const pagination = { pageSize: 10, showSizePicker: true, pageSizes: [10, 25, 50] }
const riskRows = computed(() => resources.map(key => ({ key, name: resourceNames[key],
  candidate24: losses.value?.risk.last24h.possible[key], candidate30: losses.value?.risk.last30d.possible[key],
  aggregate24: losses.value?.risk.aggregate.last24h[key], aggregate30: losses.value?.risk.aggregate.last30d[key],
  gap: key === 'v5' ? losses.value?.operating.v5CapacityGap : losses.value?.operating.unfunded[key],
  available: losses.value?.operating.available[key], returns: key === 'v5' ? null : losses.value?.operating.unattributedReturns[key]
})))
type RiskRow = typeof riskRows.value[number]
const riskColumns: DataTableColumns<RiskRow> = [
  { title: '资源', key: 'name', width: 140, sorter: 'default', filterOptions: resources.map(value => ({ label: resourceNames[value], value })), filter: (value, row) => row.key === value },
  ...(['candidate24', 'candidate30', 'aggregate24', 'aggregate30', 'gap', 'available', 'returns'] as const).map((key, index) => ({ key,
    title: ['候选风险 / 24h', '候选风险 / 30d', '聚合新增 / 24h', '聚合新增 / 30d', '待承担差额', '运营可用', '待归属入账'][index],
    width: 140, align: 'right' as const, sorter: (a: RiskRow, b: RiskRow) => (a[key] ?? -1) - (b[key] ?? -1), render: (row: RiskRow) => row[key] === null ? '不适用' : number(row[key]) }))
]
const eventColumns = computed<DataTableColumns<ManagementLossEvent>>(() => [
  { title: '时间', key: 'at', width: 185, sorter: 'default', render: row => date(row.at) },
  { title: '类型 / 标识', key: 'type', width: 260, sorter: 'default', filterOptions: [...new Set(history.value.map(row => row.type))].map(value => ({ label: eventNames[value] || value, value })), filter: (value, row) => row.type === value,
    render: row => h('div', [eventNames[row.type] || row.type, h('small', row.id), row.jobId ? h('small', '任务 ' + row.jobId) : null]) },
  { title: '成员 / 操作者', key: 'memberName', width: 170, sorter: 'default', render: row => row.memberName || row.memberId || row.actorId || '共享上游' },
  { title: '证据', key: 'evidence', width: 420, render: evidence }
])
</script>

<template>
  <section v-show="active" class="management-losses" aria-label="损耗与运营账务">
    <div id="lossReadStatus" class="refresh-status" role="status">{{ loading ? '正在更新损耗数据…' : losses ? '损耗数据更新 ' + date(losses.asOf) : '尚未读取损耗数据' }}</div>
    <NAlert v-if="readError" type="warning" role="status">{{ readError }}</NAlert>
    <NAlert type="info">疑似尝试风险与官方聚合差额可能重叠，分别展示，不相加为已确认消费。告警不停止调用或退款。</NAlert>
    <template v-if="losses">
      <NCard title="风险与运营余额" size="small">
        <p>近 24 小时异常尝试 {{ number(losses.risk.last24h.attempts) }} 次，其中金额未知 {{ number(losses.risk.last24h.unknownAmounts) }} 次；近 30 天 {{ number(losses.risk.last30d.attempts) }} 次，其中金额未知 {{ number(losses.risk.last30d.unknownAmounts) }} 次。</p>
        <div class="losses-scroll"><NDataTable :columns="riskColumns" :data="riskRows" :row-key="(row: RiskRow) => row.key" :pagination="pagination" :scroll-x="1120" size="small" /></div>
      </NCard>
      <NCard title="软告警阈值" size="small">
        <p id="lossPolicyHint">{{ policyDirty ? '未保存草稿' : '尚无修改' }} · 编辑版本 {{ policyVersion }} · 服务器版本 {{ losses.policy.version }}。后台读取不会覆盖草稿，0 表示任何正数达到阈值。购买 Anlas 有正风险即提示。</p>
        <NForm id="lossPolicyForm" @submit.prevent="savePolicy"><div class="losses-grid">
          <NFormItem v-for="field in policyFields" :key="field.key" :label="field.label"><NInputNumber :value="policyDraft[field.key]" :min="0" :step="field.integer ? 1 : 0.1" :input-props="{ id: 'loss-' + field.key }" @update:value="value => editPolicy(field.key, value)" /></NFormItem>
        </div><NSpace><NButton id="saveLossPolicy" attr-type="submit" type="primary" :loading="policySaving" :disabled="busy || policyVersion === null">保存阈值</NButton><NButton id="resetLossPolicy" :disabled="busy || policySaving" @click="resetPolicy">放弃草稿并读取最新阈值</NButton></NSpace></NForm>
      </NCard>
      <NCard title="运营资金与差额核对" size="small">
        <p>普通扣款等待核对：订阅 Anlas {{ number(losses.operating.pendingAnlasDebits?.anlasFixed) }}，购买 Anlas {{ number(losses.operating.pendingAnlasDebits?.anlasPurchased) }}。未知余额下降不会自动核销运营资金。</p>
        <NForm id="operatingForm" @submit.prevent="saveOperating"><div class="losses-grid">
          <NFormItem label="操作"><NSelect id="operatingAction" v-model:value="action" :options="actionOptions" :disabled="busy || operatingSaving" /></NFormItem>
          <NFormItem label="资源"><NSelect id="operatingResource" v-model:value="resource" :options="resourceOptions" :disabled="busy || operatingSaving" /></NFormItem>
          <NFormItem label="数量"><NInputNumber v-model:value="amount" :min="0" :step="resource === 'v5' ? 0.1 : 1" :input-props="{ id: 'operatingAmount' }" :disabled="busy || operatingSaving" /></NFormItem>
          <NFormItem label="唯一收据标识"><NInput v-model:value="receipt" :input-props="{ id: 'operatingReceipt' }" :disabled="busy || operatingSaving" /></NFormItem>
        </div><p>{{ hints[action] }}</p>
          <div v-if="register" class="losses-grid"><NFormItem label="入账归属"><NSelect id="operatingDestination" v-model:value="destination" :options="[{ label: '运营余额', value: 'operating' }, { label: '成员份额', value: 'members' }]" :disabled="busy || operatingSaving" /></NFormItem><NCheckbox v-model:checked="fromUnattributed" :disabled="busy || operatingSaving">已核实来自待归属入账</NCheckbox></div>
          <NFormItem v-if="linked" label="当前聚合差额记录 ID"><NInput v-model:value="lossEventId" :input-props="{ id: 'operatingLossId' }" :disabled="busy || operatingSaving" /><NButton id="useCurrentGap" :disabled="busy || operatingSaving" @click="useCurrentGap">使用当前差额</NButton></NFormItem>
          <NButton id="saveOperating" attr-type="submit" type="primary" :loading="operatingSaving" :disabled="busy || !state.ownerUserId">提交账务操作</NButton>
        </NForm>
        <p>失败时保留原收据及输入，核实后可用同一收据重试；成功后清空数量与收据，避免误重复提交。运营资金不足不影响普通调用。</p>
        <ul id="pendingFunding"><li v-for="funding in losses.operating.pendingExternalFunding" :key="funding.eventId">{{ funding.receipt }} · {{ resourceNames[funding.resource] }} 待确认 {{ number(funding.remaining) }} · 归属{{ funding.destination === 'members' ? '成员' : '运营' }}</li><li v-if="!losses.operating.pendingExternalFunding.length">暂无待确认注资</li></ul>
      </NCard>
      <NCard title="损耗证据与审计记录" size="small">
        <p class="section-caption">已加载 {{ history.length }} 条{{ nextCursor ? '，可继续读取更早记录' : '，已覆盖全部保留历史' }}。筛选与排序作用于已加载记录。{{ paged ? '后台只更新统计与告警，读取最新记录才重置历史。' : '' }}</p>
        <div class="losses-scroll"><NDataTable id="lossEvents" :columns="eventColumns" :data="history" :row-key="(row: ManagementLossEvent) => row.id" :pagination="pagination" :scroll-x="1035" size="small"><template #empty>暂无证据记录</template></NDataTable></div>
        <NSpace><NButton v-if="nextCursor" id="moreLossEvents" :disabled="loading" @click="load('append')">读取更早记录</NButton><NButton id="reloadLossEvents" :disabled="loading" @click="load('reset')">读取最新记录</NButton></NSpace>
      </NCard>
    </template>
  </section>
</template>

<style scoped>
.management-losses { display: grid; gap: 16px; min-width: 0; }
.losses-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 240px), 1fr)); gap: 0 16px; }
.losses-scroll { overflow-x: auto; margin-bottom: 12px; }
.losses-scroll table { min-width: 760px; }
.losses-scroll td { vertical-align: top; }
.loss-evidence { min-width: 280px; white-space: normal; overflow-wrap: anywhere; }
:deep(small) { display: block; overflow-wrap: anywhere; opacity: 0.7; }
</style>
