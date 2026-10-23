<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { NButton, NCard, NCheckbox, NForm, NFormItem, NInputNumber, NSpace } from 'naive-ui'
import type { FreeImagePolicy } from '../types/member'
import type { ManagementApi, ManagementRun } from '../types/management'

const props = defineProps<{ policy?: FreeImagePolicy; api: ManagementApi; run: ManagementRun; busy: boolean }>()
const emit = defineEmits<{ changed: [message: string] }>()
const draft = ref<FreeImagePolicy | null>(null), dirty = ref(false), saving = ref(false)
let active = true
onBeforeUnmount(() => { active = false })
watch(() => props.policy, value => { if (value && !dirty.value && !saving.value) draft.value = { ...value } }, { immediate: true })
function reset() { if (props.policy) draft.value = { ...props.policy }; dirty.value = false }
async function save() {
  if (props.busy || saving.value || !draft.value) return
  const { version, ...policy } = draft.value
  saving.value = true
  try {
    await props.run(async () => {
      const result = await props.api<{ policy: FreeImagePolicy }>('member-management', { action: 'free-image-policy', expectedVersion: version, policy })
      if (!active) return
      draft.value = { ...result.policy }; dirty.value = false
      emit('changed', '免费图片策略已保存；滚动窗口用量不会因修改策略而清零。')
    })
  } finally { saving.value = false }
}
</script>

<template>
  <NCard v-if="draft" title="免费图片用量策略" size="small">
    <p>按每个成员最近 24 小时的成功图片计数，在途生成占用名额。缓存和预览帧不计数，完整迟到图片计一次。留空表示不设置对应阈值。</p>
    <NForm id="freeImagePolicyForm" inline @submit.prevent="save">
      <NFormItem label="策略"><NCheckbox v-model:checked="draft.enabled" :disabled="busy || saving" @update:checked="dirty = true">启用</NCheckbox></NFormItem>
      <NFormItem label="提醒阈值（张）"><NInputNumber v-model:value="draft.warningThreshold" :min="1" :precision="0" :disabled="busy || saving" :input-props="{ id: 'freeImageWarning' }" @update:value="dirty = true" /></NFormItem>
      <NFormItem label="硬上限（张）"><NInputNumber v-model:value="draft.hardLimit" :min="1" :precision="0" :disabled="busy || saving" :input-props="{ id: 'freeImageLimit' }" @update:value="dirty = true" /></NFormItem>
      <NSpace><NButton attr-type="submit" :disabled="busy || saving || !dirty">保存策略</NButton><NButton :disabled="busy || saving || !dirty" @click="reset">撤销修改</NButton></NSpace>
    </NForm>
    <p v-if="dirty">有未保存修改 · 编辑版本 {{ draft.version }} · 当前版本 {{ policy?.version }}</p>
  </NCard>
</template>
