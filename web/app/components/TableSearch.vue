<script setup lang="ts">
import { onBeforeUnmount, watch } from 'vue'
import { NInput } from 'naive-ui'

const value = defineModel<string>({ default: '' })
const props = withDefaults(defineProps<{ placeholder: string; label: string; delay?: number }>(), { delay: 0 })
const emit = defineEmits<{ search: [value: string] }>()
let timer: ReturnType<typeof setTimeout> | undefined
watch(value, text => {
  clearTimeout(timer)
  if (!props.delay || !text.trim()) emit('search', text.trim())
  else timer = setTimeout(() => emit('search', text.trim()), props.delay)
})
onBeforeUnmount(() => clearTimeout(timer))
</script>
<template>
  <NInput v-model:value="value" clearable :placeholder="placeholder" :aria-label="label" class="table-search" />
</template>
