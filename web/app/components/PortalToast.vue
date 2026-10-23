<script setup lang="ts">
import { onBeforeUnmount, watch } from 'vue'
import { useMessage, type MessageReactive } from 'naive-ui'

const props = withDefaults(defineProps<{ message: string; type?: 'success' | 'error' | 'warning' | 'info' }>(), { type: 'info' })
const messages = useMessage()
let current: MessageReactive | undefined
watch(() => [props.message, props.type], () => {
  current?.destroy()
  current = props.message ? messages.create(props.message, { type: props.type, duration: 6000, closable: true, keepAliveOnHover: true }) : undefined
}, { immediate: true })
onBeforeUnmount(() => current?.destroy())
</script>

<template><span class="toast-source" hidden /></template>
