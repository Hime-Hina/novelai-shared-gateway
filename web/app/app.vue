<script setup lang="ts">
import { NConfigProvider, NGlobalStyle, NMessageProvider, darkTheme, zhCN, dateZhCN } from 'naive-ui'
</script>

<template>
  <NConfigProvider :theme="darkTheme" :locale="zhCN" :date-locale="dateZhCN" :theme-overrides="{ common: { primaryColor: '#8ebfff', primaryColorHover: '#b2d2ff', primaryColorPressed: '#6fa9f4', borderRadius: '10px' } }">
    <NGlobalStyle />
    <NMessageProvider><NuxtPage /></NMessageProvider>
  </NConfigProvider>
</template>

<style>
html { scrollbar-gutter: stable; }
body { margin: 0; font-family: system-ui, -apple-system, 'Segoe UI', 'Microsoft YaHei', sans-serif; }
* { box-sizing: border-box; }
a { color: #a3caff; }
button, input { font: inherit; }
</style>
