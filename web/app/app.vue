<script setup lang="ts">
import { NConfigProvider, NGlobalStyle, NMessageProvider, darkTheme, zhCN, dateZhCN } from 'naive-ui'
</script>

<template>
  <NConfigProvider :theme="darkTheme" :locale="zhCN" :date-locale="dateZhCN" :theme-overrides="{ common: { primaryColor: '#8ebfff', primaryColorHover: '#b2d2ff', primaryColorPressed: '#6fa9f4', borderRadius: '8px', bodyColor: '#101318', cardColor: '#171c24', tableColor: '#171c24' }, Card: { paddingSmall: '20px', titleFontSizeSmall: '16px' }, DataTable: { thColor: '#1b2230', tdColor: '#171c24', tdColorHover: '#202a38', borderColor: '#2a3444' } }">
    <NGlobalStyle />
    <NMessageProvider placement="top" :max="3" :container-style="{ top: '20px' }"><NuxtPage /></NMessageProvider>
  </NConfigProvider>
</template>

<style>
html { scrollbar-gutter: stable; color-scheme: dark; --text-muted: #9baabd; }
body { margin: 0; background: #101318; color: #e5eaf2; font-family: system-ui, -apple-system, 'Segoe UI', 'Microsoft YaHei', sans-serif; }
* { box-sizing: border-box; }
a { color: #a3caff; }
button, input { font: inherit; }
h2 { font-size: 19px; } h3 { font-size: 16px; } h2, h3 { margin: 0; }
.section-toolbar { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px; }
.toolbar-controls { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.section-caption { color: var(--text-muted); font-size: 12px; line-height: 1.7; margin: 6px 0; }
.refresh-status { height: 30px; line-height: 30px; font-size: 12px; color: var(--text-muted); overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.count-badge { display: inline-block; font-size: 12px; font-weight: 400; color: #9fbfe7; padding: 2px 8px; border-radius: 6px; background: #8ebfff12; vertical-align: middle; }
.table-search { width: 280px; max-width: 100%; }
.n-data-table { font-variant-numeric: tabular-nums; }
.n-message-container .n-message { max-width: min(420px, calc(100vw - 32px)); font-size: 13px; line-height: 1.6; }
.n-message-container .n-message__content { overflow-wrap: anywhere; }
@media (max-width: 600px) {
  .section-toolbar { align-items: flex-start; }
  .table-search { width: 210px; }
  .n-data-table .n-pagination { flex-wrap: wrap; max-width: 100%; row-gap: 8px; justify-content: flex-start; }
}
</style>
