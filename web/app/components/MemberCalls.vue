<script setup lang="ts">
import { computed } from 'vue'
import { NAlert, NCard, NTable } from 'naive-ui'
import MemberBatch from './MemberBatch.vue'
import type { LateResult, MemberApi, MemberJob } from '../types/member'
import { chargeText, date, jobStatus, resultUrl } from '../utils/member-display'

const props = defineProps<{ jobs: MemberJob[]; lateResults: LateResult[]; api: MemberApi; refreshVersion: number }>()
const emit = defineEmits<{ changed: [] }>()
const jobs = computed(() => props.jobs.slice(0, 30))
const requests = computed(() => [...new Set(jobs.value.map(job => job.requestId).filter((id): id is string => Boolean(id)))])
const lateResults = computed(() => props.lateResults.filter(job => resultUrl(job.imageUrl)).slice(0, 10))
</script>

<template>
  <section v-if="lateResults.length" id="lateResultsPanel" aria-labelledby="lateResultsTitle">
    <NAlert type="info">
      <h2 id="lateResultsTitle">有迟到结果可以领取</h2>
      <p>这些任务结束后找回了结果。退款保留，领取不另扣费。以下显示最近找回的最多 10 条，领取链接会随页面刷新更新。</p>
      <ul id="lateResults">
        <li v-for="job in lateResults" :key="job.id">
          {{ job.model }} · 提交 {{ date(job.createdAt) }} · 找回 {{ date(job.lateResultAt) }} · {{ chargeText(job.charge) }} ·
          <a :href="resultUrl(job.imageUrl) || undefined" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer">领取结果</a>
        </li>
      </ul>
    </NAlert>
  </section>
  <NCard title="最近 30 条调用" size="small">
    <div class="calls-scroll" role="region" aria-label="最近调用记录，可横向滚动" tabindex="0">
      <NTable :single-line="false" class="calls-table">
        <thead><tr><th scope="col">时间</th><th scope="col">模型</th><th scope="col">状态</th><th scope="col">额度记录</th></tr></thead>
        <tbody id="jobs">
          <tr v-for="job in jobs" :key="job.id">
            <td>{{ date(job.createdAt) }}</td>
            <td>{{ job.model }}</td>
            <td>
              {{ jobStatus(job.status) }}
              <p v-if="resultUrl(job.imageUrl)"><a :href="resultUrl(job.imageUrl) || undefined" target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer">{{ job.lateResultAvailable ? '领取迟到结果（退款保留）' : '领取结果' }}</a></p>
            </td>
            <td>{{ chargeText(job.charge) }}</td>
          </tr>
          <tr v-if="!jobs.length"><td colspan="4">暂无调用记录</td></tr>
        </tbody>
      </NTable>
    </div>
    <div v-if="requests.length" class="request-list">
      <h3>相关批次</h3>
      <MemberBatch v-for="requestId in requests" :key="requestId" :request-id="requestId" :api="api" :refresh-version="refreshVersion" @changed="emit('changed')" />
    </div>
    <p class="calls-note">“已退款”指本站额度已退回，不代表上游已退款。成员资格、分组和额度分配由管理员管理。所有成员共用本 Opus 的队列，同一时间生成一张，结束后至少间隔 10 秒再发下一张；其他请求等待，官方限流时自动退避。</p>
  </NCard>
</template>

<style scoped>
#lateResultsPanel { margin-bottom: 16px; overflow-wrap: anywhere; }
#lateResultsPanel h2 { margin-top: 0; font-size: 18px; }
#lateResults li + li { margin-top: 10px; }
.calls-scroll { max-width: 100%; overflow-x: auto; }
.calls-table { min-width: 640px; }
.calls-table td { overflow-wrap: anywhere; }
.calls-table p { margin: 4px 0; }
.request-list { display: grid; gap: 12px; min-width: 0; margin-top: 20px; }
.request-list h3 { margin: 0; }
.calls-note { color: #a9b7cb; margin-bottom: 0; }
</style>
