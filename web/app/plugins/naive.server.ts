// Adapted from becem-gharbi/nuxt-naiveui, commit 3452646; MIT notice in web/THIRD_PARTY_NOTICES.md.
import { setup } from '@css-render/vue3-ssr'

export default defineNuxtPlugin((nuxtApp) => {
  const { collect } = setup(nuxtApp.vueApp)
  // Nuxt 4 从 head 收集 SSR 样式；保留 cssr-id，使浏览器接续时不重复插入。
  nuxtApp.ssrContext!.head.push({
    style: () => collect().split('</style>').filter(block => block.trim()).map(block => ({
      'cssr-id': /cssr-id="(.+?)"/.exec(block)?.[1],
      innerHTML: (/>(.*)/s.exec(block)?.[1] ?? '').trim()
    }))
  })
})
