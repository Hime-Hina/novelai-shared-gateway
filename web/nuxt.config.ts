import { fileURLToPath } from 'node:url'

export default defineNuxtConfig({
  compatibilityDate: '2026-10-17',
  telemetry: false,
  devtools: { enabled: false },
  runtimeConfig: { executorBaseUrl: '', executorToken: '' },
  // Naive 的 ESM 入口引用 vueuc 的 CJS 包；开发 SSR 也须由 Vite 处理这两个包。
  build: { transpile: ['naive-ui', 'vueuc'] },
  hooks: {
    'build:manifest'(manifest) {
      // 匿名页不提前下载登录后才用到的异步组件；保留当前页面的 preload。
      for (const resource of Object.values(manifest)) resource.prefetch = false
    }
  },
  routeRules: {
    '/index.html': { redirect: '/' },
    '/account.html': { redirect: '/' },
    '/owner.html': { redirect: '/owner' },
    '/admin.html': { redirect: '/admin' },
    '/admin/members.html': { redirect: '/admin' }
  },
  // 开发和产物使用同一组 API 路由，业务与持久化统一交给执行器。
  nitro: {
    preset: 'node-server',
    // 外层纯模块须同时打入开发 worker，不能留下相对生成目录的运行时 import。
    externals: { inline: ['generation-request.js', 'queue-policy.ts', 'shared-surface.ts'].map(name => fileURLToPath(new URL('../server/' + name, import.meta.url))) }
  },
  typescript: { strict: true, tsConfig: { compilerOptions: { allowJs: true } } },
  app: { head: { htmlAttrs: { lang: 'zh-CN' }, title: 'NovelAI 共享', meta: [{ name: 'referrer', content: 'same-origin' }], link: [{ rel: 'icon', href: 'data:,' }] } }
})
