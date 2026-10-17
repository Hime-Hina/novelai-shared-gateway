const gateway = process.env.GATEWAY_DEV_URL || 'http://127.0.0.1:8080'

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
    '/account.html': { redirect: '/' },
    '/owner.html': { redirect: '/owner' },
    '/admin.html': { redirect: '/admin' },
    '/admin/members.html': { redirect: '/admin' }
  },
  nitro: {
    preset: 'node-server',
    // 开发同源分流；生产由 Caddy 分流，网页服务不持有数据库或上游凭据。
    devProxy: { '/api': { target: gateway + '/api', changeOrigin: false } }
  },
  typescript: { strict: true, tsConfig: { compilerOptions: { allowJs: true } } },
  app: { head: { htmlAttrs: { lang: 'zh-CN' }, title: 'NovelAI 共享', meta: [{ name: 'referrer', content: 'same-origin' }], link: [{ rel: 'icon', href: 'data:,' }] } }
})
