const gateway = process.env.GATEWAY_DEV_URL || 'http://127.0.0.1:8080'

export default defineNuxtConfig({
  compatibilityDate: '2026-10-17',
  telemetry: false,
  devtools: { enabled: false },
  routeRules: { '/account.html': { redirect: '/' } },
  nitro: {
    preset: 'node-server',
    // 开发同源分流；生产由 Caddy 分流，网页服务不持有数据库或上游凭据。
    devProxy: Object.fromEntries(['/api', '/owner', '/owner.html', '/admin', '/admin.html', '/management.js', '/members.css', '/member-events.js'].map(path =>
      [path, { target: gateway + path, changeOrigin: false, ...(/\.(html|js|css)$/.test(path) ? { ignorePath: true } : {}) }]
    ))
  },
  typescript: { strict: true, tsConfig: { compilerOptions: { allowJs: true } } },
  app: { head: { htmlAttrs: { lang: 'zh-CN' }, title: 'NovelAI 共享', meta: [{ name: 'referrer', content: 'same-origin' }], link: [{ rel: 'icon', href: 'data:,' }] } }
})
