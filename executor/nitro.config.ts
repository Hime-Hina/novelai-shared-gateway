import { defineNitroConfig } from 'nitropack/config'
import { fileURLToPath } from 'node:url'

export default defineNitroConfig({
  compatibilityDate: '2026-10-17',
  preset: 'node-listener',
  serveStatic: false,
  handlers: [{ middleware: true, handler: fileURLToPath(new URL('./gateway-handler.ts', import.meta.url)).replaceAll('\\', '/') }],
  // 业务模块使用真实文件树，保留 worker、迁移和静态资源的相对路径。
  rollupConfig: { external: [/^novelai-shared-gateway(?:\/|$)/] }
})
