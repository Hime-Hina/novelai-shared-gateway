import { defineNitroConfig } from 'nitropack/config';
import { fileURLToPath } from 'node:url';

export default defineNitroConfig({
  compatibilityDate: '2026-10-17',
  preset: 'node-listener',
  serveStatic: false,
  handlers: [
    {
      middleware: true,
      handler: fileURLToPath(new URL('./gateway-handler.ts', import.meta.url)).replaceAll(
        '\\',
        '/',
      ),
    },
  ],
  // 业务模块复用后端发布包的真实 JavaScript 文件树，不交给 Nitro 另行打包。
  rollupConfig: { external: [/^novelai-shared-gateway(?:\/|$)/] },
});
