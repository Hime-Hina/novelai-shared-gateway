import { fileURLToPath } from 'node:url'
import { defineVitestConfig } from '@nuxt/test-utils/config'

export default defineVitestConfig({
  test: {
    environment: 'nuxt',
    include: ['tests/nuxt/**/*.test.ts'],
    environmentOptions: { nuxt: { rootDir: fileURLToPath(new URL('.', import.meta.url)), domEnvironment: 'happy-dom' } }
  }
})
