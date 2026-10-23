import { isAbsolute } from 'node:path';

// 打包位置不应决定业务数据位置，避免升级时意外初始化另一份账本。
if (!process.env.DATA_DIR || !isAbsolute(process.env.DATA_DIR)) {
  throw new Error('Executor requires an absolute DATA_DIR.');
}

// 必须先设置 HOST，再加载会读取环境变量的业务模块。
process.env.HOST ||= '127.0.0.1';

const { listener } = await import('./index.mjs');
const { startGateway } = await import('novelai-shared-gateway/server/gateway-runtime.js');

await startGateway(listener);
