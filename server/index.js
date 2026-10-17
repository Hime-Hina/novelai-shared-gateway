import { handleGatewayRequest, startGateway } from './gateway-runtime.js';

await startGateway(handleGatewayRequest);
