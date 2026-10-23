import { handleGatewayRequest, startGateway } from './gateway-runtime.ts';

await startGateway(handleGatewayRequest);
