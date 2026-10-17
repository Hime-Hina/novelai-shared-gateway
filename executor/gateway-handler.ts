import { fromNodeMiddleware } from 'h3'
import { handleGatewayRequest } from 'novelai-shared-gateway/server/gateway-runtime.js'
import { protectExecutorHandler } from './executor-auth.mjs'

export default fromNodeMiddleware(
  protectExecutorHandler(handleGatewayRequest, process.env.EXECUTOR_TOKEN)
)
