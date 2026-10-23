import type { IncomingMessage, ServerResponse } from 'node:http'

export function handleGatewayRequest(req: IncomingMessage, res: ServerResponse): Promise<void>
export function startGateway(handler: (req: IncomingMessage, res: ServerResponse) => void | Promise<void>): Promise<void>
