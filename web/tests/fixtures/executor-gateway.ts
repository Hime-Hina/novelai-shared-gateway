import { forwardExecutor } from '../../server/utils/executor-client'

// Only the integration build registers this handler; production routes stay unchanged.
export default defineEventHandler(forwardExecutor)
