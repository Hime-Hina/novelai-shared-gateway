import { defineEventHandler } from 'h3'
import { forwardExecutorEvents } from '../../utils/executor-events'

export default defineEventHandler(forwardExecutorEvents)
