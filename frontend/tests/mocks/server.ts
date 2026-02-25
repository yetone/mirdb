/**
 * MSW Server Setup
 * Owner: First builder to run
 *
 * Configure MSW server for test environment.
 */
import { setupServer } from 'msw/node'
import { handlers } from './handlers'

export const server = setupServer(...handlers)
