/**
 * Shared test utilities and render helpers.
 * Owner: First builder (shared resource)
 */

import '@testing-library/jest-dom'
import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'

afterEach(() => {
  cleanup()
})

export * from '@testing-library/react'
