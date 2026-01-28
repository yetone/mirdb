/**
 * Test Setup Configuration.
 *
 * Configures testing environment with:
 * - @testing-library/react setup
 * - Mock providers (AuthContext, ThemeContext)
 * - Custom render utilities
 * - Global test utilities
 */

import '@testing-library/jest-dom'
import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'

// Cleanup after each test
afterEach(() => {
  cleanup()
})
