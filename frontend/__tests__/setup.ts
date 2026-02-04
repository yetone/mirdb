/**
 * Test setup and utilities.
 *
 * Global test configuration and helper utilities.
 *
 * Includes:
 * - Testing library cleanup
 * - Mock providers (Theme, Auth)
 * - Custom render utilities
 * - Common test fixtures
 */
import '@testing-library/jest-dom'
import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'

// Cleanup after each test
afterEach(() => {
  cleanup()
})
