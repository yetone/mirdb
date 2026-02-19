/**
 * useTheme Hook Tests
 * Owner: Scenario 6 - Dark Mode & Theme System
 *
 * Tests the re-exported useTheme hook from hooks directory
 */
import { describe, it, expect } from 'vitest'
import { useTheme } from './useTheme'
import { useTheme as contextUseTheme } from '@/context/ThemeContext'

describe('useTheme hook', () => {
  it('re-exports useTheme from ThemeContext', () => {
    // Verify it's the same function
    expect(useTheme).toBe(contextUseTheme)
  })
})
