/**
 * Clipboard Utility Unit Tests
 * Owner: Scenario 3 - Interactive Demo Section
 *
 * Test coverage:
 * - copyToClipboard with modern Clipboard API
 * - copyToClipboard fallback when Clipboard API unavailable
 * - Error handling scenarios
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { copyToClipboard } from '../../../src/utils/clipboard.js'

describe('copyToClipboard', () => {
  let originalNavigator
  let originalExecCommand

  beforeEach(() => {
    // Store original navigator
    originalNavigator = { ...navigator }
    originalExecCommand = document.execCommand

    // Reset DOM
    document.body.innerHTML = ''
  })

  afterEach(() => {
    // Restore mocks
    vi.restoreAllMocks()

    // Clean up DOM
    document.body.innerHTML = ''
  })

  describe('Modern Clipboard API', () => {
    it('should copy text using Clipboard API when available', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)

      // Mock navigator.clipboard
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const result = await copyToClipboard('test text')

      expect(mockWriteText).toHaveBeenCalledWith('test text')
      expect(result).toBe(true)
    })

    it('should return false when Clipboard API fails and fallback also fails', async () => {
      const mockWriteText = vi.fn().mockRejectedValue(new Error('Permission denied'))

      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      // Mock execCommand to fail
      document.execCommand = vi.fn().mockReturnValue(false)

      const result = await copyToClipboard('test text')

      expect(mockWriteText).toHaveBeenCalledWith('test text')
      expect(result).toBe(false)
    })

    it('should fall back when Clipboard API throws error', async () => {
      const mockWriteText = vi.fn().mockRejectedValue(new Error('Not allowed'))

      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      // Mock successful execCommand
      document.execCommand = vi.fn().mockReturnValue(true)

      const result = await copyToClipboard('fallback text')

      expect(result).toBe(true)
      expect(document.execCommand).toHaveBeenCalledWith('copy')
    })
  })

  describe('Fallback execCommand', () => {
    beforeEach(() => {
      // Remove Clipboard API
      Object.defineProperty(navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true
      })
    })

    it('should use fallback when Clipboard API is not available', async () => {
      document.execCommand = vi.fn().mockReturnValue(true)

      const result = await copyToClipboard('fallback text')

      expect(result).toBe(true)
      expect(document.execCommand).toHaveBeenCalledWith('copy')
    })

    it('should return false when execCommand fails', async () => {
      document.execCommand = vi.fn().mockReturnValue(false)

      const result = await copyToClipboard('will fail')

      expect(result).toBe(false)
    })

    it('should return false when execCommand throws exception', async () => {
      document.execCommand = vi.fn().mockImplementation(() => {
        throw new Error('execCommand not supported')
      })

      const result = await copyToClipboard('exception text')

      expect(result).toBe(false)
    })

    it('should create and remove textarea element during fallback', async () => {
      document.execCommand = vi.fn().mockReturnValue(true)

      await copyToClipboard('textarea test')

      // Textarea should be cleaned up
      const textareas = document.querySelectorAll('textarea')
      expect(textareas.length).toBe(0)
    })
  })

  describe('Edge cases', () => {
    it('should handle empty string', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)

      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const result = await copyToClipboard('')

      expect(mockWriteText).toHaveBeenCalledWith('')
      expect(result).toBe(true)
    })

    it('should handle special characters', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)

      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const specialText = 'set mykey 0 0 5\r\nmyval\r\n'
      const result = await copyToClipboard(specialText)

      expect(mockWriteText).toHaveBeenCalledWith(specialText)
      expect(result).toBe(true)
    })

    it('should handle unicode characters', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)

      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true
      })

      const unicodeText = '测试 テスト 🚀'
      const result = await copyToClipboard(unicodeText)

      expect(mockWriteText).toHaveBeenCalledWith(unicodeText)
      expect(result).toBe(true)
    })
  })
})
