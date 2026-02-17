/**
 * Unit tests for clipboard utilities.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Test cases:
 * - Clipboard API feature detection
 * - Copy to clipboard functionality
 * - Fallback behavior for older browsers
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { copyToClipboard, isClipboardSupported } from '@/utils/clipboard'

describe('clipboard utilities', () => {
  const originalNavigator = global.navigator
  const originalDocument = global.document

  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('isClipboardSupported', () => {
    it('returns true when clipboard API is available', () => {
      // The setup.ts already mocks navigator.clipboard
      expect(isClipboardSupported()).toBe(true)
    })
  })

  describe('copyToClipboard', () => {
    it('returns true when copy succeeds', async () => {
      const result = await copyToClipboard('test text')
      expect(result).toBe(true)
    })

    it('returns false for empty string', async () => {
      const result = await copyToClipboard('')
      expect(result).toBe(false)
    })

    it('calls clipboard.writeText with the correct text', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      })

      await copyToClipboard('https://urlshort.io/abc123')
      expect(mockWriteText).toHaveBeenCalledWith('https://urlshort.io/abc123')
    })

    it('handles clipboard API failure gracefully', async () => {
      const mockWriteText = vi.fn().mockRejectedValue(new Error('Permission denied'))
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      })

      // Mock document.execCommand for fallback
      const mockExecCommand = vi.fn().mockReturnValue(true)
      const mockCreateElement = vi.spyOn(document, 'createElement')
      const mockAppendChild = vi.spyOn(document.body, 'appendChild').mockImplementation(() => document.body)
      const mockRemoveChild = vi.spyOn(document.body, 'removeChild').mockImplementation(() => document.body)

      // Create a mock textarea element
      const mockTextarea = {
        value: '',
        style: {},
        focus: vi.fn(),
        select: vi.fn(),
      }
      mockCreateElement.mockReturnValue(mockTextarea as any)

      document.execCommand = mockExecCommand

      const result = await copyToClipboard('test')

      // Should still succeed via fallback
      expect(result).toBe(true)
    })

    it('copies special characters correctly', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      })

      const specialUrl = 'https://example.com/path?query=value&special=<>&unicode=🔗'
      await copyToClipboard(specialUrl)
      expect(mockWriteText).toHaveBeenCalledWith(specialUrl)
    })
  })
})
