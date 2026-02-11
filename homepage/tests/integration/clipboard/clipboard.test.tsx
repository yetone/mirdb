/**
 * Integration tests for clipboard/copy functionality.
 * Owner: Scenario 5 - Quick Start Section
 *
 * Tests the integration between:
 * - useCopyToClipboard hook
 * - CodeBlock component
 * - QuickStart section
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QuickStart } from '../../../src/components/sections/QuickStart'
import { CodeBlock } from '../../../src/components/ui/CodeBlock'

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn().mockResolvedValue(undefined),
}

beforeEach(() => {
  vi.clearAllMocks()
  Object.assign(navigator, { clipboard: mockClipboard })
})

describe('Clipboard Integration', () => {
  describe('CodeBlock copy functionality', () => {
    it('should copy code content when copy button is clicked', async () => {
      const code = 'cargo install mirdb-server'
      render(<CodeBlock code={code} title="Installation" />)

      const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
      fireEvent.click(copyButton)

      await waitFor(() => {
        expect(mockClipboard.writeText).toHaveBeenCalledWith(code)
      })
    })

    it('should show visual feedback after copy', async () => {
      render(<CodeBlock code="test" />)

      const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })

      // Before click - shows "Copy"
      expect(screen.getByText('Copy')).toBeInTheDocument()

      fireEvent.click(copyButton)

      // After click - shows "Copied!"
      await waitFor(() => {
        expect(screen.getByText('Copied!')).toBeInTheDocument()
      })
    })

    it('should update aria-label for screen readers after copy', async () => {
      render(<CodeBlock code="test" />)

      const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })

      expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard')

      fireEvent.click(copyButton)

      await waitFor(() => {
        expect(copyButton).toHaveAttribute('aria-label', 'Copied to clipboard')
      })
    })
  })

  describe('QuickStart section copy functionality', () => {
    it('should copy installation command when its copy button is clicked', async () => {
      render(<QuickStart />)

      // Find all copy buttons
      const copyButtons = screen.getAllByRole('button', { name: /copy code to clipboard/i })

      // Click the first copy button (installation command)
      fireEvent.click(copyButtons[0])

      await waitFor(() => {
        expect(mockClipboard.writeText).toHaveBeenCalledWith('cargo install mirdb-server')
      })
    })

    it('should copy usage example when its copy button is clicked', async () => {
      render(<QuickStart />)

      // Find all copy buttons
      const copyButtons = screen.getAllByRole('button', { name: /copy code to clipboard/i })

      // Click the second copy button (usage example)
      fireEvent.click(copyButtons[1])

      await waitFor(() => {
        // Verify clipboard was called with usage content
        expect(mockClipboard.writeText).toHaveBeenCalled()
        const calledWith = mockClipboard.writeText.mock.calls[0][0]
        expect(calledWith).toContain('mirdb-server')
        expect(calledWith).toContain('telnet')
        expect(calledWith).toContain('set mykey')
        expect(calledWith).toContain('get mykey')
      })
    })

    it('should show Copied feedback on the specific button that was clicked', async () => {
      render(<QuickStart />)

      const copyButtons = screen.getAllByRole('button', { name: /copy code to clipboard/i })

      // Click first button
      fireEvent.click(copyButtons[0])

      await waitFor(() => {
        // Only the clicked button should show Copied!
        const copiedText = screen.getByText('Copied!')
        expect(copiedText).toBeInTheDocument()
      })
    })
  })

  describe('Accessibility integration', () => {
    it('should have all copy buttons focusable via keyboard', () => {
      render(<QuickStart />)

      const copyButtons = screen.getAllByRole('button', { name: /copy code to clipboard/i })

      copyButtons.forEach(button => {
        button.focus()
        expect(document.activeElement).toBe(button)
      })
    })

    it('should trigger copy on Enter key press', async () => {
      render(<CodeBlock code="test code" />)

      const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
      copyButton.focus()

      fireEvent.keyDown(copyButton, { key: 'Enter', code: 'Enter' })
      fireEvent.click(copyButton) // Simulate native behavior

      await waitFor(() => {
        expect(mockClipboard.writeText).toHaveBeenCalledWith('test code')
      })
    })
  })

  describe('Error handling', () => {
    it('should handle clipboard API failure gracefully', async () => {
      mockClipboard.writeText.mockRejectedValueOnce(new Error('Clipboard access denied'))
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      render(<CodeBlock code="test" />)

      const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
      fireEvent.click(copyButton)

      // Should not crash and should maintain initial state
      await waitFor(() => {
        expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard')
      })

      consoleSpy.mockRestore()
    })
  })
})
