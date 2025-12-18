import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import QuickStartSection from './QuickStartSection'

describe('QuickStartSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // Test Case 1: Code snippet container is displayed with visible code block
  it('displays code snippet container with visible code block', () => {
    render(<QuickStartSection />)

    // Check for the code block container
    const codeBlock = screen.getByTestId('code-block')
    expect(codeBlock).toBeInTheDocument()
    expect(codeBlock).toBeVisible()

    // Verify the code content is present
    const codeContent = screen.getByRole('code')
    expect(codeContent).toBeInTheDocument()
  })

  // Test Case 2: Code snippet contains 'mirdb' or server start command
  it('contains mirdb or server start command in code snippet', () => {
    render(<QuickStartSection />)

    const codeBlock = screen.getByTestId('code-block')
    const codeText = codeBlock.textContent?.toLowerCase() || ''

    // Check for mirdb reference or server start command
    expect(
      codeText.includes('mirdb') ||
      codeText.includes('server') ||
      codeText.includes('start')
    ).toBe(true)
  })

  // Test Case 3: Code snippet contains 'set' command example
  it('contains set command example in code snippet', () => {
    render(<QuickStartSection />)

    const codeBlock = screen.getByTestId('code-block')
    const codeText = codeBlock.textContent?.toLowerCase() || ''

    expect(codeText).toContain('set')
  })

  // Test Case 4: Code snippet contains 'get' command example
  it('contains get command example in code snippet', () => {
    render(<QuickStartSection />)

    const codeBlock = screen.getByTestId('code-block')
    const codeText = codeBlock.textContent?.toLowerCase() || ''

    expect(codeText).toContain('get')
  })

  // Test Case 5: Code block has syntax highlighting applied
  it('has syntax highlighting applied to code block', () => {
    render(<QuickStartSection />)

    const codeBlock = screen.getByTestId('code-block')

    // Check for syntax highlighting CSS classes or colored text elements
    const hasHighlightingClass =
      codeBlock.classList.contains('syntax-highlight') ||
      codeBlock.classList.contains('code-highlight') ||
      codeBlock.classList.contains('highlighted') ||
      codeBlock.querySelector('[class*="highlight"]') !== null ||
      codeBlock.querySelector('[class*="syntax"]') !== null ||
      codeBlock.querySelector('[class*="token"]') !== null ||
      codeBlock.querySelector('.comment') !== null ||
      codeBlock.querySelector('.command') !== null ||
      codeBlock.querySelector('.string') !== null

    expect(hasHighlightingClass).toBe(true)
  })

  // Test Case 6: Copy-to-clipboard button is visible near the code block
  it('displays copy-to-clipboard button near code block', () => {
    render(<QuickStartSection />)

    // Look for copy button
    const copyButton = screen.getByRole('button', { name: /copy/i })
    expect(copyButton).toBeInTheDocument()
    expect(copyButton).toBeVisible()
  })

  // Test Case 7: Code content is copied to system clipboard on button click
  it('copies code content to system clipboard when copy button is clicked', async () => {
    const user = userEvent.setup()
    const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText')

    render(<QuickStartSection />)

    const copyButton = screen.getByRole('button', { name: /copy/i })
    await user.click(copyButton)

    // Verify clipboard.writeText was called
    expect(writeTextSpy).toHaveBeenCalled()

    // Verify the content includes expected code
    const calledWith = writeTextSpy.mock.calls[0][0].toLowerCase()
    expect(calledWith).toContain('mirdb')
  })

  // Test Case 8: Visual feedback is shown after clicking copy button
  it('shows visual feedback after clicking copy button', async () => {
    const user = userEvent.setup()

    render(<QuickStartSection />)

    const copyButton = screen.getByRole('button', { name: /copy/i })
    await user.click(copyButton)

    // Wait for visual feedback (button text changes to "Copied!" or shows checkmark)
    await waitFor(() => {
      const buttonText = copyButton.textContent?.toLowerCase() || ''
      const hasCheckmark = copyButton.querySelector('[data-testid="check-icon"]') !== null ||
                          copyButton.querySelector('svg') !== null

      expect(
        buttonText.includes('copied') ||
        hasCheckmark ||
        copyButton.getAttribute('data-copied') === 'true'
      ).toBe(true)
    })
  })
})
