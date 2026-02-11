/**
 * Unit tests for CodeBlock component.
 * Owner: Scenario 5 - Quick Start Section
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { CodeBlock } from '../../../../src/components/ui/CodeBlock'

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn().mockResolvedValue(undefined),
}

beforeEach(() => {
  vi.clearAllMocks()
  Object.assign(navigator, { clipboard: mockClipboard })
})

describe('CodeBlock Component', () => {
  it('should render code content', () => {
    const code = 'npm install test-package'
    render(<CodeBlock code={code} />)

    expect(screen.getByText(code)).toBeInTheDocument()
  })

  it('should render with title when provided', () => {
    render(<CodeBlock code="echo hello" title="Example Command" />)

    expect(screen.getByText('Example Command')).toBeInTheDocument()
  })

  it('should render with language class', () => {
    render(<CodeBlock code="console.log('test')" language="javascript" />)

    const codeElement = document.querySelector('.code-block__code')
    expect(codeElement).toHaveClass('language-javascript')
  })

  it('should default to bash language', () => {
    render(<CodeBlock code="ls -la" />)

    const codeElement = document.querySelector('.code-block__code')
    expect(codeElement).toHaveClass('language-bash')
  })

  it('should render copy button with correct aria-label', () => {
    render(<CodeBlock code="echo test" />)

    const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
    expect(copyButton).toBeInTheDocument()
    expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard')
  })

  it('should use monospace font for code', () => {
    render(<CodeBlock code="test code" />)

    const codeElement = document.querySelector('.code-block__code')
    expect(codeElement).toBeInTheDocument()
    expect(codeElement).toHaveClass('code-block__code')
  })

  it('should render pre element for proper code formatting', () => {
    render(<CodeBlock code="multi\nline\ncode" />)

    const preElement = document.querySelector('.code-block__pre')
    expect(preElement).toBeInTheDocument()
  })

  it('should show language label when title is provided', () => {
    render(<CodeBlock code="test" language="bash" title="Install" />)

    expect(screen.getByText('bash')).toBeInTheDocument()
  })

  it('should copy code to clipboard when copy button is clicked', async () => {
    const code = 'cargo install test'
    render(<CodeBlock code={code} />)

    const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
    fireEvent.click(copyButton)

    await waitFor(() => {
      expect(mockClipboard.writeText).toHaveBeenCalledWith(code)
    })
  })

  it('should show "Copied!" feedback after successful copy', async () => {
    render(<CodeBlock code="test code" />)

    const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
    fireEvent.click(copyButton)

    await waitFor(() => {
      expect(screen.getByText('Copied!')).toBeInTheDocument()
    })
  })

  it('should update aria-label after successful copy', async () => {
    render(<CodeBlock code="test code" />)

    const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
    fireEvent.click(copyButton)

    await waitFor(() => {
      expect(copyButton).toHaveAttribute('aria-label', 'Copied to clipboard')
    })
  })

  it('should have copy button accessible via keyboard focus', () => {
    render(<CodeBlock code="test" />)

    const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
    copyButton.focus()

    expect(document.activeElement).toBe(copyButton)
  })

  it('should accept custom className', () => {
    render(<CodeBlock code="test" className="custom-class" />)

    const codeBlock = document.querySelector('.code-block')
    expect(codeBlock).toHaveClass('custom-class')
  })
})
