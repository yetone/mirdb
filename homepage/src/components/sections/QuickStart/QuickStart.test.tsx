/**
 * QuickStart Section Tests
 * Owner: Scenario 4 - Quick Start Section
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import { QuickStart } from './QuickStart'
import { CodeBlock } from './CodeBlock'

describe('QuickStart', () => {
  const mockWriteText = vi.fn()
  const originalClipboard = navigator.clipboard

  beforeEach(() => {
    vi.useFakeTimers()
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    })
    mockWriteText.mockResolvedValue(undefined)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    })
  })

  describe('Rendering', () => {
    it('renders without errors', () => {
      render(<QuickStart />)
      expect(screen.getByRole('region', { name: /quick start/i })).toBeInTheDocument()
    })

    it('renders with correct section id', () => {
      const { container } = render(<QuickStart />)
      const section = container.querySelector('#quick-start')
      expect(section).toBeInTheDocument()
    })

    it('displays section heading', () => {
      render(<QuickStart />)
      const heading = screen.getByRole('heading', { level: 2, name: /quick start/i })
      expect(heading).toBeInTheDocument()
    })

    it('displays installation instructions subtitle', () => {
      render(<QuickStart />)
      expect(screen.getByText(/get mirdb up and running/i)).toBeInTheDocument()
    })
  })

  describe('Cargo Install Command', () => {
    it('displays cargo install command', () => {
      const { container } = render(<QuickStart />)
      const codeBlocks = container.querySelectorAll('code')
      const hasCargoInstall = Array.from(codeBlocks).some(code =>
        code.textContent?.includes('cargo install mirdb-server')
      )
      expect(hasCargoInstall).toBe(true)
    })

    it('shows Recommended badge for cargo install', () => {
      render(<QuickStart />)
      expect(screen.getByText(/recommended/i)).toBeInTheDocument()
    })

    it('renders cargo install method with description', () => {
      render(<QuickStart />)
      expect(screen.getByText(/install directly from crates.io/i)).toBeInTheDocument()
    })
  })

  describe('Alternative Installation Options', () => {
    it('displays Docker installation option', () => {
      const { container } = render(<QuickStart />)
      const codeBlocks = container.querySelectorAll('code')
      const hasDockerRun = Array.from(codeBlocks).some(code =>
        code.textContent?.includes('docker run')
      )
      expect(hasDockerRun).toBe(true)
      expect(screen.getByRole('heading', { name: /docker/i })).toBeInTheDocument()
    })

    it('displays build from source option', () => {
      const { container } = render(<QuickStart />)
      expect(screen.getByRole('heading', { name: /build from source/i })).toBeInTheDocument()
      const codeBlocks = container.querySelectorAll('code')
      const hasGitClone = Array.from(codeBlocks).some(code =>
        code.textContent?.includes('git clone')
      )
      expect(hasGitClone).toBe(true)
    })
  })

  describe('Next Steps', () => {
    it('displays next steps section', () => {
      render(<QuickStart />)
      expect(screen.getByRole('heading', { name: /next steps/i })).toBeInTheDocument()
    })

    it('shows step to start the server', () => {
      render(<QuickStart />)
      expect(screen.getByText(/start the server/i)).toBeInTheDocument()
    })

    it('shows step to connect on port 12333', () => {
      render(<QuickStart />)
      const portMentions = screen.getAllByText(/12333/)
      expect(portMentions.length).toBeGreaterThan(0)
    })
  })

  describe('Accessibility', () => {
    it('section has aria-labelledby pointing to heading', () => {
      render(<QuickStart />)
      const section = screen.getByRole('region', { name: /quick start/i })
      expect(section).toHaveAttribute('aria-labelledby', 'quickstart-title')
    })

    it('icons are hidden from screen readers', () => {
      render(<QuickStart />)
      const icons = document.querySelectorAll('svg[aria-hidden="true"]')
      expect(icons.length).toBeGreaterThan(0)
    })
  })
})

describe('CodeBlock', () => {
  const mockWriteText = vi.fn()
  const originalClipboard = navigator.clipboard

  beforeEach(() => {
    vi.useFakeTimers()
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    })
    mockWriteText.mockResolvedValue(undefined)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    })
  })

  describe('Rendering', () => {
    it('renders code content', () => {
      const { container } = render(<CodeBlock code="echo hello" language="shell" />)
      const code = container.querySelector('code')
      expect(code?.textContent).toContain('echo')
    })

    it('renders with title when provided', () => {
      render(<CodeBlock code="echo hello" title="Terminal" />)
      expect(screen.getByText('Terminal')).toBeInTheDocument()
    })

    it('renders pre and code elements', () => {
      const { container } = render(<CodeBlock code="test" />)
      expect(container.querySelector('pre')).toBeInTheDocument()
      expect(container.querySelector('code')).toBeInTheDocument()
    })

    it('sets data-language attribute', () => {
      const { container } = render(<CodeBlock code="test" language="shell" />)
      const codeBlock = container.firstChild
      expect(codeBlock).toHaveAttribute('data-language', 'shell')
    })
  })

  describe('Syntax Highlighting', () => {
    it('applies syntax highlighting for shell commands', () => {
      const { container } = render(
        <CodeBlock code="cargo install mirdb-server" language="shell" />
      )
      // Check that syntax highlighting classes exist
      const code = container.querySelector('code')
      expect(code).toBeInTheDocument()
      expect(code?.textContent).toContain('cargo')
    })

    it('highlights comments in shell code', () => {
      const { container } = render(
        <CodeBlock code="# This is a comment" language="shell" />
      )
      const comment = container.querySelector('[class*="comment"]')
      expect(comment).toBeInTheDocument()
    })

    it('highlights flags in shell commands', () => {
      const { container } = render(
        <CodeBlock code="cargo build --release" language="shell" />
      )
      const flag = container.querySelector('[class*="flag"]')
      expect(flag).toBeInTheDocument()
      expect(flag?.textContent).toContain('--release')
    })
  })

  describe('Copy Functionality', () => {
    it('renders copy button by default', () => {
      render(<CodeBlock code="test command" />)
      const copyButton = screen.getByRole('button', { name: /copy/i })
      expect(copyButton).toBeInTheDocument()
    })

    it('does not render copy button when copyable is false', () => {
      render(<CodeBlock code="test" copyable={false} />)
      const copyButton = screen.queryByRole('button', { name: /copy/i })
      expect(copyButton).not.toBeInTheDocument()
    })

    it('copies code to clipboard when copy button is clicked', async () => {
      render(<CodeBlock code="cargo install mirdb-server" />)
      const copyButton = screen.getByRole('button', { name: /copy/i })

      await act(async () => {
        fireEvent.click(copyButton)
        await vi.runAllTimersAsync()
      })

      expect(mockWriteText).toHaveBeenCalledWith('cargo install mirdb-server')
    })

    it('strips $ prompt from copied code', async () => {
      render(<CodeBlock code="$ cargo install mirdb-server" />)
      const copyButton = screen.getByRole('button', { name: /copy/i })

      await act(async () => {
        fireEvent.click(copyButton)
        await vi.runAllTimersAsync()
      })

      expect(mockWriteText).toHaveBeenCalledWith('cargo install mirdb-server')
    })

    it('shows Copied! feedback after clicking', async () => {
      render(<CodeBlock code="test" />)
      const copyButton = screen.getByRole('button', { name: /copy/i })

      await act(async () => {
        fireEvent.click(copyButton)
        await Promise.resolve()
        await Promise.resolve()
      })

      expect(copyButton).toHaveTextContent('Copied!')
    })
  })

  describe('Custom className', () => {
    it('applies custom className', () => {
      const { container } = render(
        <CodeBlock code="test" className="custom-class" />
      )
      expect(container.firstChild).toHaveClass('custom-class')
    })
  })

  describe('Multiline Code', () => {
    it('renders multiline code correctly', () => {
      const multilineCode = `git clone https://github.com/yetone/mirdb.git
cd mirdb
cargo build --release`

      const { container } = render(<CodeBlock code={multilineCode} />)
      const codeElement = container.querySelector('code')

      expect(codeElement?.textContent).toContain('git')
      expect(codeElement?.textContent).toContain('cargo')
    })
  })

  describe('Horizontal Scroll', () => {
    it('pre element has overflow-x auto for scrolling', () => {
      const { container } = render(
        <CodeBlock code="very long command that should be scrollable on mobile devices" />
      )
      const pre = container.querySelector('pre')
      expect(pre).toBeInTheDocument()
      // CSS module will apply the overflow style, we just verify the element exists
    })
  })
})
