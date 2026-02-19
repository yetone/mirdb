/**
 * CopyButton Component Tests
 * Owner: Scenario 4 - Quick Start Section
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import { CopyButton } from './CopyButton'

describe('CopyButton', () => {
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
      render(<CopyButton text="test" />)
      expect(screen.getByRole('button')).toBeInTheDocument()
    })

    it('renders with copy icon and label', () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')
      expect(button).toHaveTextContent('Copy')
      expect(button.querySelector('svg')).toBeInTheDocument()
    })

    it('has accessible label', () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button', { name: /copy to clipboard/i })
      expect(button).toBeInTheDocument()
    })

    it('uses custom aria-label when provided', () => {
      render(<CopyButton text="test" aria-label="Copy installation command" />)
      const button = screen.getByRole('button', { name: /copy installation command/i })
      expect(button).toBeInTheDocument()
    })

    it('has type="button" attribute', () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')
      expect(button).toHaveAttribute('type', 'button')
    })

    it('icon is hidden from screen readers', () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')
      const svg = button.querySelector('svg')
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })
  })

  describe('Copy functionality', () => {
    it('copies text to clipboard when clicked', async () => {
      render(<CopyButton text="cargo install mirdb-server" />)
      const button = screen.getByRole('button')

      await act(async () => {
        fireEvent.click(button)
        await vi.runAllTimersAsync()
      })

      expect(mockWriteText).toHaveBeenCalledWith('cargo install mirdb-server')
    })

    it('shows Copied! feedback after click', async () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')

      await act(async () => {
        fireEvent.click(button)
        // Just resolve the promise, don't advance timers
        await Promise.resolve()
        await Promise.resolve()
      })

      expect(button).toHaveTextContent('Copied!')
    })

    it('shows check icon after successful copy', async () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')

      await act(async () => {
        fireEvent.click(button)
        await Promise.resolve()
        await Promise.resolve()
      })

      const svg = button.querySelector('svg')
      expect(svg?.querySelector('polyline')).toBeInTheDocument()
    })

    it('reverts to Copy state after delay', async () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')

      // Click and wait for copy
      await act(async () => {
        fireEvent.click(button)
        await Promise.resolve()
        await Promise.resolve()
      })

      expect(button).toHaveTextContent('Copied!')

      // Advance timer to trigger state reset
      await act(async () => {
        vi.advanceTimersByTime(2000)
      })

      expect(button).toHaveTextContent('Copy')
    })

    it('updates title attribute when copied', async () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')

      expect(button).toHaveAttribute('title', 'Copy to clipboard')

      await act(async () => {
        fireEvent.click(button)
        await Promise.resolve()
        await Promise.resolve()
      })

      expect(button).toHaveAttribute('title', 'Copied!')
    })
  })

  describe('Styling', () => {
    it('applies custom className', () => {
      render(<CopyButton text="test" className="custom-class" />)
      const button = screen.getByRole('button')
      expect(button.className).toContain('custom-class')
    })

    it('adds copied class when copied', async () => {
      render(<CopyButton text="test" />)
      const button = screen.getByRole('button')

      await act(async () => {
        fireEvent.click(button)
        await Promise.resolve()
        await Promise.resolve()
      })

      expect(button.className).toMatch(/copied/i)
    })
  })
})
