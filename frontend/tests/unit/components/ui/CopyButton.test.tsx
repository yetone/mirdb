/**
 * Unit Tests for CopyButton Component
 * Owner: Scenario 5 - Copy to Clipboard Functionality
 *
 * Tests:
 * 1. Button displays "Copy" text by default
 * 2. Clicking calls clipboard API with correct text
 * 3. Button text changes to "Copied!" after successful copy
 * 4. Button text reverts to "Copy" after 2 seconds
 * 5. Error callback is invoked when clipboard API fails
 */

import React from 'react'
import { render, screen, act, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { CopyButton } from '@/components/ui/CopyButton'

describe('CopyButton', () => {
  const mockWriteText = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()

    // Mock clipboard API at the global level
    Object.defineProperty(global.navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    })
  })

  afterEach(() => {
    vi.runOnlyPendingTimers()
    vi.useRealTimers()
  })

  it('should display "Copy" text by default', () => {
    render(<CopyButton text="http://localhost/r/abc123" />)

    expect(screen.getByRole('button', { name: /copy to clipboard/i })).toBeInTheDocument()
    expect(screen.getByText('Copy')).toBeInTheDocument()
  })

  it('should call navigator.clipboard.writeText when clicked', async () => {
    const shortUrl = 'http://localhost/r/abc123'
    mockWriteText.mockResolvedValueOnce(undefined)

    render(<CopyButton text={shortUrl} />)

    const button = screen.getByRole('button', { name: /copy to clipboard/i })

    await act(async () => {
      fireEvent.click(button)
    })

    expect(mockWriteText).toHaveBeenCalledTimes(1)
    expect(mockWriteText).toHaveBeenCalledWith(shortUrl)
  })

  it('should change button text from "Copy" to "Copied!" after successful copy', async () => {
    mockWriteText.mockResolvedValueOnce(undefined)

    render(<CopyButton text="http://localhost/r/abc123" />)

    const button = screen.getByRole('button')
    expect(button).toHaveTextContent('Copy')

    await act(async () => {
      fireEvent.click(button)
    })

    expect(screen.getByText('Copied!')).toBeInTheDocument()
  })

  it('should revert button text to "Copy" after 2 seconds', async () => {
    mockWriteText.mockResolvedValueOnce(undefined)

    render(<CopyButton text="http://localhost/r/abc123" />)

    const button = screen.getByRole('button')

    await act(async () => {
      fireEvent.click(button)
    })

    expect(screen.getByText('Copied!')).toBeInTheDocument()

    // Advance timer by 2 seconds
    await act(async () => {
      vi.advanceTimersByTime(2000)
    })

    expect(screen.getByText('Copy')).toBeInTheDocument()
  })

  it('should call onError callback when clipboard API fails', async () => {
    const errorMessage = 'Permission denied'
    const onError = vi.fn()
    mockWriteText.mockRejectedValueOnce(new Error(errorMessage))

    render(<CopyButton text="http://localhost/r/abc123" onError={onError} />)

    const button = screen.getByRole('button')

    await act(async () => {
      fireEvent.click(button)
    })

    expect(onError).toHaveBeenCalledTimes(1)
    expect(onError).toHaveBeenCalledWith(expect.any(Error))
    // Button should still show "Copy" (not "Copied!")
    expect(screen.getByText('Copy')).toBeInTheDocument()
  })

  it('should call onCopy callback on successful copy', async () => {
    const onCopy = vi.fn()
    mockWriteText.mockResolvedValueOnce(undefined)

    render(<CopyButton text="http://localhost/r/abc123" onCopy={onCopy} />)

    const button = screen.getByRole('button')

    await act(async () => {
      fireEvent.click(button)
    })

    expect(onCopy).toHaveBeenCalledTimes(1)
  })

  it('should apply custom className', () => {
    render(<CopyButton text="test" className="custom-class" />)

    const button = screen.getByRole('button')
    expect(button).toHaveClass('custom-class')
  })

  it('should have appropriate aria-label for accessibility', async () => {
    mockWriteText.mockResolvedValueOnce(undefined)

    render(<CopyButton text="test" />)

    const button = screen.getByRole('button')
    expect(button).toHaveAttribute('aria-label', 'Copy to clipboard')

    await act(async () => {
      fireEvent.click(button)
    })

    expect(button).toHaveAttribute('aria-label', 'Copied to clipboard')
  })

  it('should apply success button style when copied', async () => {
    mockWriteText.mockResolvedValueOnce(undefined)

    render(<CopyButton text="test" />)

    const button = screen.getByRole('button')
    expect(button).toHaveClass('btn-primary')
    expect(button).not.toHaveClass('btn-success')

    await act(async () => {
      fireEvent.click(button)
    })

    expect(button).toHaveClass('btn-success')
    expect(button).not.toHaveClass('btn-primary')
  })
})
