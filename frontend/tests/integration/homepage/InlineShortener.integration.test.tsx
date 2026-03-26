/**
 * Integration Tests for InlineShortener Component
 * Owner: Scenario 2 - Inline URL Shortening
 *
 * Tests:
 * - Short URL generated and displayed within 1 second (Test Case 1)
 * - Response time under 500ms (NFR-5 requirement) (Test Case 2)
 * - End-to-end URL shortening flow
 *
 * Requirements: REQ-2, REQ-7, NFR-5
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { InlineShortener } from '../../../src/components/homepage/InlineShortener'

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn().mockResolvedValue(undefined),
}

Object.defineProperty(navigator, 'clipboard', {
  value: mockClipboard,
  writable: true,
})

describe('InlineShortener Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 1: URL Shortening Performance', () => {
    it('generates and displays short URL within 1 second', async () => {
      const startTime = performance.now()

      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      // Enter a valid long URL
      fireEvent.change(input, {
        target: { value: 'https://example.com/very/long/url/path' },
      })
      fireEvent.click(button)

      // Wait for result to appear with 1 second timeout
      await waitFor(
        () => {
          expect(screen.getByTestId('result-area')).toBeInTheDocument()
          expect(screen.getByTestId('short-url')).toBeInTheDocument()
        },
        { timeout: 1000 }
      )

      const endTime = performance.now()
      const elapsed = endTime - startTime

      // Verify result appeared within 1 second
      expect(elapsed).toBeLessThan(1000)

      // Verify the short URL is properly formatted
      const shortUrlElement = screen.getByTestId('short-url')
      expect(shortUrlElement.textContent).toMatch(/\/s\/[a-zA-Z0-9]+/)
    })
  })

  describe('Test Case 2: NFR-5 Response Time Requirement', () => {
    it('completes URL shortening within 500ms', async () => {
      let completionTime: number | null = null
      const startTime = performance.now()

      const onShortenSuccess = vi.fn(() => {
        completionTime = performance.now() - startTime
      })

      render(<InlineShortener onShortenSuccess={onShortenSuccess} />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(onShortenSuccess).toHaveBeenCalled()
      })

      // Verify response time is under 500ms (NFR-5)
      expect(completionTime).toBeLessThan(500)
    })

    it('displays result within 500ms of submission', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      const startTime = performance.now()

      fireEvent.change(input, { target: { value: 'https://example.com/test' } })
      fireEvent.click(button)

      await waitFor(
        () => {
          expect(screen.getByTestId('result-area')).toBeInTheDocument()
        },
        { timeout: 500 }
      )

      const endTime = performance.now()
      expect(endTime - startTime).toBeLessThan(500)
    })
  })

  describe('Full URL Shortening Flow', () => {
    it('completes full shortening flow: input -> submit -> display -> copy', async () => {
      render(<InlineShortener />)

      // Step 1: Input URL
      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      fireEvent.change(input, {
        target: { value: 'https://example.com/very/long/url/path' },
      })
      expect(input).toHaveValue('https://example.com/very/long/url/path')

      // Step 2: Submit
      fireEvent.click(submitButton)

      // Step 3: Wait for result display
      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })

      // Verify short URL is displayed
      const shortUrl = screen.getByTestId('short-url')
      expect(shortUrl).toBeInTheDocument()
      expect(shortUrl.textContent).toContain('/s/')

      // Step 4: Copy to clipboard
      const copyButton = screen.getByTestId('copy-button')
      fireEvent.click(copyButton)

      await waitFor(() => {
        expect(mockClipboard.writeText).toHaveBeenCalledWith(
          expect.stringContaining('/s/')
        )
        expect(screen.getByTestId('copied-feedback')).toHaveTextContent('Copied!')
      })
    })

    it('handles multiple consecutive URL shortenings', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // First URL
      fireEvent.change(input, { target: { value: 'https://first-url.com' } })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })

      const firstShortUrl = screen.getByTestId('short-url').textContent

      // Click Shorten another
      const shortenAnotherButton = screen.getByTestId('shorten-another-button')
      fireEvent.click(shortenAnotherButton)

      await waitFor(() => {
        expect(screen.queryByTestId('result-area')).not.toBeInTheDocument()
      })

      // Second URL
      fireEvent.change(input, { target: { value: 'https://second-url.com' } })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })

      const secondShortUrl = screen.getByTestId('short-url').textContent

      // URLs should be different
      expect(firstShortUrl).not.toBe(secondShortUrl)
    })
  })

  describe('Error Recovery', () => {
    it('recovers from error and allows retry', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // First, submit invalid URL
      fireEvent.change(input, { target: { value: 'invalid-url' } })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByTestId('error-message')).toBeInTheDocument()
      })

      // Then, submit valid URL
      fireEvent.change(input, { target: { value: 'https://valid-url.com' } })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.queryByTestId('error-message')).not.toBeInTheDocument()
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })
    })
  })

  describe('Loading States', () => {
    it('shows loading state and disables input during submission', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(submitButton)

      // Button should show loading state
      expect(submitButton).toHaveAttribute('aria-busy', 'true')

      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })

      // Loading state should be cleared
      expect(submitButton).not.toHaveAttribute('aria-busy', 'true')
    })
  })
})
