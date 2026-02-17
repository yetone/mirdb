/**
 * Integration tests for URL shortening demo.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Tests the complete flow of URL shortening demo:
 * - End-to-end URL shortening flow
 * - Copy to clipboard integration
 * - Error handling and recovery
 * - Registration prompt interaction
 */

import React from 'react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { UrlDemoSection } from '@/components/home/UrlDemoSection'

// Mock the API module
vi.mock('@/api', () => ({
  shortenUrl: vi.fn(),
}))

import { shortenUrl } from '@/api'

const mockedShortenUrl = vi.mocked(shortenUrl)

// Wrapper component for Router context
const TestWrapper = ({ children }: { children: React.ReactNode }) => (
  <BrowserRouter>{children}</BrowserRouter>
)

describe('URL Shortening Demo Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockedShortenUrl.mockResolvedValue({
      short_url: 'https://urlshort.io/abc123',
      short_code: 'abc123',
    })

    // Reset clipboard mock
    const mockClipboard = {
      writeText: vi.fn(() => Promise.resolve()),
      readText: vi.fn(() => Promise.resolve('')),
    }
    Object.defineProperty(navigator, 'clipboard', {
      value: mockClipboard,
      writable: true,
      configurable: true,
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  /**
   * Integration Test: Complete URL shortening flow
   * Tests the full flow from input to result display
   */
  it('completes full URL shortening flow', async () => {
    const user = userEvent.setup()
    const onRegisterPrompt = vi.fn()

    render(
      <TestWrapper>
        <UrlDemoSection onRegisterPrompt={onRegisterPrompt} />
      </TestWrapper>
    )

    // Step 1: Find and interact with input
    const input = screen.getByTestId('url-input')
    expect(input).toBeInTheDocument()

    // Step 2: Enter a valid URL
    await user.type(input, 'https://example.com/very/long/path/to/resource')
    expect(input).toHaveValue('https://example.com/very/long/path/to/resource')

    // Step 3: Click shorten button
    const shortenButton = screen.getByTestId('shorten-button')
    await user.click(shortenButton)

    // Step 4: Wait for result
    await waitFor(() => {
      expect(screen.getByTestId('result-container')).toBeInTheDocument()
    })

    // Step 5: Verify short URL is displayed
    const shortUrl = screen.getByTestId('short-url')
    expect(shortUrl).toHaveTextContent('https://urlshort.io/abc123')

    // Step 6: Verify copy button is available
    const copyButton = screen.getByTestId('copy-button')
    expect(copyButton).toBeInTheDocument()

    // Step 7: Verify registration prompt is shown
    const registrationPrompt = screen.getByTestId('registration-prompt')
    expect(registrationPrompt).toBeInTheDocument()
    expect(registrationPrompt).toHaveTextContent('Create an account')
    expect(registrationPrompt).toHaveTextContent('analytics')
  })

  /**
   * Integration Test: Copy to clipboard flow
   * Tests the complete copy-to-clipboard interaction
   */
  it('copies shortened URL to clipboard with visual feedback', async () => {
    const user = userEvent.setup()

    // Create a spy for clipboard.writeText
    const writeTextSpy = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: writeTextSpy },
      writable: true,
      configurable: true,
    })

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    // Shorten a URL first
    await user.type(screen.getByTestId('url-input'), 'https://example.com')
    await user.click(screen.getByTestId('shorten-button'))

    // Wait for result
    await waitFor(() => {
      expect(screen.getByTestId('copy-button')).toBeInTheDocument()
    })

    // Get copy button
    const copyButton = screen.getByTestId('copy-button')
    expect(copyButton).toHaveTextContent('Copy to Clipboard')

    // Click copy
    await user.click(copyButton)

    // Verify visual feedback
    await waitFor(() => {
      expect(copyButton).toHaveTextContent('Copied!')
    })

    // Verify clipboard was called
    expect(writeTextSpy).toHaveBeenCalledWith('https://urlshort.io/abc123')
  })

  /**
   * Integration Test: Validation prevents API call
   * Tests that invalid URLs are caught before API call
   */
  it('validates URL before making API call', async () => {
    const user = userEvent.setup()

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    // Try various invalid inputs
    const invalidUrls = [
      'not-a-url',
      'javascript:alert(1)',
      'data:text/html,<script>alert(1)</script>',
      'ftp://example.com',
    ]

    for (const invalidUrl of invalidUrls) {
      // Clear input
      const input = screen.getByTestId('url-input')
      await user.clear(input)

      // Enter invalid URL
      await user.type(input, invalidUrl)

      // Click shorten
      await user.click(screen.getByTestId('shorten-button'))

      // Verify error is shown
      await waitFor(() => {
        expect(screen.getByTestId('url-error')).toBeInTheDocument()
      })

      // Verify API was NOT called
      expect(mockedShortenUrl).not.toHaveBeenCalled()
    }
  })

  /**
   * Integration Test: Multiple URL shortenings
   * Tests shortening multiple URLs in sequence
   */
  it('allows shortening multiple URLs in sequence', async () => {
    const user = userEvent.setup()
    let callCount = 0

    // Return different short URLs for each call
    mockedShortenUrl.mockImplementation(async () => {
      callCount++
      return {
        short_url: `https://urlshort.io/short${callCount}`,
        short_code: `short${callCount}`,
      }
    })

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    // First URL
    const input = screen.getByTestId('url-input')
    await user.type(input, 'https://example1.com/path')
    await user.click(screen.getByTestId('shorten-button'))

    await waitFor(() => {
      expect(screen.getByTestId('short-url')).toHaveTextContent('https://urlshort.io/short1')
    })

    // Second URL
    await user.clear(input)
    await user.type(input, 'https://example2.com/path')
    await user.click(screen.getByTestId('shorten-button'))

    await waitFor(() => {
      expect(screen.getByTestId('short-url')).toHaveTextContent('https://urlshort.io/short2')
    })

    expect(mockedShortenUrl).toHaveBeenCalledTimes(2)
  })

  /**
   * Integration Test: Error recovery
   * Tests that users can recover from errors
   */
  it('allows recovery from validation errors', async () => {
    const user = userEvent.setup()

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')

    // First, trigger an error
    await user.type(input, 'invalid-url')
    await user.click(screen.getByTestId('shorten-button'))

    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    // Now fix the URL - error should clear when typing
    await user.clear(input)
    await user.type(input, 'https://example.com')

    // Error should be gone after typing
    expect(screen.queryByTestId('url-error')).not.toBeInTheDocument()

    // Submit valid URL
    await user.click(screen.getByTestId('shorten-button'))

    // Should succeed
    await waitFor(() => {
      expect(screen.getByTestId('short-url')).toBeInTheDocument()
    })
  })

  /**
   * Integration Test: API error handling
   * Tests graceful handling of API failures
   */
  it('handles API errors gracefully', async () => {
    const user = userEvent.setup()
    mockedShortenUrl.mockRejectedValue(new Error('Network error'))

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    await user.type(screen.getByTestId('url-input'), 'https://example.com')
    await user.click(screen.getByTestId('shorten-button'))

    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    expect(screen.getByTestId('url-error')).toHaveTextContent('Unable to shorten URL')

    // Result should not be shown
    expect(screen.queryByTestId('result-container')).not.toBeInTheDocument()
  })

  /**
   * Integration Test: Registration prompt callback
   * Tests that registration prompt correctly triggers callback
   */
  it('triggers registration callback when register link is clicked', async () => {
    const user = userEvent.setup()
    const onRegisterPrompt = vi.fn()

    render(
      <TestWrapper>
        <UrlDemoSection onRegisterPrompt={onRegisterPrompt} />
      </TestWrapper>
    )

    // Shorten URL first
    await user.type(screen.getByTestId('url-input'), 'https://example.com')
    await user.click(screen.getByTestId('shorten-button'))

    // Wait for result
    await waitFor(() => {
      expect(screen.getByTestId('register-link')).toBeInTheDocument()
    })

    // Click register link
    await user.click(screen.getByTestId('register-link'))

    // Callback should be triggered
    expect(onRegisterPrompt).toHaveBeenCalledTimes(1)
  })

  /**
   * Integration Test: Accessibility during flow
   * Tests that accessibility attributes are maintained throughout the flow
   */
  it('maintains accessibility throughout the flow', async () => {
    const user = userEvent.setup()

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    // Check initial accessibility
    const section = screen.getByTestId('url-demo-section')
    expect(section).toHaveAttribute('aria-labelledby', 'url-demo-heading')

    const input = screen.getByTestId('url-input')
    expect(input).toHaveAttribute('aria-label')
    expect(input).toHaveAttribute('aria-invalid', 'false')

    // Trigger an error
    await user.type(input, 'invalid')
    await user.click(screen.getByTestId('shorten-button'))

    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    // Check error accessibility
    expect(input).toHaveAttribute('aria-invalid', 'true')
    expect(input).toHaveAttribute('aria-describedby', 'url-error')

    const errorMessage = screen.getByTestId('url-error')
    expect(errorMessage).toHaveAttribute('role', 'alert')

    // Fix and complete flow
    await user.clear(input)
    await user.type(input, 'https://example.com')
    await user.click(screen.getByTestId('shorten-button'))

    await waitFor(() => {
      expect(screen.getByTestId('copy-button')).toBeInTheDocument()
    })

    // Check copy button accessibility
    const copyButton = screen.getByTestId('copy-button')
    expect(copyButton).toHaveAttribute('aria-label')
  })
})
