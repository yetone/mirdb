/**
 * Unit tests for UrlDemoSection component.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Test cases:
 * 1. Input accepts URL and validates format
 * 2. Short URL is generated and displayed on the page
 * 3. Message prompts user to register for full analytics appears after successful shortening
 * 4. Validation error displayed, shortening not attempted for invalid URLs
 * 5. Short URL copied to clipboard with visual confirmation
 * 6. URLs with javascript: or data: schemes are rejected
 */

import React from 'react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { UrlDemoSection } from '@/components/home/UrlDemoSection'

// Mock the API module
vi.mock('@/api', () => ({
  shortenUrl: vi.fn(),
}))

// Mock the clipboard utility
vi.mock('@/utils/clipboard', () => ({
  copyToClipboard: vi.fn(() => Promise.resolve(true)),
  isClipboardSupported: vi.fn(() => true),
}))

import { shortenUrl } from '@/api'
import { copyToClipboard } from '@/utils/clipboard'

const mockedShortenUrl = vi.mocked(shortenUrl)
const mockedCopyToClipboard = vi.mocked(copyToClipboard)

// Wrapper component for Router context
const TestWrapper = ({ children }: { children: React.ReactNode }) => (
  <BrowserRouter>{children}</BrowserRouter>
)

describe('UrlDemoSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockedShortenUrl.mockResolvedValue({
      short_url: 'https://urlshort.io/abc123',
      short_code: 'abc123',
    })
    mockedCopyToClipboard.mockResolvedValue(true)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  /**
   * Test Case 1: Input accepts URL and validates format
   */
  it('accepts valid URL in input field', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    expect(input).toBeInTheDocument()

    // Enter a valid URL
    await userEvent.type(input, 'https://example.com/very/long/path/to/resource')
    expect(input).toHaveValue('https://example.com/very/long/path/to/resource')

    // No error should be displayed
    expect(screen.queryByTestId('url-error')).not.toBeInTheDocument()
  })

  /**
   * Test Case 2: Short URL is generated and displayed on the page
   */
  it('generates and displays short URL after clicking shorten button', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    // Enter a valid URL
    await userEvent.type(input, 'https://example.com/very/long/path')

    // Click shorten button
    fireEvent.click(shortenButton)

    // Wait for the result to appear
    await waitFor(() => {
      expect(screen.getByTestId('result-container')).toBeInTheDocument()
    })

    // Verify the short URL is displayed
    expect(screen.getByTestId('short-url')).toHaveTextContent('https://urlshort.io/abc123')
    expect(mockedShortenUrl).toHaveBeenCalledWith('https://example.com/very/long/path')
  })

  /**
   * Test Case 3: Registration prompt appears after successful shortening
   */
  it('displays registration prompt after successful URL shortening', async () => {
    const onRegisterPrompt = vi.fn()

    render(
      <TestWrapper>
        <UrlDemoSection onRegisterPrompt={onRegisterPrompt} />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    // Enter a valid URL and shorten
    await userEvent.type(input, 'https://example.com/long/url')
    fireEvent.click(shortenButton)

    // Wait for the registration prompt to appear
    await waitFor(() => {
      expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
    })

    // Verify the prompt text
    const prompt = screen.getByTestId('registration-prompt')
    expect(prompt).toHaveTextContent('Create an account')
    expect(prompt).toHaveTextContent('analytics')

    // Click the register link
    const registerLink = screen.getByTestId('register-link')
    fireEvent.click(registerLink)
    expect(onRegisterPrompt).toHaveBeenCalledTimes(1)
  })

  /**
   * Test Case 4: Validation error displayed for invalid URL
   */
  it('displays validation error for invalid URL', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    // Enter an invalid URL
    await userEvent.type(input, 'not-a-url')

    // Click shorten button
    fireEvent.click(shortenButton)

    // Wait for error to appear
    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    // Verify error message
    expect(screen.getByTestId('url-error')).toHaveTextContent('valid URL')

    // API should NOT be called
    expect(mockedShortenUrl).not.toHaveBeenCalled()
  })

  /**
   * Test Case 5: Short URL copied to clipboard with visual confirmation
   */
  it('copies short URL to clipboard with visual confirmation', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    // Enter a valid URL and shorten
    await userEvent.type(input, 'https://example.com/path')
    fireEvent.click(shortenButton)

    // Wait for result
    await waitFor(() => {
      expect(screen.getByTestId('copy-button')).toBeInTheDocument()
    })

    // Click copy button
    const copyButton = screen.getByTestId('copy-button')
    fireEvent.click(copyButton)

    // Wait for visual confirmation
    await waitFor(() => {
      expect(copyButton).toHaveTextContent('Copied!')
    })

    // Verify clipboard was called
    expect(mockedCopyToClipboard).toHaveBeenCalledWith('https://urlshort.io/abc123')
  })

  /**
   * Test Case 6: URLs with javascript: or data: schemes are rejected
   */
  it('rejects URLs with javascript: scheme', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    // Enter a malicious URL with javascript: scheme
    await userEvent.type(input, 'javascript:alert(1)')
    fireEvent.click(shortenButton)

    // Wait for error to appear
    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    // Verify error message about malicious schemes
    const errorText = screen.getByTestId('url-error').textContent?.toLowerCase() || ''
    expect(errorText).toContain('javascript')

    // API should NOT be called
    expect(mockedShortenUrl).not.toHaveBeenCalled()
  })

  it('rejects URLs with data: scheme', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    // Enter a malicious URL with data: scheme
    await userEvent.type(input, 'data:text/html,<script>alert(1)</script>')
    fireEvent.click(shortenButton)

    // Wait for error to appear
    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    // Verify error message about malicious schemes
    const errorText = screen.getByTestId('url-error').textContent?.toLowerCase() || ''
    expect(errorText).toContain('data')

    // API should NOT be called
    expect(mockedShortenUrl).not.toHaveBeenCalled()
  })

  /**
   * Additional test: Empty input validation
   */
  it('displays error when input is empty', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const shortenButton = screen.getByTestId('shorten-button')
    fireEvent.click(shortenButton)

    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    expect(screen.getByTestId('url-error')).toHaveTextContent('enter a URL')
    expect(mockedShortenUrl).not.toHaveBeenCalled()
  })

  /**
   * Additional test: Loading state during API call
   */
  it('shows loading state while shortening URL', async () => {
    // Make the API take some time
    mockedShortenUrl.mockImplementation(
      () => new Promise(resolve => setTimeout(() => resolve({ short_url: 'https://urlshort.io/xyz', short_code: 'xyz' }), 100))
    )

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    await userEvent.type(input, 'https://example.com')
    fireEvent.click(shortenButton)

    // Button should show loading state
    expect(shortenButton).toHaveTextContent('Loading')
    expect(shortenButton).toBeDisabled()

    // Wait for the operation to complete
    await waitFor(() => {
      expect(screen.getByTestId('short-url')).toBeInTheDocument()
    })
  })

  /**
   * Additional test: Accessibility
   */
  it('has proper accessibility attributes', () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    // Section should have aria-labelledby
    const section = screen.getByTestId('url-demo-section')
    expect(section).toHaveAttribute('aria-labelledby', 'url-demo-heading')

    // Input should have aria-label
    const input = screen.getByTestId('url-input')
    expect(input).toHaveAttribute('aria-label', 'URL to shorten')

    // Shorten button should have aria-label
    const shortenButton = screen.getByTestId('shorten-button')
    expect(shortenButton).toHaveAttribute('aria-label', 'Shorten URL')
  })

  /**
   * Additional test: Error clears when user starts typing
   */
  it('clears error when user starts typing', async () => {
    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    // Trigger an error
    await userEvent.type(input, 'invalid')
    fireEvent.click(shortenButton)

    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    // Start typing again
    await userEvent.type(input, 'https://valid.com')

    // Error should be cleared
    expect(screen.queryByTestId('url-error')).not.toBeInTheDocument()
  })

  /**
   * Additional test: API error handling
   */
  it('handles API errors gracefully', async () => {
    mockedShortenUrl.mockRejectedValue(new Error('Network error'))

    render(
      <TestWrapper>
        <UrlDemoSection />
      </TestWrapper>
    )

    const input = screen.getByTestId('url-input')
    const shortenButton = screen.getByTestId('shorten-button')

    await userEvent.type(input, 'https://example.com')
    fireEvent.click(shortenButton)

    await waitFor(() => {
      expect(screen.getByTestId('url-error')).toBeInTheDocument()
    })

    expect(screen.getByTestId('url-error')).toHaveTextContent('Unable to shorten URL')
  })
})
