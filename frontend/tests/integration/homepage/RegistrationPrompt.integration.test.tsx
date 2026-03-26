/**
 * Integration Tests for Registration Prompt After Shortening
 * Owner: Scenario 13 - Registration Prompt After Shortening
 *
 * Tests:
 * - Registration prompt appears below shortened URL (Test Case 1)
 * - Text includes 'analytics' to highlight benefit (Test Case 2)
 * - Click registration prompt redirects to /register (Test Case 3)
 *
 * Requirements: REQ-7, US-7
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { InlineShortener } from '../../../src/components/homepage/InlineShortener'

// Mock clipboard API
const mockClipboard = {
  writeText: vi.fn().mockResolvedValue(undefined),
}

Object.defineProperty(navigator, 'clipboard', {
  value: mockClipboard,
  writable: true,
})

describe('Registration Prompt After Shortening', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 1: Registration prompt appears below shortened URL', () => {
    it('displays registration prompt after successful URL shortening', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      // Shorten a URL as anonymous user
      fireEvent.change(input, {
        target: { value: 'https://example.com/very/long/url/path' },
      })
      fireEvent.click(button)

      // Wait for result to appear
      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })

      // Verify registration prompt is visible below the result
      const registrationPrompt = screen.getByTestId('registration-prompt')
      expect(registrationPrompt).toBeInTheDocument()

      // Verify the prompt is inside the result area (below shortened URL)
      const resultArea = screen.getByTestId('result-area')
      expect(resultArea).toContainElement(registrationPrompt)
    })

    it('shows registration prompt after short URL and copy button', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })

      // Verify elements appear in order: short URL, copy button, then registration prompt
      const shortUrl = screen.getByTestId('short-url')
      const copyButton = screen.getByTestId('copy-button')
      const registrationPrompt = screen.getByTestId('registration-prompt')

      expect(shortUrl).toBeInTheDocument()
      expect(copyButton).toBeInTheDocument()
      expect(registrationPrompt).toBeInTheDocument()

      // Verify registration prompt appears after copy button in DOM order
      const resultArea = screen.getByTestId('result-area')
      const elements = resultArea.querySelectorAll('[data-testid]')
      const testIds = Array.from(elements).map((el) => el.getAttribute('data-testid'))

      const shortUrlIndex = testIds.indexOf('short-url')
      const copyButtonIndex = testIds.indexOf('copy-button')
      const promptIndex = testIds.indexOf('registration-prompt')

      expect(shortUrlIndex).toBeLessThan(promptIndex)
      expect(copyButtonIndex).toBeLessThan(promptIndex)
    })

    it('does not show registration prompt before URL is shortened', () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      // Before shortening, registration prompt should not be visible
      expect(screen.queryByTestId('registration-prompt')).not.toBeInTheDocument()
      expect(screen.queryByTestId('register-cta')).not.toBeInTheDocument()
    })

    it('hides registration prompt when user clicks "Shorten another"', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      // Shorten URL first
      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
      })

      // Click "Shorten another" button
      const shortenAnotherButton = screen.getByTestId('shorten-another-button')
      fireEvent.click(shortenAnotherButton)

      // Registration prompt should disappear along with result area
      await waitFor(() => {
        expect(screen.queryByTestId('registration-prompt')).not.toBeInTheDocument()
      })
    })
  })

  describe('Test Case 2: Registration prompt text includes analytics', () => {
    it('includes "analytics" text in registration prompt to highlight benefit', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
      })

      // Check that the CTA button contains "analytics"
      const registerCta = screen.getByTestId('register-cta')
      expect(registerCta.textContent?.toLowerCase()).toContain('analytics')
    })

    it('displays descriptive text about tracking links', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
      })

      // Verify there's descriptive text about tracking
      const registrationPrompt = screen.getByTestId('registration-prompt')
      const promptText = registrationPrompt.textContent?.toLowerCase() || ''

      // Should mention tracking or analytics benefit
      expect(promptText).toMatch(/track|analytics/)
    })

    it('has clear call-to-action text for creating account', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('register-cta')).toBeInTheDocument()
      })

      const registerCta = screen.getByTestId('register-cta')
      const ctaText = registerCta.textContent?.toLowerCase() || ''

      // Should mention creating an account
      expect(ctaText).toMatch(/create|account|sign up|register/)
    })
  })

  describe('Test Case 3: Click registration prompt redirects to /register', () => {
    it('registration CTA links to /register page', async () => {
      render(
        <MemoryRouter>
          <InlineShortener />
        </MemoryRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('register-cta')).toBeInTheDocument()
      })

      const registerCta = screen.getByTestId('register-cta')

      // Verify the link has correct href
      expect(registerCta).toHaveAttribute('href', '/register')
    })

    it('registration CTA is a clickable link element', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('register-cta')).toBeInTheDocument()
      })

      const registerCta = screen.getByTestId('register-cta')

      // Verify it's a link element (anchor tag)
      expect(registerCta.tagName.toLowerCase()).toBe('a')
    })

    it('registration CTA is accessible via keyboard', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('register-cta')).toBeInTheDocument()
      })

      const registerCta = screen.getByTestId('register-cta')

      // Link should be focusable
      registerCta.focus()
      expect(document.activeElement).toBe(registerCta)
    })

    it('registration CTA has button styling for visibility', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('register-cta')).toBeInTheDocument()
      })

      const registerCta = screen.getByTestId('register-cta')

      // Should have button-like styling classes
      expect(registerCta.className).toContain('btn')
    })
  })

  describe('Registration Prompt Visibility Across States', () => {
    it('registration prompt persists when copying short URL', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
      })

      // Click copy button
      const copyButton = screen.getByTestId('copy-button')
      fireEvent.click(copyButton)

      // Registration prompt should still be visible after copying
      await waitFor(() => {
        expect(screen.getByTestId('copied-feedback')).toBeInTheDocument()
      })

      expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
    })

    it('shows new registration prompt after shortening another URL', async () => {
      render(
        <BrowserRouter>
          <InlineShortener />
        </BrowserRouter>
      )

      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      // First URL
      fireEvent.change(input, { target: { value: 'https://first.com' } })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
      })

      // Shorten another
      fireEvent.click(screen.getByTestId('shorten-another-button'))

      await waitFor(() => {
        expect(screen.queryByTestId('registration-prompt')).not.toBeInTheDocument()
      })

      // Second URL
      fireEvent.change(input, { target: { value: 'https://second.com' } })
      fireEvent.click(submitButton)

      // Registration prompt should appear again
      await waitFor(() => {
        expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
      })
    })
  })
})
