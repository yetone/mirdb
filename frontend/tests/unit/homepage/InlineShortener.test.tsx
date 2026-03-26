/**
 * Unit Tests for InlineShortener Component
 * Owner: Scenario 2 - Inline URL Shortening
 *
 * Tests:
 * - Input field is auto-focused on page load
 * - Placeholder text displays correctly
 * - Loading state is shown during processing
 * - Error messages display properly
 * - Result area shows shortened URL
 * - Copy button functionality
 *
 * Requirements: REQ-2, REQ-7
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

describe('InlineShortener', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Initial Render', () => {
    it('renders the inline shortener section', () => {
      render(<InlineShortener />)
      expect(screen.getByTestId('inline-shortener')).toBeInTheDocument()
    })

    it('input field is auto-focused on page load', () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      expect(document.activeElement).toBe(input)
    })

    it('renders input with correct placeholder text', () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      expect(input).toHaveAttribute('placeholder', 'Paste your long URL here...')
    })

    it('renders Shorten button', () => {
      render(<InlineShortener />)
      const button = screen.getByTestId('shorten-button')
      expect(button).toBeInTheDocument()
      expect(button).toHaveTextContent('Shorten')
    })

    it('Shorten button is disabled when input is empty', () => {
      render(<InlineShortener />)
      const button = screen.getByTestId('shorten-button')
      expect(button).toBeDisabled()
    })

    it('does not auto-focus when autoFocus prop is false', () => {
      render(<InlineShortener autoFocus={false} />)
      const input = screen.getByTestId('url-input')
      expect(document.activeElement).not.toBe(input)
    })
  })

  describe('URL Input', () => {
    it('enables Shorten button when URL is entered', () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      expect(button).not.toBeDisabled()
    })

    it('disables Shorten button when input is whitespace only', () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: '   ' } })
      expect(button).toBeDisabled()
    })
  })

  describe('Form Submission', () => {
    it('submits form when Enter key is pressed in input', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' })

      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })
    })

    it('shows loading state during URL shortening', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      // Check for loading spinner
      expect(button).toHaveAttribute('aria-busy', 'true')
    })

    it('displays shortened URL after successful submission', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com/very/long/url/path' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
        expect(screen.getByTestId('short-url')).toBeInTheDocument()
      })
    })

    it('shows error message for invalid URL', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'not-a-valid-url' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('error-message')).toBeInTheDocument()
        expect(screen.getByTestId('error-message')).toHaveTextContent('Please enter a valid URL')
      })
    })

    it('calls onShortenSuccess callback when URL is shortened', async () => {
      const onShortenSuccess = vi.fn()
      render(<InlineShortener onShortenSuccess={onShortenSuccess} />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(onShortenSuccess).toHaveBeenCalledWith(expect.stringContaining('/s/'))
      })
    })
  })

  describe('Copy Functionality', () => {
    it('shows copy button after URL is shortened', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('copy-button')).toBeInTheDocument()
      })
    })

    it('copies URL to clipboard and shows Copied! feedback', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const submitButton = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(submitButton)

      await waitFor(() => {
        expect(screen.getByTestId('copy-button')).toBeInTheDocument()
      })

      const copyButton = screen.getByTestId('copy-button')
      fireEvent.click(copyButton)

      await waitFor(() => {
        expect(mockClipboard.writeText).toHaveBeenCalled()
        expect(screen.getByTestId('copied-feedback')).toBeInTheDocument()
        expect(screen.getByTestId('copied-feedback')).toHaveTextContent('Copied!')
      })
    })
  })

  describe('Registration Prompt', () => {
    it('shows registration prompt after successful shortening', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('registration-prompt')).toBeInTheDocument()
        expect(screen.getByTestId('register-cta')).toBeInTheDocument()
      })
    })

    it('registration CTA links to /register', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const registerLink = screen.getByTestId('register-cta')
        expect(registerLink).toHaveAttribute('href', '/register')
      })
    })
  })

  describe('Shorten Another', () => {
    it('shows Shorten another button after successful shortening', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('shorten-another-button')).toBeInTheDocument()
      })
    })

    it('resets form when Shorten another is clicked', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(screen.getByTestId('result-area')).toBeInTheDocument()
      })

      const shortenAnotherButton = screen.getByTestId('shorten-another-button')
      fireEvent.click(shortenAnotherButton)

      await waitFor(() => {
        expect(screen.queryByTestId('result-area')).not.toBeInTheDocument()
        expect(input).toHaveValue('')
        expect(document.activeElement).toBe(input)
      })
    })
  })

  describe('Accessibility', () => {
    it('input has proper aria-label', () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      expect(input).toHaveAttribute('aria-label', 'URL to shorten')
    })

    it('input has aria-invalid when error occurs', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'invalid-url' } })
      fireEvent.click(button)

      await waitFor(() => {
        expect(input).toHaveAttribute('aria-invalid', 'true')
      })
    })

    it('error message has role alert', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'invalid-url' } })
      fireEvent.click(button)

      await waitFor(() => {
        const error = screen.getByTestId('error-message')
        expect(error).toHaveAttribute('role', 'alert')
      })
    })

    it('URL input has associated label element or aria-label (Test Case 3)', () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')

      // Check for aria-label attribute
      const ariaLabel = input.getAttribute('aria-label')

      // Verify aria-label exists and is meaningful
      expect(ariaLabel).toBeTruthy()
      expect(ariaLabel?.length).toBeGreaterThan(0)
    })

    it('all SVG icons have aria-hidden attribute (Test Case 4)', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      // Trigger error to show error icon
      fireEvent.change(input, { target: { value: 'invalid-url' } })
      fireEvent.click(button)

      await waitFor(() => {
        // Error icon SVG should have aria-hidden
        const svgs = document.querySelectorAll('svg')
        svgs.forEach((svg) => {
          const ariaHidden = svg.getAttribute('aria-hidden')
          expect(ariaHidden).toBe('true')
        })
      })
    })

    it('copy button has accessible aria-label', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      await waitFor(() => {
        const copyButton = screen.getByTestId('copy-button')
        const ariaLabel = copyButton.getAttribute('aria-label')
        expect(ariaLabel).toBeTruthy()
        expect(ariaLabel).toContain('clipboard')
      })
    })

    it('error message is associated with input via aria-describedby', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'invalid-url' } })
      fireEvent.click(button)

      await waitFor(() => {
        const ariaDescribedBy = input.getAttribute('aria-describedby')
        expect(ariaDescribedBy).toBe('url-error')

        // Verify the referenced error element exists
        const errorElement = document.getElementById('url-error')
        expect(errorElement).toBeInTheDocument()
      })
    })

    it('loading state has aria-busy attribute', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      // During loading, button should have aria-busy
      expect(button).toHaveAttribute('aria-busy', 'true')
    })

    it('loading spinner has sr-only text for screen readers', async () => {
      render(<InlineShortener />)
      const input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')

      fireEvent.change(input, { target: { value: 'https://example.com' } })
      fireEvent.click(button)

      // Look for screen reader only text during loading
      const srOnlyText = button.querySelector('.sr-only')
      expect(srOnlyText).toBeInTheDocument()
      expect(srOnlyText?.textContent).toContain('Shortening')
    })
  })
})
