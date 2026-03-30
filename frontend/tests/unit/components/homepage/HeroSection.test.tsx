/**
 * Tests for HeroSection component.
 * Owner: Scenario 2 - Hero Section URL Input and Shortening CTA
 *
 * Test cases:
 * - URL input field exists with correct type and placeholder
 * - URL input field receives focus on mount
 * - Shorten URL button exists
 * - Enter key triggers URL shortening
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, waitFor } from '../../../test-utils'
import userEvent from '@testing-library/user-event'
import HeroSection from '@/components/homepage/HeroSection'

describe('HeroSection', () => {
  describe('URL Input Field (Test Case 1)', () => {
    it('renders URL input field with type="url" and appropriate placeholder', () => {
      render(<HeroSection />)

      const input = screen.getByTestId('url-input')
      expect(input).toBeInTheDocument()
      expect(input).toHaveAttribute('type', 'url')
      expect(input).toHaveAttribute('placeholder', 'Enter your long URL here...')
    })

    it('has a text-like input for URL entry', () => {
      render(<HeroSection />)

      const input = screen.getByRole('textbox', { name: /url to shorten/i })
      expect(input).toBeInTheDocument()
    })
  })

  describe('Input Auto-Focus (Test Case 2)', () => {
    it('URL input field has focus after component mounts', async () => {
      render(<HeroSection />)

      const input = screen.getByTestId('url-input')

      await waitFor(() => {
        expect(document.activeElement).toBe(input)
      })
    })
  })

  describe('Shorten URL Button (Test Case 3)', () => {
    it('renders button with text "Shorten URL"', () => {
      render(<HeroSection />)

      const button = screen.getByTestId('shorten-button')
      expect(button).toBeInTheDocument()
      expect(button).toHaveTextContent('Shorten URL')
    })

    it('Shorten URL button exists as primary CTA', () => {
      render(<HeroSection />)

      const button = screen.getByRole('button', { name: /shorten url/i })
      expect(button).toBeInTheDocument()
    })
  })

  describe('Enter Key Submission (Test Case 4)', () => {
    it('triggers form submission when Enter key is pressed with URL content', async () => {
      const handleSubmit = vi.fn()
      const user = userEvent.setup()
      render(<HeroSection onUrlSubmit={handleSubmit} />)

      const input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com')
      await user.keyboard('{Enter}')

      expect(handleSubmit).toHaveBeenCalledWith('https://example.com')
    })

    it('Enter key submission has same effect as clicking Shorten URL button', async () => {
      const handleSubmitEnter = vi.fn()
      const handleSubmitClick = vi.fn()
      const user = userEvent.setup()

      // Test Enter key submission
      const { unmount } = render(<HeroSection onUrlSubmit={handleSubmitEnter} />)
      let input = screen.getByTestId('url-input')
      await user.type(input, 'https://example.com')
      await user.keyboard('{Enter}')

      unmount()

      // Test button click submission
      render(<HeroSection onUrlSubmit={handleSubmitClick} />)
      input = screen.getByTestId('url-input')
      const button = screen.getByTestId('shorten-button')
      await user.type(input, 'https://example.com')
      await user.click(button)

      // Both should be called with the same argument
      expect(handleSubmitEnter).toHaveBeenCalledWith('https://example.com')
      expect(handleSubmitClick).toHaveBeenCalledWith('https://example.com')
    })
  })

  describe('Hero Section Structure', () => {
    it('renders hero section with correct test id', () => {
      render(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('renders headline text', () => {
      render(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten URLs, Track Insights')
    })

    it('renders subheadline text', () => {
      render(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toContain('Transform long URLs')
    })

    it('renders secondary CTAs', () => {
      render(<HeroSection />)

      const learnMore = screen.getByRole('button', { name: /learn more/i })
      expect(learnMore).toBeInTheDocument()
    })

    it('shows Sign Up button for non-authenticated users', () => {
      render(<HeroSection isAuthenticated={false} />)

      const signUp = screen.getByRole('button', { name: /sign up free/i })
      expect(signUp).toBeInTheDocument()
    })

    it('hides Sign Up button for authenticated users', () => {
      render(<HeroSection isAuthenticated={true} />)

      const signUp = screen.queryByRole('button', { name: /sign up free/i })
      expect(signUp).not.toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('has proper aria-label on hero section', () => {
      render(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('aria-label', 'Hero section')
    })

    it('URL input has proper aria-label', () => {
      render(<HeroSection />)

      const input = screen.getByLabelText(/url to shorten/i)
      expect(input).toBeInTheDocument()
    })
  })
})
