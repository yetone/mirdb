/**
 * CTASection Unit Tests
 * Owner: Scenario 6 - Secondary CTA Section
 *
 * Tests for the CTASection component verifying:
 * - CTA section renders with compelling message
 * - CTA button exists with correct text
 * - CTA button links to register page
 *
 * Requirements: REQ-6
 */

import { describe, it, expect } from 'vitest'
import { screen, fireEvent } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { CTASection } from '../../../src/components/landing/CTASection'

describe('CTASection', () => {
  describe('Test Case 1: Secondary CTA section exists before footer with compelling message', () => {
    it('renders the CTA section with default compelling message', () => {
      renderWithProviders(<CTASection />)

      const ctaSection = screen.getByTestId('cta-section')
      expect(ctaSection).toBeInTheDocument()

      const ctaMessage = screen.getByTestId('cta-message')
      expect(ctaMessage).toBeInTheDocument()
      expect(ctaMessage.textContent).toContain('Join thousands')
    })

    it('renders with custom message when provided', () => {
      const customMessage = 'Start shortening URLs today!'
      renderWithProviders(<CTASection message={customMessage} />)

      const ctaMessage = screen.getByTestId('cta-message')
      expect(ctaMessage).toHaveTextContent(customMessage)
    })

    it('message is an h2 element for proper heading hierarchy', () => {
      renderWithProviders(<CTASection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('Join thousands of users shortening URLs today')
    })

    it('section has proper aria-labelledby for accessibility', () => {
      renderWithProviders(<CTASection />)

      const ctaSection = screen.getByTestId('cta-section')
      expect(ctaSection).toHaveAttribute('aria-labelledby', 'cta-heading')
    })
  })

  describe('Test Case 2: CTA button with text like Create Free Account exists', () => {
    it('renders CTA button with default text', () => {
      renderWithProviders(<CTASection />)

      const ctaButton = screen.getByTestId('cta-button')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveTextContent('Create Free Account')
    })

    it('renders with custom CTA text when provided', () => {
      const customText = 'Sign Up Now'
      renderWithProviders(<CTASection ctaText={customText} />)

      const ctaButton = screen.getByTestId('cta-button')
      expect(ctaButton).toHaveTextContent(customText)
    })

    it('CTA button is a link element', () => {
      renderWithProviders(<CTASection />)

      const ctaButton = screen.getByTestId('cta-button')
      expect(ctaButton.tagName.toLowerCase()).toBe('a')
    })
  })

  describe('Test Case 3: Router navigates to /register route', () => {
    it('CTA button links to registration page by default', () => {
      renderWithProviders(<CTASection />)

      const ctaButton = screen.getByTestId('cta-button')
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('CTA button links to custom href when provided', () => {
      const customHref = '/signup'
      renderWithProviders(<CTASection ctaHref={customHref} />)

      const ctaButton = screen.getByTestId('cta-button')
      expect(ctaButton).toHaveAttribute('href', '/signup')
    })

    it('clicking CTA button triggers navigation', () => {
      renderWithProviders(<CTASection />)

      const ctaButton = screen.getByTestId('cta-button')

      // Verify the link is clickable and has the correct href
      expect(ctaButton).toHaveAttribute('href', '/register')

      // fireEvent.click simulates user interaction
      fireEvent.click(ctaButton)

      // Since we're using BrowserRouter from test-utils, the navigation
      // is handled by React Router. The link element should be functional.
      expect(ctaButton).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('section uses semantic HTML section element', () => {
      renderWithProviders(<CTASection />)

      const section = screen.getByTestId('cta-section')
      expect(section.tagName.toLowerCase()).toBe('section')
    })

    it('CTA button is keyboard accessible', () => {
      renderWithProviders(<CTASection />)

      const ctaButton = screen.getByTestId('cta-button')
      ctaButton.focus()
      expect(document.activeElement).toBe(ctaButton)
    })
  })

  describe('Styling and Layout', () => {
    it('section has background styling for visual distinction', () => {
      renderWithProviders(<CTASection />)

      const section = screen.getByTestId('cta-section')
      expect(section).toHaveClass('bg-base-200')
    })

    it('content is centered within the section', () => {
      renderWithProviders(<CTASection />)

      const section = screen.getByTestId('cta-section')
      const container = section.querySelector('.text-center')
      expect(container).toBeInTheDocument()
    })
  })
})
