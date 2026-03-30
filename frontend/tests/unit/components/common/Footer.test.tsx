/**
 * Footer component unit tests.
 * Owner: Scenario 10 - Footer Content and Links
 *
 * Tests:
 * 1. Footer element exists at bottom of page
 * 2. Copyright text is present with current year and product name
 * 3. Terms of Service link is present
 * 4. Privacy Policy link is present
 */

import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../../../test-utils'
import { Footer } from '@/components/common/Footer'

describe('Footer', () => {
  describe('Test Case 1: Footer element exists', () => {
    it('should render the footer element', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('should have a footer HTML element', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Copyright text is present', () => {
    it('should display copyright text with current year', () => {
      renderWithProviders(<Footer />)

      const currentYear = new Date().getFullYear()
      const copyright = screen.getByTestId('copyright')

      expect(copyright).toBeInTheDocument()
      expect(copyright.textContent).toContain(String(currentYear))
    })

    it('should display the product name in copyright', () => {
      renderWithProviders(<Footer />)

      const copyright = screen.getByTestId('copyright')
      expect(copyright.textContent).toContain('URL Shortener')
    })

    it('should include "All rights reserved" text', () => {
      renderWithProviders(<Footer />)

      const copyright = screen.getByTestId('copyright')
      expect(copyright.textContent).toContain('All rights reserved')
    })
  })

  describe('Test Case 3: Terms of Service link is present', () => {
    it('should render Terms of Service link', () => {
      renderWithProviders(<Footer />)

      const termsLink = screen.getByTestId('terms-link')
      expect(termsLink).toBeInTheDocument()
    })

    it('should have correct link text for Terms of Service', () => {
      renderWithProviders(<Footer />)

      const termsLink = screen.getByRole('link', { name: /terms of service/i })
      expect(termsLink).toBeInTheDocument()
    })

    it('should link to /terms path', () => {
      renderWithProviders(<Footer />)

      const termsLink = screen.getByTestId('terms-link')
      expect(termsLink).toHaveAttribute('href', '/terms')
    })
  })

  describe('Test Case 4: Privacy Policy link is present', () => {
    it('should render Privacy Policy link', () => {
      renderWithProviders(<Footer />)

      const privacyLink = screen.getByTestId('privacy-link')
      expect(privacyLink).toBeInTheDocument()
    })

    it('should have correct link text for Privacy Policy', () => {
      renderWithProviders(<Footer />)

      const privacyLink = screen.getByRole('link', { name: /privacy policy/i })
      expect(privacyLink).toBeInTheDocument()
    })

    it('should link to /privacy path', () => {
      renderWithProviders(<Footer />)

      const privacyLink = screen.getByTestId('privacy-link')
      expect(privacyLink).toHaveAttribute('href', '/privacy')
    })
  })

  describe('Accessibility', () => {
    it('should have accessible navigation', () => {
      renderWithProviders(<Footer />)

      const nav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(nav).toBeInTheDocument()
    })

    it('should have all links accessible via role', () => {
      renderWithProviders(<Footer />)

      const links = screen.getAllByRole('link')
      expect(links.length).toBeGreaterThanOrEqual(2)
    })
  })
})
