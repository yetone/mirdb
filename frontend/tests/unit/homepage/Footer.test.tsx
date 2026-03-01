/**
 * Footer Component Unit Tests
 * Owner: Scenario 6 - Footer Display
 *
 * Tests for the Footer component.
 *
 * Test coverage:
 * - Footer element is present on the page
 * - Copyright text is displayed
 * - Footer contains relevant links
 * - Footer is positioned at the bottom of the page
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '../../utils/render'
import { Footer } from '../../../src/components/homepage/Footer'
import { Home } from '../../../src/pages/Home'

describe('Footer Component', () => {
  describe('Test Case 1: Footer element is present on the page', () => {
    it('should render the footer element with correct testid', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('should render the footer with semantic HTML (footer element)', () => {
      render(<Footer />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('should have proper aria-label for accessibility', () => {
      render(<Footer />)

      const footer = screen.getByRole('contentinfo', { name: /site footer/i })
      expect(footer).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Copyright text is displayed in footer', () => {
    it('should display copyright text', () => {
      render(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright).toHaveTextContent(/copyright/i)
    })

    it('should include the current year in copyright', () => {
      render(<Footer />)

      const currentYear = new Date().getFullYear()
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveTextContent(currentYear.toString())
    })

    it('should mention URL Shortener in copyright', () => {
      render(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveTextContent(/URL Shortener/i)
    })

    it('should include "All rights reserved" text', () => {
      render(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveTextContent(/all rights reserved/i)
    })
  })

  describe('Test Case 3: Footer contains relevant links (if applicable)', () => {
    it('should render footer links container', () => {
      render(<Footer />)

      const footerLinks = screen.getByTestId('footer-links')
      expect(footerLinks).toBeInTheDocument()
    })

    it('should contain Login link pointing to /login', () => {
      render(<Footer />)

      const loginLink = screen.getByTestId('footer-link-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
      expect(loginLink).toHaveTextContent(/login/i)
    })

    it('should contain Register link pointing to /register', () => {
      render(<Footer />)

      const registerLink = screen.getByTestId('footer-link-register')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
      expect(registerLink).toHaveTextContent(/register/i)
    })

    it('should have footer links container with accessible links', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      const linksContainer = within(footer).getByTestId('footer-links')
      expect(linksContainer).toBeInTheDocument()

      // Both links should be within the container
      const links = within(linksContainer).getAllByRole('link')
      expect(links.length).toBe(2)
    })
  })

  describe('Test Case 4: Footer is positioned at the bottom of the page', () => {
    it('should have appropriate CSS classes for footer positioning', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('footer')
      expect(footer).toHaveClass('footer-center')
    })

    it('should have base-200 background for visual distinction', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('should be rendered within the Home page at the bottom', () => {
      render(<Home />)

      // Footer section should be present in Home page
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toBeInTheDocument()
    })
  })

  describe('Theme consistency', () => {
    it('should use theme-aware text colors', () => {
      render(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('text-base-content')
    })
  })

  describe('Accessibility', () => {
    it('should be accessible with proper semantic structure', () => {
      render(<Footer />)

      // Footer should be a contentinfo landmark
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()

      // Links should be accessible
      const links = screen.getAllByRole('link')
      expect(links.length).toBeGreaterThanOrEqual(2)
    })
  })
})
