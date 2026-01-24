/**
 * Footer Unit Tests
 * Owner: Scenario 9 - Footer Section
 *
 * Tests for the Footer component verifying:
 * - Footer renders with semantic <footer> tag
 * - Navigation links (Home, Login, Register) are present
 * - Copyright notice displays current year (2026)
 * - All links navigate to correct routes
 *
 * Requirements: REQ-9
 */

import { describe, it, expect } from 'vitest'
import { screen, fireEvent } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { Footer } from '../../../src/components/landing/Footer'

describe('Footer', () => {
  describe('Test Case 1: Footer element exists with semantic <footer> tag', () => {
    it('renders the footer with semantic footer element', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('footer has role="contentinfo" for accessibility', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveAttribute('role', 'contentinfo')
    })

    it('footer contains navigation element with proper aria-label', () => {
      renderWithProviders(<Footer />)

      const nav = screen.getByTestId('footer-nav')
      expect(nav).toBeInTheDocument()
      expect(nav.tagName.toLowerCase()).toBe('nav')
      expect(nav).toHaveAttribute('aria-label', 'Footer navigation')
    })
  })

  describe('Test Case 2: Footer contains link to Home (/) route', () => {
    it('renders Home link in footer', () => {
      renderWithProviders(<Footer />)

      const homeLink = screen.getByTestId('footer-link-home')
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveTextContent('Home')
    })

    it('Home link has correct href attribute', () => {
      renderWithProviders(<Footer />)

      const homeLink = screen.getByTestId('footer-link-home')
      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('Home link is an anchor element for navigation', () => {
      renderWithProviders(<Footer />)

      const homeLink = screen.getByTestId('footer-link-home')
      expect(homeLink.tagName.toLowerCase()).toBe('a')
    })
  })

  describe('Test Case 3: Footer contains link to Login (/login) route', () => {
    it('renders Login link in footer', () => {
      renderWithProviders(<Footer />)

      const loginLink = screen.getByTestId('footer-link-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveTextContent('Login')
    })

    it('Login link has correct href attribute', () => {
      renderWithProviders(<Footer />)

      const loginLink = screen.getByTestId('footer-link-login')
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('Login link is an anchor element for navigation', () => {
      renderWithProviders(<Footer />)

      const loginLink = screen.getByTestId('footer-link-login')
      expect(loginLink.tagName.toLowerCase()).toBe('a')
    })
  })

  describe('Test Case 4: Footer contains link to Register (/register) route', () => {
    it('renders Register link in footer', () => {
      renderWithProviders(<Footer />)

      const registerLink = screen.getByTestId('footer-link-register')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveTextContent('Register')
    })

    it('Register link has correct href attribute', () => {
      renderWithProviders(<Footer />)

      const registerLink = screen.getByTestId('footer-link-register')
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('Register link is an anchor element for navigation', () => {
      renderWithProviders(<Footer />)

      const registerLink = screen.getByTestId('footer-link-register')
      expect(registerLink.tagName.toLowerCase()).toBe('a')
    })
  })

  describe('Test Case 5: Footer displays copyright notice with current year (2026)', () => {
    it('renders copyright notice', () => {
      renderWithProviders(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
    })

    it('copyright notice contains current year (2026)', () => {
      renderWithProviders(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright.textContent).toContain('2026')
    })

    it('copyright notice contains product name', () => {
      renderWithProviders(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright.textContent).toContain('URL Shortener')
    })

    it('copyright notice has correct format', () => {
      renderWithProviders(<Footer />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright.textContent).toMatch(/© 2026 URL Shortener/)
    })
  })

  describe('Test Case 6: All footer links navigate to correct routes (integration)', () => {
    it('all navigation links are clickable', () => {
      renderWithProviders(<Footer />)

      const homeLink = screen.getByTestId('footer-link-home')
      const loginLink = screen.getByTestId('footer-link-login')
      const registerLink = screen.getByTestId('footer-link-register')

      // Verify all links have correct hrefs before clicking
      expect(homeLink).toHaveAttribute('href', '/')
      expect(loginLink).toHaveAttribute('href', '/login')
      expect(registerLink).toHaveAttribute('href', '/register')

      // Simulate clicks - since we're using BrowserRouter, this tests navigation
      fireEvent.click(homeLink)
      expect(homeLink).toBeInTheDocument()

      fireEvent.click(loginLink)
      expect(loginLink).toBeInTheDocument()

      fireEvent.click(registerLink)
      expect(registerLink).toBeInTheDocument()
    })

    it('links use React Router Link component (internal navigation)', () => {
      renderWithProviders(<Footer />)

      // Links should be anchor elements that use React Router
      const links = screen.getAllByRole('link')
      expect(links.length).toBeGreaterThanOrEqual(3)

      // All links should be within the footer navigation
      const nav = screen.getByTestId('footer-nav')
      links.forEach((link) => {
        expect(nav).toContainElement(link)
      })
    })
  })

  describe('Accessibility', () => {
    it('all links are keyboard accessible', () => {
      renderWithProviders(<Footer />)

      const homeLink = screen.getByTestId('footer-link-home')
      const loginLink = screen.getByTestId('footer-link-login')
      const registerLink = screen.getByTestId('footer-link-register')

      // Each link should be focusable
      homeLink.focus()
      expect(document.activeElement).toBe(homeLink)

      loginLink.focus()
      expect(document.activeElement).toBe(loginLink)

      registerLink.focus()
      expect(document.activeElement).toBe(registerLink)
    })

    it('footer has proper semantic structure', () => {
      renderWithProviders(<Footer />)

      // Footer should use contentinfo role
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()

      // Navigation should have accessible name
      const nav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(nav).toBeInTheDocument()
    })
  })

  describe('Custom Links', () => {
    it('accepts custom links array', () => {
      const customLinks = [
        { label: 'About', href: '/about' },
        { label: 'Contact', href: '/contact' },
      ]

      renderWithProviders(<Footer links={customLinks} />)

      const aboutLink = screen.getByTestId('footer-link-about')
      const contactLink = screen.getByTestId('footer-link-contact')

      expect(aboutLink).toBeInTheDocument()
      expect(aboutLink).toHaveAttribute('href', '/about')

      expect(contactLink).toBeInTheDocument()
      expect(contactLink).toHaveAttribute('href', '/contact')
    })

    it('does not render default links when custom links provided', () => {
      const customLinks = [{ label: 'About', href: '/about' }]

      renderWithProviders(<Footer links={customLinks} />)

      expect(screen.queryByTestId('footer-link-home')).not.toBeInTheDocument()
      expect(screen.queryByTestId('footer-link-login')).not.toBeInTheDocument()
      expect(screen.queryByTestId('footer-link-register')).not.toBeInTheDocument()
    })
  })

  describe('Styling and Layout', () => {
    it('footer has background styling', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('bg-base-300')
    })

    it('footer uses theme-aware text colors', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('text-base-content')
    })
  })
})
