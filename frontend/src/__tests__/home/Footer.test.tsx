/**
 * Footer Tests
 * Owner: Scenario 10 - Footer Section
 *
 * Test coverage:
 * - Footer renders at page bottom
 * - Copyright text present
 * - Quick links present and functional
 */
import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { Footer } from '../../components/home/Footer'

describe('Footer', () => {
  describe('Test Case 1: Footer renders at bottom of homepage', () => {
    it('should render the footer component', () => {
      renderWithProviders(<Footer />)

      // Check that the footer element exists
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('should have correct test id for targeting', () => {
      renderWithProviders(<Footer />)

      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should have proper aria-label for accessibility', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toHaveAttribute('aria-label', 'Site footer')
    })
  })

  describe('Test Case 2: Footer contains copyright information', () => {
    it('should display copyright text with current year', () => {
      renderWithProviders(<Footer />)

      const currentYear = new Date().getFullYear()
      const copyrightElement = screen.getByTestId('copyright')

      expect(copyrightElement).toBeInTheDocument()
      expect(copyrightElement).toHaveTextContent(`${currentYear}`)
      expect(copyrightElement).toHaveTextContent(/URL Shortener/i)
      expect(copyrightElement).toHaveTextContent(/All rights reserved/i)
    })

    it('should have copyright symbol', () => {
      renderWithProviders(<Footer />)

      const copyrightElement = screen.getByTestId('copyright')
      expect(copyrightElement.textContent).toContain('©')
    })
  })

  describe('Test Case 3: Footer contains quick links to Login and Register pages', () => {
    it('should render Login link', () => {
      renderWithProviders(<Footer />)

      const loginLink = screen.getByTestId('footer-login-link')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveTextContent('Login')
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should render Register link', () => {
      renderWithProviders(<Footer />)

      const registerLink = screen.getByTestId('footer-register-link')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveTextContent('Register')
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should have footer navigation with proper aria-label', () => {
      renderWithProviders(<Footer />)

      const nav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(nav).toBeInTheDocument()
    })

    it('should contain both Login and Register links in the navigation', () => {
      renderWithProviders(<Footer />)

      const nav = screen.getByRole('navigation', { name: /footer navigation/i })
      const links = nav.querySelectorAll('a')

      expect(links).toHaveLength(2)
      expect(links[0]).toHaveAttribute('href', '/login')
      expect(links[1]).toHaveAttribute('href', '/register')
    })
  })

  describe('Additional coverage: Accessibility', () => {
    it('should have accessible link names', () => {
      renderWithProviders(<Footer />)

      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /register/i })).toBeInTheDocument()
    })
  })
})
