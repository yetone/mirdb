/**
 * Unit tests for Footer component.
 * Owner: Scenario 6 - Footer Links and Legal Information
 *
 * Test cases:
 * 1. Footer section is visible at bottom of homepage
 * 2. Privacy Policy link is present
 * 3. Terms of Service link is present
 * 4. Footer contains links to Home, Login, and Register
 * 5. Copyright notice with current year is displayed
 */

import React from 'react'
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { Footer } from '@/components/layout/Footer'

// Wrapper component for Router context
const TestWrapper = ({ children }: { children: React.ReactNode }) => (
  <BrowserRouter>{children}</BrowserRouter>
)

describe('Footer', () => {
  /**
   * Test Case 1: Footer section is visible at bottom of homepage
   */
  it('renders footer section visible on the page', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    // Footer should be present
    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()
    expect(footer).toBeVisible()

    // Footer should have correct role
    expect(footer).toHaveAttribute('role', 'contentinfo')
    expect(footer).toHaveAttribute('aria-label', 'Site footer')
  })

  /**
   * Test Case 2: Privacy Policy link is present
   */
  it('displays Privacy Policy link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    // Privacy Policy link should be present
    const privacyLink = screen.getByTestId('footer-link-privacy')
    expect(privacyLink).toBeInTheDocument()
    expect(privacyLink).toBeVisible()
    expect(privacyLink).toHaveTextContent('Privacy Policy')
    expect(privacyLink).toHaveAttribute('href', '/privacy')
  })

  /**
   * Test Case 3: Terms of Service link is present
   */
  it('displays Terms of Service link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    // Terms of Service link should be present
    const termsLink = screen.getByTestId('footer-link-terms')
    expect(termsLink).toBeInTheDocument()
    expect(termsLink).toBeVisible()
    expect(termsLink).toHaveTextContent('Terms of Service')
    expect(termsLink).toHaveAttribute('href', '/terms')
  })

  /**
   * Test Case 4: Footer contains links to Home, Login, and Register
   */
  it('contains navigation links to Home, Login, and Register', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    // Navigation section should be present
    const navSection = screen.getByTestId('footer-nav')
    expect(navSection).toBeInTheDocument()
    expect(navSection).toHaveAttribute('aria-label', 'Footer navigation')

    // Home link should be present
    const homeLink = screen.getByTestId('footer-link-home')
    expect(homeLink).toBeInTheDocument()
    expect(homeLink).toBeVisible()
    expect(homeLink).toHaveTextContent('Home')
    expect(homeLink).toHaveAttribute('href', '/')

    // Login link should be present
    const loginLink = screen.getByTestId('footer-link-login')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toBeVisible()
    expect(loginLink).toHaveTextContent('Login')
    expect(loginLink).toHaveAttribute('href', '/login')

    // Register link should be present
    const registerLink = screen.getByTestId('footer-link-register')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toBeVisible()
    expect(registerLink).toHaveTextContent('Register')
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  /**
   * Test Case 5: Copyright notice with current year is displayed
   */
  it('displays copyright notice with current year', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    // Copyright section should be present
    const copyrightSection = screen.getByTestId('footer-copyright')
    expect(copyrightSection).toBeInTheDocument()
    expect(copyrightSection).toBeVisible()

    // Should contain the current year
    const currentYear = new Date().getFullYear().toString()
    expect(copyrightSection).toHaveTextContent(currentYear)

    // Should contain copyright text
    expect(copyrightSection).toHaveTextContent('Copyright')
    expect(copyrightSection).toHaveTextContent('URLShort')
    expect(copyrightSection).toHaveTextContent('All rights reserved')
  })

  /**
   * Additional test: Legal links section has proper aria label
   */
  it('legal links section has proper accessibility attributes', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const legalSection = screen.getByTestId('footer-legal')
    expect(legalSection).toBeInTheDocument()
    expect(legalSection).toHaveAttribute('aria-label', 'Legal links')
  })

  /**
   * Additional test: All links are clickable and have proper styling classes
   */
  it('all links have proper styling classes', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const allLinks = [
      screen.getByTestId('footer-link-home'),
      screen.getByTestId('footer-link-login'),
      screen.getByTestId('footer-link-register'),
      screen.getByTestId('footer-link-privacy'),
      screen.getByTestId('footer-link-terms'),
    ]

    allLinks.forEach((link) => {
      expect(link).toHaveClass('link')
      expect(link).toHaveClass('link-hover')
    })
  })

  /**
   * Additional test: Footer is a proper semantic footer element
   */
  it('uses semantic footer element', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const footer = screen.getByTestId('footer')
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  /**
   * Additional test: Navigation links use correct routing
   */
  it('navigation links have correct routing paths', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Footer />
      </MemoryRouter>
    )

    // Verify all navigation links have correct hrefs
    expect(screen.getByTestId('footer-link-home')).toHaveAttribute('href', '/')
    expect(screen.getByTestId('footer-link-login')).toHaveAttribute('href', '/login')
    expect(screen.getByTestId('footer-link-register')).toHaveAttribute('href', '/register')
    expect(screen.getByTestId('footer-link-privacy')).toHaveAttribute('href', '/privacy')
    expect(screen.getByTestId('footer-link-terms')).toHaveAttribute('href', '/terms')
  })
})
