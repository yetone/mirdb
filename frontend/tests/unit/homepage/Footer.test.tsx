/**
 * Footer Unit Tests
 * Owner: Scenario 7 - Footer Section
 *
 * Tests for the Footer component to verify:
 * - Footer element exists in the DOM
 * - Copyright text with current year is present
 * - Privacy Policy link exists
 * - Terms of Service link exists
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from './setup'
import { Footer } from '@/components/homepage/Footer'

describe('Footer', () => {
  it('renders the footer element in the DOM', () => {
    render(<Footer />)

    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
  })

  it('renders copyright text with current year', () => {
    render(<Footer />)

    const currentYear = new Date().getFullYear()
    const copyrightText = screen.getByTestId('copyright-text')

    expect(copyrightText).toBeInTheDocument()
    expect(copyrightText).toHaveTextContent(`© ${currentYear}`)
    expect(copyrightText).toHaveTextContent('URL Shortener')
  })

  it('renders Privacy Policy link', () => {
    render(<Footer />)

    const privacyLink = screen.getByTestId('privacy-policy-link')
    expect(privacyLink).toBeInTheDocument()
    expect(privacyLink).toHaveTextContent('Privacy Policy')
    expect(privacyLink).toHaveAttribute('href', '/privacy')
  })

  it('renders Terms of Service link', () => {
    render(<Footer />)

    const termsLink = screen.getByTestId('terms-of-service-link')
    expect(termsLink).toBeInTheDocument()
    expect(termsLink).toHaveTextContent('Terms of Service')
    expect(termsLink).toHaveAttribute('href', '/terms')
  })

  it('footer has proper role attribute for accessibility', () => {
    render(<Footer />)

    const footer = screen.getByRole('contentinfo')
    expect(footer).toBeInTheDocument()
  })

  it('footer contains navigation with aria-label', () => {
    render(<Footer />)

    const nav = screen.getByRole('navigation', { name: /footer navigation/i })
    expect(nav).toBeInTheDocument()
  })

  it('footer links container contains both legal links', () => {
    render(<Footer />)

    const footerLinks = screen.getByTestId('footer-links')
    expect(footerLinks).toBeInTheDocument()

    const privacyLink = screen.getByTestId('privacy-policy-link')
    const termsLink = screen.getByTestId('terms-of-service-link')

    expect(footerLinks).toContainElement(privacyLink)
    expect(footerLinks).toContainElement(termsLink)
  })
})
