/**
 * Unit tests for Footer component.
 * Owner: Scenario 6 - Footer Rendering
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '../../../setup'
import { Footer } from '@/components/homepage/Footer'

describe('Footer', () => {
  const originalDate = global.Date

  beforeEach(() => {
    // Mock current year to 2026 for consistent testing
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-03-28'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // Test Case 1: Footer element is present at the bottom of the page
  it('renders footer element with correct semantic markup', () => {
    render(<Footer />)

    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  // Test Case 2: Footer contains product branding/logo
  it('displays product branding with logo', () => {
    render(<Footer />)

    const branding = screen.getByTestId('footer-branding')
    expect(branding).toBeInTheDocument()
    expect(branding).toHaveTextContent('URL Shortener')
  })

  it('displays custom brand name when provided', () => {
    render(<Footer brandName="Custom Brand" />)

    const branding = screen.getByTestId('footer-branding')
    expect(branding).toHaveTextContent('Custom Brand')
  })

  // Test Case 3: Privacy Policy link is present in footer
  it('displays Privacy Policy link', () => {
    render(<Footer />)

    const privacyLink = screen.getByTestId('footer-link-privacy-policy')
    expect(privacyLink).toBeInTheDocument()
    expect(privacyLink).toHaveTextContent('Privacy Policy')
    expect(privacyLink).toHaveAttribute('href', '/privacy')
  })

  // Test Case 4: Terms of Service link is present in footer
  it('displays Terms of Service link', () => {
    render(<Footer />)

    const termsLink = screen.getByTestId('footer-link-terms-of-service')
    expect(termsLink).toBeInTheDocument()
    expect(termsLink).toHaveTextContent('Terms of Service')
    expect(termsLink).toHaveAttribute('href', '/terms')
  })

  // Test Case 5: Contact link is present in footer
  it('displays Contact link', () => {
    render(<Footer />)

    const contactLink = screen.getByTestId('footer-link-contact')
    expect(contactLink).toBeInTheDocument()
    expect(contactLink).toHaveTextContent('Contact')
    expect(contactLink).toHaveAttribute('href', '/contact')
  })

  // Test Case 6: Copyright notice with current year is displayed
  it('displays copyright notice with current year', () => {
    render(<Footer />)

    const copyright = screen.getByTestId('footer-copyright')
    expect(copyright).toBeInTheDocument()
    expect(copyright).toHaveTextContent('Copyright © 2026')
    expect(copyright).toHaveTextContent('URL Shortener')
    expect(copyright).toHaveTextContent('All rights reserved')
  })

  it('displays copyright with custom brand name', () => {
    render(<Footer brandName="My Brand" />)

    const copyright = screen.getByTestId('footer-copyright')
    expect(copyright).toHaveTextContent('My Brand')
  })

  // Additional tests for completeness
  it('renders all legal links in navigation', () => {
    render(<Footer />)

    const footerLinks = screen.getByTestId('footer-links')
    expect(footerLinks).toBeInTheDocument()
    expect(footerLinks.tagName.toLowerCase()).toBe('nav')
    expect(footerLinks).toHaveAttribute('aria-label', 'Footer navigation')
  })

  it('applies correct DaisyUI footer classes', () => {
    render(<Footer />)

    const footer = screen.getByTestId('footer')
    expect(footer).toHaveClass('footer')
    expect(footer).toHaveClass('footer-center')
    expect(footer).toHaveClass('bg-base-200')
  })

  it('renders links with hover styling', () => {
    render(<Footer />)

    const privacyLink = screen.getByTestId('footer-link-privacy-policy')
    expect(privacyLink).toHaveClass('link')
    expect(privacyLink).toHaveClass('link-hover')
  })

  it('accepts custom links via props', () => {
    const customLinks = [
      { label: 'Custom Link', href: '/custom' },
      { label: 'Another Link', href: '/another' },
    ]

    render(<Footer links={customLinks} />)

    const customLink = screen.getByTestId('footer-link-custom-link')
    expect(customLink).toBeInTheDocument()
    expect(customLink).toHaveAttribute('href', '/custom')

    const anotherLink = screen.getByTestId('footer-link-another-link')
    expect(anotherLink).toBeInTheDocument()
  })

  it('does not render social links by default', () => {
    render(<Footer />)

    const socialLinks = screen.queryByTestId('footer-social')
    expect(socialLinks).not.toBeInTheDocument()
  })

  it('renders social links when showSocialLinks is true', () => {
    render(<Footer showSocialLinks={true} />)

    const socialLinks = screen.getByTestId('footer-social')
    expect(socialLinks).toBeInTheDocument()
  })
})
