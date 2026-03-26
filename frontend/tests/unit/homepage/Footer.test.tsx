/**
 * Unit Tests for Footer Component
 * Owner: Scenario 6 - Footer Component
 *
 * Tests:
 * - Footer is visible at bottom of page
 * - Copyright information with current year is displayed
 * - Privacy Policy link is present
 * - Terms of Service link is present
 *
 * Requirements: REQ-6
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Footer } from '../../../src/components/homepage/Footer'

describe('Footer', () => {
  let originalDate: DateConstructor

  beforeEach(() => {
    originalDate = global.Date
    const mockDate = new Date('2026-03-26T12:00:00Z')
    vi.useFakeTimers()
    vi.setSystemTime(mockDate)
  })

  afterEach(() => {
    vi.useRealTimers()
    global.Date = originalDate
  })

  it('renders footer component that is visible at bottom of page', () => {
    render(<Footer />)

    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()

    // Check footer is a footer element
    expect(footer.tagName.toLowerCase()).toBe('footer')

    // Check footer has mt-auto class for bottom positioning
    expect(footer.className).toContain('mt-auto')
  })

  it('displays copyright information with current year', () => {
    render(<Footer />)

    const copyright = screen.getByTestId('copyright')
    expect(copyright).toBeInTheDocument()

    // Check that copyright contains the current year (2026)
    expect(copyright.textContent).toContain('2026')

    // Check for copyright symbol or text
    const text = copyright.textContent ?? ''
    const hasCopyrightSymbol = text.includes('©')
    const hasCopyrightText = text.toLowerCase().includes('copyright')

    expect(hasCopyrightSymbol || hasCopyrightText).toBe(true)
  })

  it('renders Privacy Policy link that is present and accessible', () => {
    render(<Footer />)

    const privacyLink = screen.getByTestId('privacy-link')
    expect(privacyLink).toBeInTheDocument()

    // Check link text contains "Privacy"
    expect(privacyLink.textContent?.toLowerCase()).toContain('privacy')

    // Check it's an anchor element with href
    expect(privacyLink.tagName.toLowerCase()).toBe('a')
    expect(privacyLink).toHaveAttribute('href')
  })

  it('renders Terms of Service link that is present and accessible', () => {
    render(<Footer />)

    const termsLink = screen.getByTestId('terms-link')
    expect(termsLink).toBeInTheDocument()

    // Check link text contains "Terms"
    expect(termsLink.textContent?.toLowerCase()).toContain('terms')

    // Check it's an anchor element with href
    expect(termsLink.tagName.toLowerCase()).toBe('a')
    expect(termsLink).toHaveAttribute('href')
  })

  it('has footer links within a navigation container', () => {
    render(<Footer />)

    const footerLinks = screen.getByTestId('footer-links')
    expect(footerLinks).toBeInTheDocument()

    // Check it's a nav element for accessibility
    expect(footerLinks.tagName.toLowerCase()).toBe('nav')

    // Check for aria-label for accessibility
    expect(footerLinks).toHaveAttribute('aria-label')
  })

  it('accepts optional className prop', () => {
    render(<Footer className="custom-class" />)

    const footer = screen.getByTestId('footer')
    expect(footer.className).toContain('custom-class')
  })
})
