/**
 * Footer Component Tests
 * Owner: Scenario 12 - Footer Display
 *
 * Unit tests for the Footer component.
 * Tests cover:
 * - Footer section is rendered at bottom of page
 * - Footer contains product name or branding
 * - Footer uses semantic <footer> HTML element
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Footer } from '../../../../src/components/home/Footer'

// Helper to render Footer with Router context
function renderFooter() {
  return render(
    <BrowserRouter>
      <Footer />
    </BrowserRouter>
  )
}

describe('Footer', () => {
  // Test Case 1: Footer section is rendered at bottom of page
  it('renders footer section at the bottom of the page', () => {
    renderFooter()

    // Check that footer is rendered with proper test ID
    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()

    // Verify it's in the document and visible
    expect(footer).toBeVisible()
  })

  // Test Case 2: Footer contains product name or branding
  it('contains product name or branding', () => {
    renderFooter()

    // Check for product name heading
    const productName = screen.getByRole('heading', { name: /url shortener/i })
    expect(productName).toBeInTheDocument()

    // Check for product tagline/description
    const tagline = screen.getByText(/shorten, share, and track your links/i)
    expect(tagline).toBeInTheDocument()
  })

  // Test Case 3: Footer uses semantic <footer> HTML element
  it('uses semantic <footer> HTML element', () => {
    renderFooter()

    // Check for semantic footer element with proper role
    const footer = screen.getByRole('contentinfo')
    expect(footer).toBeInTheDocument()

    // Verify it has aria-label for accessibility
    expect(footer).toHaveAttribute('aria-label', 'Footer')

    // Verify it's actually a <footer> element
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  // Additional tests for comprehensive coverage

  it('contains navigation links', () => {
    renderFooter()

    // Check for footer navigation
    const nav = screen.getByRole('navigation', { name: /footer navigation/i })
    expect(nav).toBeInTheDocument()

    // Check for specific links
    const homeLink = screen.getByRole('link', { name: /home/i })
    expect(homeLink).toBeInTheDocument()
    expect(homeLink).toHaveAttribute('href', '/')

    const loginLink = screen.getByRole('link', { name: /login/i })
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')

    const registerLink = screen.getByRole('link', { name: /register/i })
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  it('displays copyright information with current year', () => {
    renderFooter()

    const currentYear = new Date().getFullYear()
    const copyright = screen.getByText(
      new RegExp(`© ${currentYear} URL Shortener. All rights reserved.`, 'i')
    )
    expect(copyright).toBeInTheDocument()
  })

  it('displays features information', () => {
    renderFooter()

    // Check for features section
    const featuresHeading = screen.getByRole('heading', { name: /features/i })
    expect(featuresHeading).toBeInTheDocument()

    // Check for feature items
    expect(screen.getByText(/analytics tracking/i)).toBeInTheDocument()
    expect(screen.getByText(/dashboard management/i)).toBeInTheDocument()
    expect(screen.getByText(/theme support/i)).toBeInTheDocument()
  })

  it('has proper accessibility attributes', () => {
    renderFooter()

    const footer = screen.getByTestId('footer-section')

    // Check for aria-label
    expect(footer).toHaveAttribute('aria-label', 'Footer')

    // Check that links are accessible
    const links = screen.getAllByRole('link')
    links.forEach((link) => {
      expect(link).toHaveAccessibleName()
    })
  })

  it('has proper responsive layout classes', () => {
    renderFooter()

    const footer = screen.getByTestId('footer-section')

    // Check for responsive padding/layout classes
    expect(footer.className).toContain('py-8')
    expect(footer.className).toContain('px-4')
    expect(footer.className).toContain('bg-base-200')
  })
})
