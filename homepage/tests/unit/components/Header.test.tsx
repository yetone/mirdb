/**
 * Unit tests for Header component.
 * Owner: Scenario 1 - Header Section Implementation
 *
 * Test cases:
 * - Logo has descriptive alt text
 * - Header renders with correct structure
 * - Navigation links render correctly
 * - CTA button renders with correct attributes
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Header } from '../../../src/components/layout/Header'

const mockNavLinks = [
  { label: 'Features', href: '#features' },
  { label: 'Pricing', href: '#pricing' },
  { label: 'About', href: '#about' },
]

const mockCtaButton = {
  label: 'Get Started',
  href: '#signup',
  variant: 'primary' as const,
  size: 'md' as const,
}

describe('Header Component', () => {
  it('should render product logo with descriptive alt text for accessibility', () => {
    const productName = 'TestProduct'

    render(
      <Header
        productName={productName}
        logo="/logo.svg"
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    const logo = screen.getByTestId('header-logo')
    expect(logo).toBeInTheDocument()
    expect(logo).toHaveAttribute('alt', `${productName} logo`)
  })

  it('should render product name correctly', () => {
    const productName = 'MyAwesomeProduct'

    render(
      <Header
        productName={productName}
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    expect(screen.getByText(productName)).toBeInTheDocument()
  })

  it('should render tagline when provided', () => {
    const tagline = 'Your workflow, simplified'

    render(
      <Header
        productName="TestProduct"
        tagline={tagline}
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    expect(screen.getByText(tagline)).toBeInTheDocument()
  })

  it('should render navigation links', () => {
    render(
      <Header
        productName="TestProduct"
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    mockNavLinks.forEach((link) => {
      expect(screen.getByText(link.label)).toBeInTheDocument()
    })
  })

  it('should render CTA button with correct label', () => {
    render(
      <Header
        productName="TestProduct"
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    const ctaButton = screen.getByTestId('header-cta')
    expect(ctaButton).toBeInTheDocument()
    expect(ctaButton).toHaveTextContent(mockCtaButton.label)
    expect(ctaButton).toHaveAttribute('href', mockCtaButton.href)
  })

  it('should render header with banner role for accessibility', () => {
    render(
      <Header
        productName="TestProduct"
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    expect(screen.getByRole('banner')).toBeInTheDocument()
  })

  it('should render hamburger menu button', () => {
    render(
      <Header
        productName="TestProduct"
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    const hamburgerButton = screen.getByTestId('hamburger-menu-button')
    expect(hamburgerButton).toBeInTheDocument()
    expect(hamburgerButton).toHaveAttribute('aria-label')
  })

  it('should have main navigation with proper aria-label', () => {
    render(
      <Header
        productName="TestProduct"
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    const nav = screen.getByRole('navigation', { name: 'Main navigation' })
    expect(nav).toBeInTheDocument()
  })

  it('should not render logo when not provided', () => {
    render(
      <Header
        productName="TestProduct"
        navLinks={mockNavLinks}
        ctaButton={mockCtaButton}
      />
    )

    expect(screen.queryByTestId('header-logo')).not.toBeInTheDocument()
  })
})
