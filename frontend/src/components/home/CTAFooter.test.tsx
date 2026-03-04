/**
 * CTAFooter Component Tests
 * Owner: Scenario 2 - Navigation and CTA Buttons
 *
 * Test cases:
 * - Test case 6: Verify CTA footer 'Get Started' button navigates to /register
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { CTAFooter } from './CTAFooter'

describe('CTAFooter', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  /**
   * Test Case 6: Verify CTA footer 'Get Started' button
   * Expected: Secondary CTA at page bottom navigates to /register
   */
  it('should navigate to /register when Get Started button is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<CTAFooter />} />
          <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        </Routes>
      </MemoryRouter>
    )

    const ctaButton = screen.getByTestId('cta-footer-button')
    fireEvent.click(ctaButton)

    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  it('should render with default headline and subheading', () => {
    render(
      <MemoryRouter>
        <CTAFooter />
      </MemoryRouter>
    )

    expect(screen.getByTestId('cta-footer')).toBeInTheDocument()
    expect(screen.getByTestId('cta-footer-headline')).toHaveTextContent('Ready to Get Started?')
    expect(screen.getByTestId('cta-footer-subheading')).toHaveTextContent(
      'Join thousands of users who trust us with their URL shortening needs.'
    )
  })

  it('should render with custom headline and subheading', () => {
    render(
      <MemoryRouter>
        <CTAFooter
          headline="Custom Headline"
          subheading="Custom subheading text"
        />
      </MemoryRouter>
    )

    expect(screen.getByTestId('cta-footer-headline')).toHaveTextContent('Custom Headline')
    expect(screen.getByTestId('cta-footer-subheading')).toHaveTextContent('Custom subheading text')
  })

  it('should render Get Started button with default text', () => {
    render(
      <MemoryRouter>
        <CTAFooter />
      </MemoryRouter>
    )

    const ctaButton = screen.getByTestId('cta-footer-button')
    expect(ctaButton).toHaveTextContent('Get Started')
  })

  it('should render with custom CTA text', () => {
    render(
      <MemoryRouter>
        <CTAFooter ctaText="Sign Up Now" />
      </MemoryRouter>
    )

    const ctaButton = screen.getByTestId('cta-footer-button')
    expect(ctaButton).toHaveTextContent('Sign Up Now')
  })

  it('should navigate to custom ctaLink when provided', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<CTAFooter ctaLink="/custom-signup" />} />
          <Route path="/custom-signup" element={<div data-testid="custom-signup-page">Custom Signup</div>} />
        </Routes>
      </MemoryRouter>
    )

    const ctaButton = screen.getByTestId('cta-footer-button')
    fireEvent.click(ctaButton)

    expect(screen.getByTestId('custom-signup-page')).toBeInTheDocument()
  })

  it('should show login link by default', () => {
    render(
      <MemoryRouter>
        <CTAFooter />
      </MemoryRouter>
    )

    const loginLink = screen.getByTestId('cta-footer-login')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')
    expect(loginLink).toHaveTextContent('Already have an account? Login')
  })

  it('should hide login link when showLoginLink is false', () => {
    render(
      <MemoryRouter>
        <CTAFooter showLoginLink={false} />
      </MemoryRouter>
    )

    expect(screen.queryByTestId('cta-footer-login')).not.toBeInTheDocument()
  })

  it('should have proper accessibility attributes', () => {
    render(
      <MemoryRouter>
        <CTAFooter />
      </MemoryRouter>
    )

    const section = screen.getByTestId('cta-footer')
    expect(section).toHaveAttribute('aria-labelledby', 'cta-footer-headline')

    const ctaButton = screen.getByTestId('cta-footer-button')
    expect(ctaButton).toHaveAttribute('aria-label', 'Get Started - Create an account')
  })
})
