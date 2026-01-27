import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter, MemoryRouter, useLocation } from 'react-router-dom'
import { HeroSection } from '../../../src/components/home/HeroSection'

// Helper component to track navigation
function LocationDisplay() {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

// Test wrapper with necessary providers
function TestWrapper({ children }: { children: React.ReactNode }) {
  return <BrowserRouter>{children}</BrowserRouter>
}

function TestWrapperWithMemoryRouter({
  children,
  initialEntries = ['/'],
}: {
  children: React.ReactNode
  initialEntries?: string[]
}) {
  return (
    <MemoryRouter initialEntries={initialEntries}>
      {children}
      <LocationDisplay />
    </MemoryRouter>
  )
}

describe('HeroSection', () => {
  // Test Case 1: Hero section renders with headline visible
  it('renders hero section with headline visible when navigating to "/" route', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()
    expect(heroSection).toBeVisible()

    // Headline is rendered (animation may start with opacity 0 but element exists)
    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeInTheDocument()
    expect(headline).toHaveTextContent(/shorten/i)
  })

  // Test Case 2: Main headline contains value proposition text about URL shortening
  it('displays headline with value proposition about URL shortening', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const headline = screen.getByTestId('hero-headline')
    expect(headline).toHaveTextContent(/shorten.*url/i)
    expect(headline).toHaveTextContent(/track.*performance/i)
  })

  // Test Case 3: Subheadline provides supporting details about the service
  it('displays subheadline with supporting details about the service', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline).toHaveTextContent(/analytics/i)
    expect(subheadline).toHaveTextContent(/track/i)
  })

  // Test Case 4: Primary CTA button with text 'Get Started' or 'Sign Up' is present
  it('renders primary CTA button with "Get Started" or "Sign Up" text', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const ctaButton = screen.getByTestId('hero-cta-primary')
    expect(ctaButton).toBeInTheDocument()
    expect(ctaButton).toHaveTextContent(/get started|sign up/i)
  })

  // Test Case 5: Secondary login link is present with appropriate text
  it('renders secondary login link with appropriate text', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const loginLink = screen.getByTestId('hero-login-link')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveTextContent(/log in/i)
    expect(loginLink).toHaveTextContent(/already have an account/i)
  })

  // Test Case 6: Clicking primary CTA navigates to /register
  it('navigates to /register when clicking primary CTA button', () => {
    render(
      <TestWrapperWithMemoryRouter initialEntries={['/']}>
        <HeroSection />
      </TestWrapperWithMemoryRouter>
    )

    const ctaButton = screen.getByTestId('hero-cta-primary')
    fireEvent.click(ctaButton)

    const locationDisplay = screen.getByTestId('location-display')
    expect(locationDisplay).toHaveTextContent('/register')
  })

  // Test Case 7: Clicking login link navigates to /login
  it('navigates to /login when clicking login link', () => {
    render(
      <TestWrapperWithMemoryRouter initialEntries={['/']}>
        <HeroSection />
      </TestWrapperWithMemoryRouter>
    )

    const loginLink = screen.getByTestId('hero-login-link')
    fireEvent.click(loginLink)

    const locationDisplay = screen.getByTestId('location-display')
    expect(locationDisplay).toHaveTextContent('/login')
  })

  // Test Case 8: Background visual effect renders without errors
  it('renders BackgroundEffect without errors', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  // Additional tests for robustness
  it('has correct structure with proper accessibility attributes', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    // Hero section is a semantic section element
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection.tagName.toLowerCase()).toBe('section')

    // CTA link is accessible
    const ctaButton = screen.getByTestId('hero-cta-primary')
    expect(ctaButton).toHaveAttribute('href', '/register')
  })
})
