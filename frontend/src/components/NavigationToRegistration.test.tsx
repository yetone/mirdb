/**
 * Navigation to Registration - Scenario Tests
 *
 * This test file covers the scenario: "Verify users can navigate from homepage
 * to registration page via CTA buttons as specified in US-4"
 *
 * Test Cases:
 * 1. E2E: Click 'Get Started Free' button in hero section → navigates to /register
 * 2. Integration: Verify button uses React Router Link → no full page reload
 * 3. E2E: Check Get Started button in footer CTA → navigates to /register
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter, MemoryRouter, useLocation } from 'react-router-dom'
import HeroSection from './HeroSection'
import FooterCTA from './FooterCTA'
import { ThemeProvider } from '../contexts/ThemeContext'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h1 {...props}>{children}</h1>
    ),
    h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h2 {...props}>{children}</h2>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
    nav: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <nav {...props}>{children}</nav>
    ),
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
  },
}))

const renderWithProviders = (component: React.ReactNode, initialRoute = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <ThemeProvider>
        {component}
      </ThemeProvider>
    </MemoryRouter>
  )
}

// Helper component to track location changes
function LocationDisplay() {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

describe('Navigation to Registration Scenario', () => {
  /**
   * Test Case 1: E2E Test
   * Input: Click 'Get Started Free' button in hero section
   * Expected: User is navigated to /register route
   */
  describe('Test Case 1: Hero Section CTA Navigation', () => {
    it('Get Started Free button in hero section navigates to /register', () => {
      renderWithProviders(
        <>
          <HeroSection />
          <LocationDisplay />
        </>
      )

      const getStartedButton = screen.getByTestId('cta-get-started')
      expect(getStartedButton).toBeInTheDocument()
      expect(getStartedButton).toHaveTextContent('Get Started Free')

      // Verify the button links to /register
      expect(getStartedButton).toHaveAttribute('href', '/register')
    })

    it('hero CTA button is clickable and triggers navigation', () => {
      renderWithProviders(
        <>
          <HeroSection />
          <LocationDisplay />
        </>
      )

      const getStartedButton = screen.getByTestId('cta-get-started')

      // Click the button
      fireEvent.click(getStartedButton)

      // Verify navigation occurred (location changed to /register)
      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/register')
    })

    it('hero CTA is prominently displayed in the hero section', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const getStartedButton = screen.getByTestId('cta-get-started')

      // Verify the button is within the hero section
      expect(heroSection).toContainElement(getStartedButton)

      // Verify it has large size styling
      expect(getStartedButton).toHaveClass('px-8', 'py-4', 'text-lg')
    })
  })

  /**
   * Test Case 2: Integration Test
   * Input: Verify button uses React Router Link
   * Expected: Navigation occurs without full page reload
   */
  describe('Test Case 2: React Router Link Integration', () => {
    it('Get Started button uses React Router Link (not regular anchor)', () => {
      renderWithProviders(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')

      // The button should be an anchor element when using Link component
      expect(getStartedButton.tagName.toLowerCase()).toBe('a')

      // It should have href attribute (React Router Link renders as <a>)
      expect(getStartedButton).toHaveAttribute('href', '/register')

      // Verify it's not an external link (no target attribute)
      expect(getStartedButton).not.toHaveAttribute('target')
    })

    it('navigation does not trigger full page reload (SPA navigation)', () => {
      renderWithProviders(
        <>
          <HeroSection />
          <LocationDisplay />
        </>
      )

      // Get initial location
      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/')

      // Click the Get Started button
      const getStartedButton = screen.getByTestId('cta-get-started')
      fireEvent.click(getStartedButton)

      // After click, location should change without page reload
      // (If it was a full reload, the component would remount and location would be lost)
      expect(screen.getByTestId('location-display')).toHaveTextContent('/register')
    })

    it('Login button also uses React Router Link', () => {
      renderWithProviders(<HeroSection />)

      const loginButton = screen.getByTestId('cta-login')

      expect(loginButton.tagName.toLowerCase()).toBe('a')
      expect(loginButton).toHaveAttribute('href', '/login')
    })
  })

  /**
   * Test Case 3: E2E Test
   * Input: Check Get Started button in footer CTA
   * Expected: Footer CTA button also navigates to /register
   */
  describe('Test Case 3: Footer CTA Navigation', () => {
    it('Get Started button exists in footer CTA section', () => {
      renderWithProviders(<FooterCTA />)

      const footerCtaButton = screen.getByTestId('footer-cta-get-started')
      expect(footerCtaButton).toBeInTheDocument()
      expect(footerCtaButton).toHaveTextContent('Get Started Free')
    })

    it('footer CTA button navigates to /register route', () => {
      renderWithProviders(<FooterCTA />)

      const footerCtaButton = screen.getByTestId('footer-cta-get-started')
      expect(footerCtaButton).toHaveAttribute('href', '/register')
    })

    it('footer CTA button triggers SPA navigation when clicked', () => {
      renderWithProviders(
        <>
          <FooterCTA />
          <LocationDisplay />
        </>
      )

      const footerCtaButton = screen.getByTestId('footer-cta-get-started')

      // Click the button
      fireEvent.click(footerCtaButton)

      // Verify navigation occurred
      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/register')
    })

    it('footer CTA uses React Router Link (not regular anchor)', () => {
      renderWithProviders(<FooterCTA />)

      const footerCtaButton = screen.getByTestId('footer-cta-get-started')

      // Should be an anchor element rendered by React Router Link
      expect(footerCtaButton.tagName.toLowerCase()).toBe('a')

      // Should not be an external link
      expect(footerCtaButton).not.toHaveAttribute('target')
    })

    it('footer CTA has consistent styling with hero CTA', () => {
      renderWithProviders(
        <>
          <HeroSection />
          <FooterCTA />
        </>
      )

      const heroCta = screen.getByTestId('cta-get-started')
      const footerCta = screen.getByTestId('footer-cta-get-started')

      // Both should have primary styling
      expect(heroCta).toHaveClass('bg-primary')
      expect(footerCta).toHaveClass('bg-primary')

      // Both should have large size
      expect(heroCta).toHaveClass('px-8', 'py-4', 'text-lg')
      expect(footerCta).toHaveClass('px-8', 'py-4', 'text-lg')
    })
  })

  describe('Full Page Navigation Flow', () => {
    it('both hero and footer CTAs navigate to the same /register route', () => {
      renderWithProviders(
        <>
          <HeroSection />
          <FooterCTA />
        </>
      )

      const heroCta = screen.getByTestId('cta-get-started')
      const footerCta = screen.getByTestId('footer-cta-get-started')

      // Both should link to /register
      expect(heroCta).toHaveAttribute('href', '/register')
      expect(footerCta).toHaveAttribute('href', '/register')
    })
  })
})
