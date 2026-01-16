/**
 * Navigation to Login - Scenario Tests
 *
 * This test file covers the scenario: "Verify users can navigate from homepage
 * to login page as specified in US-5"
 *
 * Test Cases:
 * 1. E2E: Click 'Login' button in hero section → navigates to /login
 * 2. E2E: Click 'Login' in Navbar → navigates to /login
 * 3. Integration: Verify Login navigation is client-side (React Router)
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import HeroSection from './HeroSection'
import Navbar from './Navbar'
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
    header: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <header {...props}>{children}</header>
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

describe('Navigation to Login Scenario', () => {
  /**
   * Test Case 1: E2E Test
   * Input: Click 'Login' button in hero section
   * Expected: User is navigated to /login route
   */
  describe('Test Case 1: Hero Section Login Navigation', () => {
    it('Login button exists in hero section', () => {
      renderWithProviders(<HeroSection />)

      const loginButton = screen.getByTestId('cta-login')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveTextContent('Login')
    })

    it('Login button in hero section has correct href to /login', () => {
      renderWithProviders(<HeroSection />)

      const loginButton = screen.getByTestId('cta-login')
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('clicking Login button in hero section navigates to /login', () => {
      renderWithProviders(
        <>
          <HeroSection />
          <LocationDisplay />
        </>
      )

      const loginButton = screen.getByTestId('cta-login')
      fireEvent.click(loginButton)

      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/login')
    })

    it('hero Login button is prominently displayed alongside Get Started', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const loginButton = screen.getByTestId('cta-login')
      const getStartedButton = screen.getByTestId('cta-get-started')

      // Both buttons should be within the hero section
      expect(heroSection).toContainElement(loginButton)
      expect(heroSection).toContainElement(getStartedButton)

      // Login button should have large size styling
      expect(loginButton).toHaveClass('px-8', 'py-4', 'text-lg')
    })
  })

  /**
   * Test Case 2: E2E Test
   * Input: Click 'Login' in Navbar
   * Expected: User is navigated to /login route
   */
  describe('Test Case 2: Navbar Login Navigation', () => {
    it('Login link exists in Navbar', () => {
      renderWithProviders(<Navbar />)

      const navbarLogin = screen.getByTestId('navbar-login')
      expect(navbarLogin).toBeInTheDocument()
      expect(navbarLogin).toHaveTextContent('Login')
    })

    it('Navbar Login link has correct href to /login', () => {
      renderWithProviders(<Navbar />)

      const navbarLogin = screen.getByTestId('navbar-login')
      expect(navbarLogin).toHaveAttribute('href', '/login')
    })

    it('clicking Login in Navbar navigates to /login', () => {
      renderWithProviders(
        <>
          <Navbar />
          <LocationDisplay />
        </>
      )

      const navbarLogin = screen.getByTestId('navbar-login')
      fireEvent.click(navbarLogin)

      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/login')
    })

    it('Navbar Login is accessible alongside Get Started button', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      const navbarLogin = screen.getByTestId('navbar-login')
      const navbarGetStarted = screen.getByTestId('navbar-get-started')

      // Both elements should be in the navbar
      expect(navbar).toContainElement(navbarLogin)
      expect(navbar).toContainElement(navbarGetStarted)
    })
  })

  /**
   * Test Case 3: Integration Test
   * Input: Verify Login navigation is client-side
   * Expected: Navigation uses React Router without page reload
   */
  describe('Test Case 3: Client-Side Navigation Verification', () => {
    it('hero Login button uses React Router Link (renders as anchor)', () => {
      renderWithProviders(<HeroSection />)

      const loginButton = screen.getByTestId('cta-login')

      // Should render as an anchor element
      expect(loginButton.tagName.toLowerCase()).toBe('a')
      expect(loginButton).toHaveAttribute('href', '/login')

      // Should not be an external link
      expect(loginButton).not.toHaveAttribute('target')
    })

    it('Navbar Login uses React Router Link (renders as anchor)', () => {
      renderWithProviders(<Navbar />)

      const navbarLogin = screen.getByTestId('navbar-login')

      // Should render as an anchor element
      expect(navbarLogin.tagName.toLowerCase()).toBe('a')
      expect(navbarLogin).toHaveAttribute('href', '/login')

      // Should not be an external link
      expect(navbarLogin).not.toHaveAttribute('target')
    })

    it('hero Login navigation is SPA-style (no page reload)', () => {
      renderWithProviders(
        <>
          <HeroSection />
          <LocationDisplay />
        </>
      )

      // Get initial location
      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/')

      // Click the Login button
      const loginButton = screen.getByTestId('cta-login')
      fireEvent.click(loginButton)

      // After click, location should change without page reload
      // (If it was a full reload, the LocationDisplay component would be unmounted/remounted)
      expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
    })

    it('Navbar Login navigation is SPA-style (no page reload)', () => {
      renderWithProviders(
        <>
          <Navbar />
          <LocationDisplay />
        </>
      )

      // Get initial location
      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/')

      // Click the Login link
      const navbarLogin = screen.getByTestId('navbar-login')
      fireEvent.click(navbarLogin)

      // After click, location should change without page reload
      expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
    })
  })

  describe('Full Page Navigation Flow', () => {
    it('both hero and navbar Login links navigate to the same /login route', () => {
      renderWithProviders(
        <>
          <Navbar />
          <HeroSection />
        </>
      )

      const heroLogin = screen.getByTestId('cta-login')
      const navbarLogin = screen.getByTestId('navbar-login')

      // Both should link to /login
      expect(heroLogin).toHaveAttribute('href', '/login')
      expect(navbarLogin).toHaveAttribute('href', '/login')
    })
  })
})
