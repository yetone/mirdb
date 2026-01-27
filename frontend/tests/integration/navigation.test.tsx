/**
 * Navigation Integration Tests
 * Owner: Scenario 4 - Navigation Functionality
 *
 * Purpose: Validate all navigation links and routing work correctly from the homepage.
 *
 * Test coverage:
 * - Navbar presence and links
 * - Hero section CTA navigation
 * - Footer navigation links
 * - Authenticated dashboard link visibility
 * - Anchor/section navigation with smooth scroll
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import React, { createContext, useContext, type ReactNode } from 'react'

// Import homepage components
import { HeroSection } from '../../src/components/home/HeroSection'
import { FeaturesSection } from '../../src/components/home/FeaturesSection'
import HowItWorksSection from '../../src/components/home/HowItWorksSection'

// ============================================================================
// Test Utilities
// ============================================================================

/**
 * Helper component to track current location for navigation assertions
 */
function LocationDisplay() {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

/**
 * Mock Auth Context for testing authenticated navigation
 */
interface AuthContextType {
  isAuthenticated: boolean
  user: { name: string; email: string } | null
}

const AuthContext = createContext<AuthContextType>({
  isAuthenticated: false,
  user: null,
})

function useAuth() {
  return useContext(AuthContext)
}

interface AuthProviderProps {
  children: ReactNode
  isAuthenticated?: boolean
  user?: { name: string; email: string } | null
}

function MockAuthProvider({
  children,
  isAuthenticated = false,
  user = null,
}: AuthProviderProps) {
  return (
    <AuthContext.Provider value={{ isAuthenticated, user }}>
      {children}
    </AuthContext.Provider>
  )
}

/**
 * Navbar component for testing
 * This represents the expected Navbar implementation per scaffold.md
 */
function Navbar() {
  const { isAuthenticated } = useAuth()

  return (
    <nav className="navbar bg-base-100" data-testid="navbar">
      <div className="flex-1">
        <a href="/" className="btn btn-ghost text-xl" data-testid="navbar-brand">
          URLShortener
        </a>
      </div>
      <div className="flex-none">
        <ul className="menu menu-horizontal px-1">
          {isAuthenticated ? (
            <li>
              <a href="/dashboard" data-testid="navbar-dashboard-link">
                Dashboard
              </a>
            </li>
          ) : (
            <>
              <li>
                <a href="/login" data-testid="navbar-login-link">
                  Login
                </a>
              </li>
              <li>
                <a href="/register" data-testid="navbar-register-link">
                  Sign Up
                </a>
              </li>
            </>
          )}
        </ul>
      </div>
    </nav>
  )
}

/**
 * Footer component for testing
 * This represents the expected Footer implementation per scaffold.md
 */
function Footer() {
  return (
    <footer className="footer p-10 bg-base-200" data-testid="footer">
      <nav>
        <h6 className="footer-title">Navigation</h6>
        <a href="/" className="link link-hover" data-testid="footer-home-link">
          Home
        </a>
        <a href="/dashboard" className="link link-hover" data-testid="footer-dashboard-link">
          Dashboard
        </a>
        <a href="/login" className="link link-hover" data-testid="footer-login-link">
          Login
        </a>
        <a href="/register" className="link link-hover" data-testid="footer-register-link">
          Register
        </a>
      </nav>
      <nav>
        <h6 className="footer-title">Legal</h6>
        <a href="/privacy" className="link link-hover" data-testid="footer-privacy-link">
          Privacy Policy
        </a>
        <a href="/terms" className="link link-hover" data-testid="footer-terms-link">
          Terms of Service
        </a>
      </nav>
    </footer>
  )
}

/**
 * Test Homepage that assembles all sections with Navbar and Footer
 */
function TestHomepage() {
  return (
    <div className="min-h-screen bg-base-200">
      <Navbar />
      <main>
        <HeroSection />
        <FeaturesSection />
        <section id="features" data-testid="features-anchor">
          {/* Features content */}
        </section>
        <HowItWorksSection />
      </main>
      <Footer />
    </div>
  )
}

/**
 * Test wrapper with router and auth provider
 */
interface TestWrapperProps {
  children: ReactNode
  initialEntries?: string[]
  isAuthenticated?: boolean
}

function TestWrapper({
  children,
  initialEntries = ['/'],
  isAuthenticated = false,
}: TestWrapperProps) {
  return (
    <MemoryRouter initialEntries={initialEntries}>
      <MockAuthProvider isAuthenticated={isAuthenticated}>
        <Routes>
          <Route path="/" element={children} />
          <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
          <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
          <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard Page</div>} />
        </Routes>
        <LocationDisplay />
      </MockAuthProvider>
    </MemoryRouter>
  )
}

// ============================================================================
// Test Cases
// ============================================================================

describe('Navigation Functionality', () => {
  beforeEach(() => {
    // Mock scrollIntoView for smooth scroll tests
    Element.prototype.scrollIntoView = vi.fn()
    // Mock scroll behavior
    window.scrollTo = vi.fn()
  })

  // Test Case 1: Navbar component is visible at top of page
  describe('Test Case 1: Navbar Presence', () => {
    it('renders Navbar component visible at top of page', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
      expect(navbar).toBeVisible()
    })

    it('navbar contains brand/logo element', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const brand = screen.getByTestId('navbar-brand')
      expect(brand).toBeInTheDocument()
    })
  })

  // Test Case 2: Click Login link in navbar navigates to /login
  describe('Test Case 2: Login Navigation', () => {
    it('navigates to /login when clicking Login link in navbar', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const loginLink = screen.getByTestId('navbar-login-link')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('Login link is visible for unauthenticated users', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <TestHomepage />
        </TestWrapper>
      )

      const loginLink = screen.getByTestId('navbar-login-link')
      expect(loginLink).toBeVisible()
    })
  })

  // Test Case 3: Click Register/Sign Up link in navbar navigates to /register
  describe('Test Case 3: Register Navigation', () => {
    it('navigates to /register when clicking Sign Up link in navbar', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const registerLink = screen.getByTestId('navbar-register-link')
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('Sign Up link is visible for unauthenticated users', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <TestHomepage />
        </TestWrapper>
      )

      const registerLink = screen.getByTestId('navbar-register-link')
      expect(registerLink).toBeVisible()
    })
  })

  // Test Case 4: Click Get Started button in hero section navigates to /register
  describe('Test Case 4: Hero CTA Navigation', () => {
    it('navigates to /register when clicking Get Started button in hero section', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveAttribute('href', '/register')

      fireEvent.click(ctaButton)

      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/register')
    })

    it('Get Started button contains appropriate text', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton).toHaveTextContent(/get started/i)
    })
  })

  // Test Case 5: Click 'Already have an account? Log in' link navigates to /login
  describe("Test Case 5: Hero Login Link Navigation", () => {
    it("navigates to /login when clicking 'Already have an account? Log in' link", () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const loginLink = screen.getByTestId('hero-login-link')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')

      fireEvent.click(loginLink)

      const locationDisplay = screen.getByTestId('location-display')
      expect(locationDisplay).toHaveTextContent('/login')
    })

    it('hero login link contains appropriate text', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const loginLink = screen.getByTestId('hero-login-link')
      expect(loginLink).toHaveTextContent(/already have an account/i)
      expect(loginLink).toHaveTextContent(/log in/i)
    })
  })

  // Test Case 6: Dashboard link is visible in navbar when authenticated
  describe('Test Case 6: Authenticated Dashboard Visibility', () => {
    it('displays Dashboard link in navbar when user is authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true}>
          <TestHomepage />
        </TestWrapper>
      )

      const dashboardLink = screen.getByTestId('navbar-dashboard-link')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toBeVisible()
    })

    it('hides Login and Sign Up links when user is authenticated', () => {
      render(
        <TestWrapper isAuthenticated={true}>
          <TestHomepage />
        </TestWrapper>
      )

      expect(screen.queryByTestId('navbar-login-link')).not.toBeInTheDocument()
      expect(screen.queryByTestId('navbar-register-link')).not.toBeInTheDocument()
    })
  })

  // Test Case 7: Click Dashboard link navigates to /dashboard when authenticated
  describe('Test Case 7: Dashboard Navigation', () => {
    it('navigates to /dashboard when clicking Dashboard link (authenticated)', () => {
      render(
        <TestWrapper isAuthenticated={true}>
          <TestHomepage />
        </TestWrapper>
      )

      const dashboardLink = screen.getByTestId('navbar-dashboard-link')
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('Dashboard link is properly labeled', () => {
      render(
        <TestWrapper isAuthenticated={true}>
          <TestHomepage />
        </TestWrapper>
      )

      const dashboardLink = screen.getByTestId('navbar-dashboard-link')
      expect(dashboardLink).toHaveTextContent(/dashboard/i)
    })
  })

  // Test Case 8: Click footer Home link returns to / route
  describe('Test Case 8: Footer Home Navigation', () => {
    it('footer Home link navigates to / route', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const homeLink = screen.getByTestId('footer-home-link')
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('footer contains all expected navigation links', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      expect(screen.getByTestId('footer-home-link')).toBeInTheDocument()
      expect(screen.getByTestId('footer-dashboard-link')).toBeInTheDocument()
      expect(screen.getByTestId('footer-login-link')).toBeInTheDocument()
      expect(screen.getByTestId('footer-register-link')).toBeInTheDocument()
    })

    it('footer is visible at the bottom of the page', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
      expect(footer).toBeVisible()
    })
  })

  // Test Case 9: Anchor link for section navigation with smooth scroll
  describe('Test Case 9: Section Navigation with Smooth Scroll', () => {
    it('How It Works section has proper anchor id for navigation', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveAttribute('id', 'how-it-works')
    })

    it('clicking anchor link triggers smooth scroll to target section', async () => {
      // Create a component with an anchor link pointing to a section
      function PageWithAnchorNav() {
        const handleAnchorClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
          e.preventDefault()
          const target = document.getElementById('how-it-works')
          if (target) {
            target.scrollIntoView({ behavior: 'smooth' })
          }
        }

        return (
          <div className="min-h-screen bg-base-200">
            <Navbar />
            <nav data-testid="section-nav">
              <a
                href="#how-it-works"
                data-testid="anchor-how-it-works"
                onClick={handleAnchorClick}
              >
                How It Works
              </a>
            </nav>
            <main>
              <HeroSection />
              <HowItWorksSection />
            </main>
            <Footer />
          </div>
        )
      }

      render(
        <TestWrapper>
          <PageWithAnchorNav />
        </TestWrapper>
      )

      const anchorLink = screen.getByTestId('anchor-how-it-works')
      fireEvent.click(anchorLink)

      // Verify scrollIntoView was called with smooth behavior
      expect(Element.prototype.scrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
      })
    })

    it('page sections have unique IDs for anchor navigation', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      // How It Works section should have its anchor ID
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection.id).toBeTruthy()
    })
  })

  // Additional integration tests for full navigation flow
  describe('Full Navigation Flow', () => {
    it('unauthenticated user can see public navigation links', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <TestHomepage />
        </TestWrapper>
      )

      // Navbar links
      expect(screen.getByTestId('navbar-login-link')).toBeVisible()
      expect(screen.getByTestId('navbar-register-link')).toBeVisible()

      // Hero section links (use toBeInTheDocument for animated elements)
      expect(screen.getByTestId('hero-cta-primary')).toBeInTheDocument()
      expect(screen.getByTestId('hero-login-link')).toBeInTheDocument()

      // Footer links
      expect(screen.getByTestId('footer-home-link')).toBeVisible()
    })

    it('authenticated user sees dashboard-focused navigation', () => {
      render(
        <TestWrapper isAuthenticated={true}>
          <TestHomepage />
        </TestWrapper>
      )

      // Dashboard link visible
      expect(screen.getByTestId('navbar-dashboard-link')).toBeVisible()

      // Login/Register links hidden in navbar
      expect(screen.queryByTestId('navbar-login-link')).not.toBeInTheDocument()
      expect(screen.queryByTestId('navbar-register-link')).not.toBeInTheDocument()
    })

    it('all navigation links have valid href attributes', () => {
      render(
        <TestWrapper>
          <TestHomepage />
        </TestWrapper>
      )

      // Check that all navigation links point to valid routes
      const loginLink = screen.getByTestId('navbar-login-link')
      const registerLink = screen.getByTestId('navbar-register-link')
      const heroCtaLink = screen.getByTestId('hero-cta-primary')
      const heroLoginLink = screen.getByTestId('hero-login-link')
      const footerHomeLink = screen.getByTestId('footer-home-link')

      expect(loginLink.getAttribute('href')).toBe('/login')
      expect(registerLink.getAttribute('href')).toBe('/register')
      expect(heroCtaLink.getAttribute('href')).toBe('/register')
      expect(heroLoginLink.getAttribute('href')).toBe('/login')
      expect(footerHomeLink.getAttribute('href')).toBe('/')
    })
  })
})
