/**
 * Authenticated User Experience - Scenario Tests
 *
 * This test file covers the scenario: "Verify homepage displays appropriate content
 * for authenticated users based on AuthContext integration"
 *
 * Test Cases:
 * 1. Integration: Load homepage as authenticated user → Hero section shows 'Go to Dashboard' CTA instead of 'Get Started'
 * 2. Unit: Check AuthContext integration → Homepage conditionally renders based on authentication state
 * 3. Integration: Verify Navbar for authenticated user → Navbar displays user menu with Dashboard link and logout option
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import HeroSection from './HeroSection'
import Navbar from './Navbar'
import { ThemeProvider } from '../contexts/ThemeContext'
import { AuthProvider, User } from '../contexts/AuthContext'
import App from '../App'

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
    section: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <section {...props}>{children}</section>
    ),
    svg: ({ children, ...props }: React.SVGProps<SVGSVGElement>) => (
      <svg {...props}>{children}</svg>
    ),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// Helper to render with all required providers
const renderWithProviders = (
  component: React.ReactNode,
  options?: {
    initialRoute?: string
    initialUser?: User | null
  }
) => {
  const { initialRoute = '/', initialUser = null } = options || {}

  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <ThemeProvider>
        <AuthProvider initialUser={initialUser}>
          {component}
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

// Mock authenticated user
const mockAuthenticatedUser: User = {
  id: '1',
  username: 'testuser',
  email: 'testuser@example.com',
  is_admin: false,
}

describe('Authenticated User Experience Scenario', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  /**
   * Test Case 1: Integration Test
   * Input: Load homepage as authenticated user
   * Expected: Hero section shows 'Go to Dashboard' CTA instead of 'Get Started'
   */
  describe('Test Case 1: Hero Section CTA for Authenticated User', () => {
    it('displays "Go to Dashboard" CTA for authenticated user', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      const dashboardCta = screen.getByTestId('cta-dashboard')
      expect(dashboardCta).toBeInTheDocument()
      expect(dashboardCta).toHaveTextContent('Go to Dashboard')
    })

    it('Dashboard CTA links to /dashboard', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      const dashboardCta = screen.getByTestId('cta-dashboard')
      expect(dashboardCta).toHaveAttribute('href', '/dashboard')
    })

    it('does not display "Get Started Free" CTA for authenticated user', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      const getStartedCta = screen.queryByTestId('cta-get-started')
      expect(getStartedCta).not.toBeInTheDocument()
    })

    it('does not display "Login" CTA for authenticated user', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      const loginCta = screen.queryByTestId('cta-login')
      expect(loginCta).not.toBeInTheDocument()
    })

    it('displays "Get Started Free" CTA for unauthenticated user', () => {
      renderWithProviders(<HeroSection />)

      const getStartedCta = screen.getByTestId('cta-get-started')
      expect(getStartedCta).toBeInTheDocument()
      expect(getStartedCta).toHaveTextContent('Get Started Free')
    })

    it('displays "Login" CTA for unauthenticated user', () => {
      renderWithProviders(<HeroSection />)

      const loginCta = screen.getByTestId('cta-login')
      expect(loginCta).toBeInTheDocument()
      expect(loginCta).toHaveTextContent('Login')
    })

    it('does not display "Go to Dashboard" CTA for unauthenticated user', () => {
      renderWithProviders(<HeroSection />)

      const dashboardCta = screen.queryByTestId('cta-dashboard')
      expect(dashboardCta).not.toBeInTheDocument()
    })

    it('full homepage shows correct CTAs for authenticated user', () => {
      renderWithProviders(<App />, { initialUser: mockAuthenticatedUser })

      // Check hero section dashboard CTA
      const dashboardCta = screen.getByTestId('cta-dashboard')
      expect(dashboardCta).toBeInTheDocument()
      expect(dashboardCta).toHaveTextContent('Go to Dashboard')

      // Verify Get Started and Login are not present
      const getStartedCta = screen.queryByTestId('cta-get-started')
      const loginCta = screen.queryByTestId('cta-login')
      expect(getStartedCta).not.toBeInTheDocument()
      expect(loginCta).not.toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Unit Test
   * Input: Check AuthContext integration
   * Expected: Homepage conditionally renders based on authentication state
   */
  describe('Test Case 2: AuthContext Integration', () => {
    it('HeroSection uses AuthContext to determine authentication state', () => {
      // Authenticated user
      const { unmount } = renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })
      expect(screen.getByTestId('cta-dashboard')).toBeInTheDocument()
      unmount()

      // Render as unauthenticated
      renderWithProviders(<HeroSection />, { initialUser: null })
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
    })

    it('renders different content based on isAuthenticated flag', () => {
      // With authenticated user
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })
      expect(screen.getByTestId('cta-dashboard')).toBeInTheDocument()
      expect(screen.queryByTestId('cta-get-started')).not.toBeInTheDocument()
      expect(screen.queryByTestId('cta-login')).not.toBeInTheDocument()
    })

    it('renders unauthenticated content when no user is logged in', () => {
      renderWithProviders(<HeroSection />, { initialUser: null })
      expect(screen.queryByTestId('cta-dashboard')).not.toBeInTheDocument()
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()
    })

    it('AuthContext provides correct isAuthenticated value', () => {
      // Test with authenticated user - Dashboard CTA should render
      const { unmount } = renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })
      expect(screen.getByTestId('cta-dashboard')).toBeInTheDocument()
      unmount()

      // Test without user - Get Started CTA should render
      renderWithProviders(<HeroSection />, { initialUser: null })
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
    })

    it('homepage correctly integrates with AuthProvider', () => {
      renderWithProviders(<App />, { initialUser: mockAuthenticatedUser })

      // Navbar should show authenticated state
      const navbarDashboard = screen.getByTestId('navbar-dashboard')
      expect(navbarDashboard).toBeInTheDocument()

      // Hero should show authenticated state
      const heroDashboardCta = screen.getByTestId('cta-dashboard')
      expect(heroDashboardCta).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Integration Test
   * Input: Verify Navbar for authenticated user
   * Expected: Navbar displays user menu with Dashboard link and logout option
   */
  describe('Test Case 3: Navbar for Authenticated User', () => {
    it('displays Dashboard link in Navbar for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const dashboardLink = screen.getByTestId('navbar-dashboard')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveTextContent('Dashboard')
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('displays user menu button for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      expect(userMenuButton).toBeInTheDocument()
    })

    it('user menu contains logout option when opened', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open user menu
      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      fireEvent.click(userMenuButton)

      // Check for logout button
      const logoutButton = screen.getByTestId('navbar-logout-button')
      expect(logoutButton).toBeInTheDocument()
      expect(logoutButton).toHaveTextContent('Logout')
    })

    it('user menu contains Dashboard-related links', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open user menu
      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      fireEvent.click(userMenuButton)

      // Check for profile and settings links
      const profileLink = screen.getByTestId('navbar-profile-link')
      const settingsLink = screen.getByTestId('navbar-settings-link')

      expect(profileLink).toBeInTheDocument()
      expect(profileLink).toHaveAttribute('href', '/profile')

      expect(settingsLink).toBeInTheDocument()
      expect(settingsLink).toHaveAttribute('href', '/settings')
    })

    it('Navbar does not show Login/Register for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const loginLink = screen.queryByTestId('navbar-login')
      const getStartedLink = screen.queryByTestId('navbar-get-started')

      expect(loginLink).not.toBeInTheDocument()
      expect(getStartedLink).not.toBeInTheDocument()
    })

    it('displays username in Navbar for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const username = screen.getByTestId('navbar-username')
      expect(username).toBeInTheDocument()
      expect(username).toHaveTextContent('testuser')
    })

    it('displays user avatar with first letter of username', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const avatar = screen.getByTestId('navbar-user-avatar')
      expect(avatar).toBeInTheDocument()
      expect(avatar).toHaveTextContent('T') // First letter of 'testuser'
    })

    it('mobile menu shows authenticated user options', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-button')
      fireEvent.click(hamburgerButton)

      // Check for Dashboard link
      const mobileDashboardLink = screen.getByTestId('nav-dashboard-mobile')
      expect(mobileDashboardLink).toBeInTheDocument()

      // Check for logout option
      const mobileLogoutButton = screen.getByTestId('nav-logout-mobile')
      expect(mobileLogoutButton).toBeInTheDocument()
    })

    it('mobile menu does not show Login/Register for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-button')
      fireEvent.click(hamburgerButton)

      // Verify Login and Register are not present
      const mobileLoginLink = screen.queryByTestId('nav-login-mobile')
      const mobileRegisterLink = screen.queryByTestId('nav-register-mobile')

      expect(mobileLoginLink).not.toBeInTheDocument()
      expect(mobileRegisterLink).not.toBeInTheDocument()
    })
  })

  /**
   * Additional Tests: Full User Journey
   */
  describe('Full Authenticated User Journey', () => {
    it('complete homepage renders correctly for authenticated user', () => {
      renderWithProviders(<App />, { initialUser: mockAuthenticatedUser })

      // Navbar elements
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('navbar-dashboard')).toBeInTheDocument()
      expect(screen.getByTestId('navbar-user-menu-button')).toBeInTheDocument()

      // Hero section
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-dashboard')).toBeInTheDocument()

      // Unauthenticated CTAs should not be present
      expect(screen.queryByTestId('cta-get-started')).not.toBeInTheDocument()
      expect(screen.queryByTestId('cta-login')).not.toBeInTheDocument()
      expect(screen.queryByTestId('navbar-login')).not.toBeInTheDocument()
      expect(screen.queryByTestId('navbar-get-started')).not.toBeInTheDocument()
    })

    it('complete homepage renders correctly for unauthenticated user', () => {
      renderWithProviders(<App />, { initialUser: null })

      // Navbar elements for unauthenticated
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument()
      expect(screen.getByTestId('navbar-get-started')).toBeInTheDocument()

      // Hero section for unauthenticated
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()

      // Authenticated elements should not be present
      expect(screen.queryByTestId('cta-dashboard')).not.toBeInTheDocument()
      expect(screen.queryByTestId('navbar-dashboard')).not.toBeInTheDocument()
      expect(screen.queryByTestId('navbar-user-menu-button')).not.toBeInTheDocument()
    })

    it('authentication state is consistent across Navbar and HeroSection', () => {
      renderWithProviders(<App />, { initialUser: mockAuthenticatedUser })

      // Both Navbar and HeroSection should show authenticated state
      const navbarDashboard = screen.getByTestId('navbar-dashboard')
      const heroDashboard = screen.getByTestId('cta-dashboard')

      expect(navbarDashboard).toBeInTheDocument()
      expect(heroDashboard).toBeInTheDocument()
    })
  })
})
