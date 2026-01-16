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
    it('displays "Go to Dashboard" button for authenticated user instead of "Get Started Free"', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      // Should show "Go to Dashboard" button
      const dashboardCta = screen.getByTestId('cta-dashboard')
      expect(dashboardCta).toBeInTheDocument()
      expect(dashboardCta).toHaveTextContent('Go to Dashboard')
      expect(dashboardCta).toHaveAttribute('href', '/dashboard')
    })

    it('does not display "Get Started Free" button for authenticated user', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      // Should NOT show "Get Started Free" button
      const getStartedCta = screen.queryByTestId('cta-get-started')
      expect(getStartedCta).not.toBeInTheDocument()
    })

    it('does not display "Login" button for authenticated user', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      // Should NOT show "Login" button in hero section
      const loginCta = screen.queryByTestId('cta-login')
      expect(loginCta).not.toBeInTheDocument()
    })

    it('displays "Get Started Free" and "Login" buttons for unauthenticated user', () => {
      renderWithProviders(<HeroSection />, { initialUser: null })

      // Should show "Get Started Free" button
      const getStartedCta = screen.getByTestId('cta-get-started')
      expect(getStartedCta).toBeInTheDocument()
      expect(getStartedCta).toHaveTextContent('Get Started Free')

      // Should show "Login" button
      const loginCta = screen.getByTestId('cta-login')
      expect(loginCta).toBeInTheDocument()
      expect(loginCta).toHaveTextContent('Login')
    })

    it('"Go to Dashboard" button navigates to /dashboard', () => {
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      const dashboardCta = screen.getByTestId('cta-dashboard')
      expect(dashboardCta).toHaveAttribute('href', '/dashboard')
    })

    it('hero section renders correctly with authenticated user on full App', () => {
      renderWithProviders(<App />, { initialUser: mockAuthenticatedUser })

      // Hero section should be present
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Should show dashboard CTA
      const dashboardCta = screen.getByTestId('cta-dashboard')
      expect(dashboardCta).toBeInTheDocument()
      expect(dashboardCta).toHaveTextContent('Go to Dashboard')
    })
  })

  /**
   * Test Case 2: Unit Test
   * Input: Check AuthContext integration
   * Expected: Homepage conditionally renders based on authentication state
   */
  describe('Test Case 2: AuthContext Integration', () => {
    it('HeroSection accesses AuthContext to determine authentication state', () => {
      // When unauthenticated - shows Get Started
      const { unmount } = renderWithProviders(<HeroSection />, { initialUser: null })
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      unmount()

      // When authenticated - shows Dashboard
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })
      expect(screen.getByTestId('cta-dashboard')).toBeInTheDocument()
    })

    it('conditional rendering differs based on authentication state', () => {
      // Test unauthenticated state
      const { unmount: unmountUnauth } = renderWithProviders(<HeroSection />, { initialUser: null })

      // Unauthenticated shows Get Started and Login
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()
      expect(screen.queryByTestId('cta-dashboard')).not.toBeInTheDocument()

      unmountUnauth()

      // Test authenticated state - should show different content
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })

      // Authenticated shows Dashboard button only
      expect(screen.queryByTestId('cta-get-started')).not.toBeInTheDocument()
      expect(screen.queryByTestId('cta-login')).not.toBeInTheDocument()
      expect(screen.getByTestId('cta-dashboard')).toBeInTheDocument()
    })

    it('HeroSection renders correctly with different user states', () => {
      // Test with admin user
      const adminUser: User = {
        ...mockAuthenticatedUser,
        is_admin: true,
      }

      renderWithProviders(<HeroSection />, { initialUser: adminUser })
      expect(screen.getByTestId('cta-dashboard')).toBeInTheDocument()
    })

    it('HeroSection shows correct headline regardless of auth state', () => {
      // Unauthenticated
      const { unmount } = renderWithProviders(<HeroSection />, { initialUser: null })
      expect(screen.getByTestId('hero-headline')).toHaveTextContent('Shorten. Share. Analyze.')
      unmount()

      // Authenticated
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })
      expect(screen.getByTestId('hero-headline')).toHaveTextContent('Shorten. Share. Analyze.')
    })

    it('HeroSection shows correct subheadline regardless of auth state', () => {
      // Unauthenticated
      const { unmount } = renderWithProviders(<HeroSection />, { initialUser: null })
      expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()
      unmount()

      // Authenticated
      renderWithProviders(<HeroSection />, { initialUser: mockAuthenticatedUser })
      expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Integration Test
   * Input: Verify Navbar for authenticated user
   * Expected: Navbar displays user menu with Dashboard link and logout option
   */
  describe('Test Case 3: Navbar for Authenticated User', () => {
    it('Navbar displays Dashboard link for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const dashboardLink = screen.getByTestId('navbar-dashboard')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveTextContent('Dashboard')
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('Navbar displays user menu button for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      expect(userMenuButton).toBeInTheDocument()
    })

    it('Navbar user menu contains logout option', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open user menu
      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      fireEvent.click(userMenuButton)

      const logoutButton = screen.getByTestId('navbar-logout-button')
      expect(logoutButton).toBeInTheDocument()
      expect(logoutButton).toHaveTextContent('Logout')
    })

    it('Navbar user menu shows Dashboard in dropdown', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Dashboard link should be visible in navbar (not dropdown)
      const dashboardLink = screen.getByTestId('navbar-dashboard')
      expect(dashboardLink).toBeInTheDocument()
    })

    it('Navbar does not show Login/Register for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const loginLink = screen.queryByTestId('navbar-login')
      const getStartedLink = screen.queryByTestId('navbar-get-started')

      expect(loginLink).not.toBeInTheDocument()
      expect(getStartedLink).not.toBeInTheDocument()
    })

    it('Navbar displays username in user menu', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const username = screen.getByTestId('navbar-username')
      expect(username).toBeInTheDocument()
      expect(username).toHaveTextContent('testuser')
    })

    it('Navbar shows Login/Register for unauthenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: null })

      const loginLink = screen.getByTestId('navbar-login')
      const getStartedLink = screen.getByTestId('navbar-get-started')

      expect(loginLink).toBeInTheDocument()
      expect(getStartedLink).toBeInTheDocument()
    })

    it('Navbar logout button clears authentication', () => {
      const { rerender } = render(
        <MemoryRouter>
          <ThemeProvider>
            <AuthProvider initialUser={mockAuthenticatedUser}>
              <Navbar />
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      )

      // Open user menu and click logout
      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      fireEvent.click(userMenuButton)

      const logoutButton = screen.getByTestId('navbar-logout-button')
      fireEvent.click(logoutButton)

      // After logout, should show login/register links
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument()
      expect(screen.getByTestId('navbar-get-started')).toBeInTheDocument()
    })
  })

  /**
   * Full Homepage Integration Test
   */
  describe('Full Homepage Authenticated Experience', () => {
    it('homepage shows authenticated experience when user is logged in', () => {
      renderWithProviders(<App />, { initialUser: mockAuthenticatedUser })

      // Navbar should show authenticated state
      const navbarDashboard = screen.getByTestId('navbar-dashboard')
      expect(navbarDashboard).toBeInTheDocument()

      // Hero should show dashboard CTA
      const heroDashboardCta = screen.getByTestId('cta-dashboard')
      expect(heroDashboardCta).toBeInTheDocument()
      expect(heroDashboardCta).toHaveTextContent('Go to Dashboard')

      // Should not show unauthenticated CTAs
      expect(screen.queryByTestId('cta-get-started')).not.toBeInTheDocument()
      expect(screen.queryByTestId('cta-login')).not.toBeInTheDocument()
    })

    it('homepage shows unauthenticated experience when user is not logged in', () => {
      renderWithProviders(<App />, { initialUser: null })

      // Navbar should show unauthenticated state
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument()
      expect(screen.getByTestId('navbar-get-started')).toBeInTheDocument()

      // Hero should show get started and login CTAs
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()

      // Should not show authenticated CTA
      expect(screen.queryByTestId('cta-dashboard')).not.toBeInTheDocument()
    })
  })
})
