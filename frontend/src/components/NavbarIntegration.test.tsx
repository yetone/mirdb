/**
 * Navbar Integration - Scenario Tests
 *
 * This test file covers the scenario: "Verify homepage includes navigation header
 * consistent with authenticated pages as specified in REQ-9"
 *
 * Test Cases:
 * 1. Integration: Render homepage with Navbar → Navbar component is rendered at top of page
 * 2. Integration: Check Navbar for unauthenticated user → Navbar displays Login and Register links
 * 3. Integration: Check Navbar for authenticated user → Navbar displays Dashboard link and user menu
 * 4. Unit: Verify ThemeToggle in Navbar → Theme toggle button is accessible in Navbar
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
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

describe('Navbar Integration Scenario', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  /**
   * Test Case 1: Integration Test
   * Input: Render homepage with Navbar
   * Expected: Navbar component is rendered at top of page
   */
  describe('Test Case 1: Navbar Presence on Homepage', () => {
    it('Navbar component is rendered on homepage', () => {
      renderWithProviders(<App />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
    })

    it('Navbar is positioned at the top of the page (fixed positioning)', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveClass('fixed', 'top-0')
    })

    it('Navbar contains logo linking to homepage', () => {
      renderWithProviders(<Navbar />)

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveTextContent('URL Shortener')
      expect(logo).toHaveAttribute('href', '/')
    })

    it('Navbar contains navigation links to Features and How It Works', () => {
      renderWithProviders(<Navbar />)

      const featuresLink = screen.getByTestId('nav-features-desktop')
      const howItWorksLink = screen.getByTestId('nav-how-it-works-desktop')

      expect(featuresLink).toBeInTheDocument()
      expect(featuresLink).toHaveAttribute('href', '#features')

      expect(howItWorksLink).toBeInTheDocument()
      expect(howItWorksLink).toHaveAttribute('href', '#how-it-works')
    })

    it('Navbar is rendered at the top of App component with HeroSection below', () => {
      renderWithProviders(<App />)

      const navbar = screen.getByTestId('navbar')
      const heroSection = screen.getByTestId('hero-section')

      expect(navbar).toBeInTheDocument()
      expect(heroSection).toBeInTheDocument()

      // Navbar should come before HeroSection in DOM order
      expect(navbar.compareDocumentPosition(heroSection) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })
  })

  /**
   * Test Case 2: Integration Test
   * Input: Check Navbar for unauthenticated user
   * Expected: Navbar displays Login and Register links
   */
  describe('Test Case 2: Unauthenticated User Navbar', () => {
    it('displays Login link for unauthenticated user', () => {
      renderWithProviders(<Navbar />)

      const loginLink = screen.getByTestId('navbar-login')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveTextContent('Login')
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('displays Get Started (Register) link for unauthenticated user', () => {
      renderWithProviders(<Navbar />)

      const getStartedLink = screen.getByTestId('navbar-get-started')
      expect(getStartedLink).toBeInTheDocument()
      expect(getStartedLink).toHaveTextContent('Get Started')
      expect(getStartedLink).toHaveAttribute('href', '/register')
    })

    it('does not display Dashboard link for unauthenticated user', () => {
      renderWithProviders(<Navbar />)

      const dashboardLink = screen.queryByTestId('navbar-dashboard')
      expect(dashboardLink).not.toBeInTheDocument()
    })

    it('does not display user menu for unauthenticated user', () => {
      renderWithProviders(<Navbar />)

      const userMenuButton = screen.queryByTestId('navbar-user-menu-button')
      expect(userMenuButton).not.toBeInTheDocument()
    })

    it('Login and Register links are accessible on mobile menu', () => {
      renderWithProviders(<Navbar />)

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-button')
      fireEvent.click(hamburgerButton)

      // Check mobile login and register links
      const mobileLoginLink = screen.getByTestId('nav-login-mobile')
      const mobileRegisterLink = screen.getByTestId('nav-register-mobile')

      expect(mobileLoginLink).toBeInTheDocument()
      expect(mobileRegisterLink).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Integration Test
   * Input: Check Navbar for authenticated user
   * Expected: Navbar displays Dashboard link and user menu
   */
  describe('Test Case 3: Authenticated User Navbar', () => {
    it('displays Dashboard link for authenticated user', () => {
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

    it('displays username in user menu button', () => {
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

    it('does not display Login link for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const loginLink = screen.queryByTestId('navbar-login')
      expect(loginLink).not.toBeInTheDocument()
    })

    it('does not display Get Started link for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const getStartedLink = screen.queryByTestId('navbar-get-started')
      expect(getStartedLink).not.toBeInTheDocument()
    })

    it('user menu opens when clicking user menu button', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      fireEvent.click(userMenuButton)

      const userMenu = screen.getByTestId('navbar-user-menu')
      expect(userMenu).toBeInTheDocument()
    })

    it('user menu contains Profile, Settings, and Logout options', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open user menu
      const userMenuButton = screen.getByTestId('navbar-user-menu-button')
      fireEvent.click(userMenuButton)

      const profileLink = screen.getByTestId('navbar-profile-link')
      const settingsLink = screen.getByTestId('navbar-settings-link')
      const logoutButton = screen.getByTestId('navbar-logout-button')

      expect(profileLink).toBeInTheDocument()
      expect(profileLink).toHaveAttribute('href', '/profile')

      expect(settingsLink).toBeInTheDocument()
      expect(settingsLink).toHaveAttribute('href', '/settings')

      expect(logoutButton).toBeInTheDocument()
      expect(logoutButton).toHaveTextContent('Logout')
    })

    it('Dashboard link is accessible on mobile menu for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-button')
      fireEvent.click(hamburgerButton)

      const mobileDashboardLink = screen.getByTestId('nav-dashboard-mobile')
      expect(mobileDashboardLink).toBeInTheDocument()
    })

    it('Logout option is accessible on mobile menu for authenticated user', () => {
      renderWithProviders(<Navbar />, { initialUser: mockAuthenticatedUser })

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-button')
      fireEvent.click(hamburgerButton)

      const mobileLogoutButton = screen.getByTestId('nav-logout-mobile')
      expect(mobileLogoutButton).toBeInTheDocument()
    })
  })

  /**
   * Test Case 4: Unit Test
   * Input: Verify ThemeToggle in Navbar
   * Expected: Theme toggle button is accessible in Navbar
   */
  describe('Test Case 4: ThemeToggle in Navbar', () => {
    it('ThemeToggle component is rendered in Navbar', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = screen.getByTestId('navbar-theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })

    it('ThemeToggle button is accessible and clickable', () => {
      renderWithProviders(<Navbar />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')
      expect(themeToggleButton).toBeInTheDocument()
      expect(themeToggleButton).toHaveAttribute('aria-label', 'Toggle theme')
    })

    it('ThemeToggle is present in desktop navigation', () => {
      renderWithProviders(<Navbar />)

      const desktopNav = screen.getByTestId('desktop-nav')
      const themeToggle = screen.getByTestId('navbar-theme-toggle')

      expect(desktopNav).toContainElement(themeToggle)
    })

    it('ThemeToggle is present in mobile menu when opened', () => {
      renderWithProviders(<Navbar />)

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-button')
      fireEvent.click(hamburgerButton)

      const mobileThemeToggle = screen.getByTestId('navbar-theme-toggle-mobile')
      expect(mobileThemeToggle).toBeInTheDocument()
    })

    it('ThemeToggle dropdown opens when clicking the button', () => {
      renderWithProviders(<Navbar />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(themeToggleButton)

      const themeDropdown = screen.getByTestId('theme-dropdown')
      expect(themeDropdown).toBeInTheDocument()
    })

    it('ThemeToggle has proper ARIA attributes for accessibility', () => {
      renderWithProviders(<Navbar />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')

      expect(themeToggleButton).toHaveAttribute('aria-label', 'Toggle theme')
      expect(themeToggleButton).toHaveAttribute('aria-haspopup', 'listbox')
      expect(themeToggleButton).toHaveAttribute('aria-expanded')
    })
  })

  /**
   * Additional Integration Tests for Homepage Navbar
   */
  describe('Homepage Navbar Integration', () => {
    it('Navbar is consistent across the homepage', () => {
      renderWithProviders(<App />)

      // Navbar should be present
      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      // Should have correct brand name
      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toHaveTextContent('URL Shortener')
    })

    it('Navbar styling is consistent with glass morphism design', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByTestId('navbar')

      // Should have backdrop blur and semi-transparent background
      expect(navbar).toHaveClass('backdrop-blur-md')
      expect(navbar).toHaveClass('bg-base-100/80')
      expect(navbar).toHaveClass('border-b')
    })

    it('Navbar has proper z-index for layering', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveClass('z-50')
    })

    it('Mobile menu toggle works correctly', () => {
      renderWithProviders(<Navbar />)

      // Menu should be closed initially
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument()

      // Open menu
      const hamburgerButton = screen.getByTestId('hamburger-button')
      fireEvent.click(hamburgerButton)

      // Menu should be open
      expect(screen.getByTestId('mobile-menu')).toBeInTheDocument()

      // Close menu
      fireEvent.click(hamburgerButton)

      // Menu should be closed
      expect(screen.queryByTestId('mobile-menu')).not.toBeInTheDocument()
    })

    it('Hamburger button has proper accessibility attributes', () => {
      renderWithProviders(<Navbar />)

      const hamburgerButton = screen.getByTestId('hamburger-button')

      expect(hamburgerButton).toHaveAttribute('aria-controls', 'mobile-menu')
      expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false')
      expect(hamburgerButton).toHaveAttribute('aria-label', 'Open menu')
    })
  })
})
