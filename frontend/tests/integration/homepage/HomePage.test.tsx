/**
 * Homepage Integration Tests - Authenticated User Experience
 * Owner: Scenario 6 - Authenticated User Experience
 *
 * Tests for REQ-10 and US-6:
 * - Authenticated users see 'Go to Dashboard' option prominently displayed
 * - 'Get Started' CTA is replaced with dashboard access for logged-in users
 * - Navigation reflects authenticated state
 */

import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { AuthContext } from '@/contexts/AuthContext'
import { ThemeContext } from '@/contexts/ThemeContext'
import { Home } from '@/pages/Home'
import { Dashboard } from '@/pages/Dashboard'
import type { AuthContextType, ThemeContextType, User } from '@/types/custom'

// Helper to render with all required providers
function renderWithProviders(
  initialPath: string = '/',
  authOverrides: Partial<AuthContextType> = {}
) {
  const mockAuthContext: AuthContextType = {
    user: null,
    isAuthenticated: false,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
    ...authOverrides,
  }

  const mockThemeContext: ThemeContextType = {
    theme: 'dark',
    setTheme: vi.fn(),
  }

  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <ThemeContext.Provider value={mockThemeContext}>
        <AuthContext.Provider value={mockAuthContext}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<Dashboard />} />
          </Routes>
        </AuthContext.Provider>
      </ThemeContext.Provider>
    </MemoryRouter>
  )
}

// Create authenticated user fixture
function createAuthenticatedUser(): User {
  return {
    id: 1,
    username: 'testuser',
    email: 'testuser@example.com',
    is_admin: false,
  }
}

describe('Authenticated User Experience - Homepage Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 1: Go to Dashboard button is visible and prominent for authenticated users', () => {
    it("should display 'Go to Dashboard' button when user is authenticated", () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // Find the "Go to Dashboard" button in Hero section
      const dashboardBtn = screen.getByTestId('hero-dashboard-btn')
      expect(dashboardBtn).toBeInTheDocument()
      expect(dashboardBtn).toHaveTextContent('Go to Dashboard')
      expect(dashboardBtn).toBeVisible()
    })

    it("should have 'Go to Dashboard' button prominently styled", () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      const dashboardBtn = screen.getByTestId('hero-dashboard-btn')
      // The button should exist within the hero section (labeled by heading), indicating prominence
      const heroSection = screen.getByRole('region', { name: /shorten urls/i })
      expect(heroSection).toContainElement(dashboardBtn)
    })
  })

  describe("Test Case 2: Log In button is not visible for authenticated users", () => {
    it("should NOT display 'Log In' button in hero section when authenticated", () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // The hero login button should not exist for authenticated users
      const heroLoginBtn = screen.queryByTestId('hero-login-btn')
      expect(heroLoginBtn).not.toBeInTheDocument()
    })

    it("should NOT display 'Get Started' button when authenticated", () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // The hero get started button should not exist for authenticated users
      const getStartedBtn = screen.queryByTestId('hero-get-started-btn')
      expect(getStartedBtn).not.toBeInTheDocument()
    })
  })

  describe('Test Case 3: Click Go to Dashboard button navigates to /dashboard', () => {
    it('should navigate to /dashboard when clicking the dashboard button', () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // Find and click the dashboard button
      const dashboardBtn = screen.getByTestId('hero-dashboard-btn')
      fireEvent.click(dashboardBtn)

      // Verify navigation to Dashboard page
      expect(screen.getByRole('heading', { name: /dashboard/i })).toBeInTheDocument()
      expect(screen.getByText(/welcome to your dashboard/i)).toBeInTheDocument()
    })

    it('should navigate from navbar Dashboard link to /dashboard', () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // Find the Dashboard link in navbar
      const navbarDashboardLink = screen.getByRole('link', { name: /dashboard/i })
      expect(navbarDashboardLink).toBeInTheDocument()

      fireEvent.click(navbarDashboardLink)

      // Verify navigation to Dashboard page
      expect(screen.getByRole('heading', { name: /dashboard/i })).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Page may display personalized or user-specific content', () => {
    it('should render homepage with user context available', () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // The homepage should render properly with user data in context
      // Note: The current implementation shows "Go to Dashboard" for authenticated users
      // which is a form of personalized experience - different from guest view
      const dashboardBtn = screen.getByTestId('hero-dashboard-btn')
      expect(dashboardBtn).toBeInTheDocument()

      // Verify the page content is different from unauthenticated view
      expect(screen.queryByTestId('hero-get-started-btn')).not.toBeInTheDocument()
      expect(screen.queryByTestId('hero-login-btn')).not.toBeInTheDocument()
    })

    it('should maintain consistent page structure for authenticated users', () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // Key sections should still be present
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByText('How It Works')).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Navbar reflects authenticated state', () => {
    it('should show Logout option in navbar for authenticated users', () => {
      const user = createAuthenticatedUser()
      const mockLogout = vi.fn()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
        logout: mockLogout,
      })

      // Find logout button in navbar
      const logoutBtn = screen.getByRole('button', { name: /logout/i })
      expect(logoutBtn).toBeInTheDocument()
    })

    it('should show Dashboard link in navbar for authenticated users', () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // Find Dashboard link in navbar
      const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('should NOT show Log In link in navbar for authenticated users', () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // Login link should not be present
      const loginLink = screen.queryByTestId('navbar-login')
      expect(loginLink).not.toBeInTheDocument()
    })

    it('should NOT show Sign Up link in navbar for authenticated users', () => {
      const user = createAuthenticatedUser()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
      })

      // Sign Up link should not be present
      const registerLink = screen.queryByTestId('navbar-register')
      expect(registerLink).not.toBeInTheDocument()
    })

    it('should call logout when clicking Logout button', () => {
      const user = createAuthenticatedUser()
      const mockLogout = vi.fn()

      renderWithProviders('/', {
        isAuthenticated: true,
        user,
        logout: mockLogout,
      })

      // Find and click logout button
      const logoutBtn = screen.getByRole('button', { name: /logout/i })
      fireEvent.click(logoutBtn)

      // Verify logout was called
      expect(mockLogout).toHaveBeenCalledTimes(1)
    })
  })

  describe('Contrast with unauthenticated state', () => {
    it('should show Get Started and Log In buttons when NOT authenticated', () => {
      renderWithProviders('/', {
        isAuthenticated: false,
        user: null,
      })

      // Guest users see Get Started and Log In
      expect(screen.getByTestId('hero-get-started-btn')).toBeInTheDocument()
      expect(screen.getByTestId('hero-login-btn')).toBeInTheDocument()

      // Dashboard button should not exist
      expect(screen.queryByTestId('hero-dashboard-btn')).not.toBeInTheDocument()
    })

    it('should show Log In and Sign Up links in navbar when NOT authenticated', () => {
      renderWithProviders('/', {
        isAuthenticated: false,
        user: null,
      })

      // Guest navbar shows login and register
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument()
      expect(screen.getByTestId('navbar-register')).toBeInTheDocument()

      // Logout should not be present
      expect(screen.queryByRole('button', { name: /logout/i })).not.toBeInTheDocument()
    })
  })
})
