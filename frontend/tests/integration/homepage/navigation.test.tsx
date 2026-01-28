/**
 * Navigation and CTAs Integration Tests
 * Owner: Scenario 3 - Navigation and CTAs
 *
 * Tests for REQ-2 and REQ-6:
 * - CTA buttons route to login and registration
 * - Navigation links accessible from Navbar
 */

import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { AuthContext } from '@/contexts/AuthContext'
import { ThemeContext } from '@/contexts/ThemeContext'
import { Home } from '@/pages/Home'
import { Login } from '@/pages/Login'
import { Register } from '@/pages/Register'
import type { AuthContextType, ThemeContextType } from '@/types/custom'

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
    theme: 'light',
    setTheme: vi.fn(),
  }

  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <ThemeContext.Provider value={mockThemeContext}>
        <AuthContext.Provider value={mockAuthContext}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<Login />} />
            <Route path="/register" element={<Register />} />
          </Routes>
        </AuthContext.Provider>
      </ThemeContext.Provider>
    </MemoryRouter>
  )
}

describe('Navigation and CTAs Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 1: Get Started CTA navigates to /register', () => {
    it("should navigate to /register when clicking 'Get Started' button", () => {
      renderWithProviders('/')

      // Find and click the Get Started button in Hero section
      const getStartedBtn = screen.getByTestId('hero-get-started-btn')
      expect(getStartedBtn).toBeInTheDocument()
      expect(getStartedBtn).toHaveTextContent('Get Started')

      fireEvent.click(getStartedBtn)

      // Verify we navigated to the Register page
      expect(screen.getByText('Sign Up')).toBeInTheDocument()
      expect(screen.getByText('Create your account')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Log In CTA navigates to /login', () => {
    it("should navigate to /login when clicking 'Log In' button in Hero", () => {
      renderWithProviders('/')

      // Find and click the Log In button in Hero section
      const loginBtn = screen.getByTestId('hero-login-btn')
      expect(loginBtn).toBeInTheDocument()
      expect(loginBtn).toHaveTextContent('Log In')

      fireEvent.click(loginBtn)

      // Verify we navigated to the Login page
      expect(screen.getByRole('heading', { name: /log in/i })).toBeInTheDocument()
      expect(screen.getByText('Welcome back!')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Navbar contains login link', () => {
    it('should have a login link in Navbar that navigates to /login', () => {
      renderWithProviders('/')

      // Find the login link in Navbar
      const navbarLoginLink = screen.getByTestId('navbar-login')
      expect(navbarLoginLink).toBeInTheDocument()
      expect(navbarLoginLink).toHaveTextContent('Log In')
      expect(navbarLoginLink).toHaveAttribute('href', '/login')

      fireEvent.click(navbarLoginLink)

      // Verify we navigated to the Login page
      expect(screen.getByRole('heading', { name: /log in/i })).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Navbar contains registration link', () => {
    it('should have a registration link in Navbar that navigates to /register', () => {
      renderWithProviders('/')

      // Find the Sign Up link in Navbar
      const navbarRegisterLink = screen.getByTestId('navbar-register')
      expect(navbarRegisterLink).toBeInTheDocument()
      expect(navbarRegisterLink).toHaveTextContent('Sign Up')
      expect(navbarRegisterLink).toHaveAttribute('href', '/register')

      fireEvent.click(navbarRegisterLink)

      // Verify we navigated to the Register page
      expect(screen.getByText('Sign Up')).toBeInTheDocument()
      expect(screen.getByText('Create your account')).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Home component renders at / route', () => {
    it('should render Home component when navigating to / route', () => {
      renderWithProviders('/')

      // Verify the Home page content is rendered
      // Use specific name to avoid multiple navigation elements (Navbar and Footer nav)
      expect(screen.getByRole('navigation', { name: /main navigation/i })).toBeInTheDocument()
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
      expect(screen.getByTestId('hero-get-started-btn')).toBeInTheDocument()
      expect(screen.getByTestId('hero-login-btn')).toBeInTheDocument()

      // Verify other sections are present
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByText('How It Works')).toBeInTheDocument()
    })
  })

  describe('Additional navigation edge cases', () => {
    it('should show different CTAs when authenticated', () => {
      renderWithProviders('/', {
        isAuthenticated: true,
        user: {
          id: 1,
          username: 'testuser',
          email: 'test@example.com',
          is_admin: false,
        },
      })

      // Authenticated users see "Go to Dashboard" instead of "Get Started"
      expect(screen.getByTestId('hero-dashboard-btn')).toBeInTheDocument()
      expect(screen.getByTestId('hero-dashboard-btn')).toHaveTextContent('Go to Dashboard')

      // Login button should not be present in hero for authenticated users
      expect(screen.queryByTestId('hero-login-btn')).not.toBeInTheDocument()
      expect(screen.queryByTestId('hero-get-started-btn')).not.toBeInTheDocument()
    })

    it('should have proper accessibility attributes on navigation', () => {
      renderWithProviders('/')

      // Check for navigation landmark
      const nav = screen.getByRole('navigation', { name: /main navigation/i })
      expect(nav).toBeInTheDocument()

      // Check for main content area
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })
  })
})
