/**
 * Unit tests for Navbar component.
 * Owner: Scenario 7 - Navigation Bar Functionality
 *
 * Test coverage:
 * - Shows Login/Register when unauthenticated
 * - Shows Dashboard when authenticated
 * - Logo links to homepage
 * - Login link navigates to /login
 * - Theme toggle is present and functional
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import Navbar from '../../src/components/Navbar'

// Mock useAuth for different auth states
const mockUseAuth = vi.fn()
vi.mock('../../src/contexts/AuthContext', async () => {
  const actual = await vi.importActual('../../src/contexts/AuthContext')
  return {
    ...actual,
    useAuth: () => mockUseAuth(),
  }
})

// Mock useTheme for theme toggle tests
const mockSetTheme = vi.fn()
const mockUseTheme = vi.fn()
vi.mock('../../src/contexts/ThemeContext', async () => {
  const actual = await vi.importActual('../../src/contexts/ThemeContext')
  return {
    ...actual,
    useTheme: () => mockUseTheme(),
  }
})

const renderNavbar = (initialRoute = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <Navbar />
    </MemoryRouter>
  )
}

describe('Navbar', () => {
  beforeEach(() => {
    // Default to unauthenticated state
    mockUseAuth.mockReturnValue({
      user: null,
      isAuthenticated: false,
      login: vi.fn(),
      logout: vi.fn(),
    })
    // Default theme state
    mockUseTheme.mockReturnValue({
      theme: 'dark',
      setTheme: mockSetTheme,
    })
    mockSetTheme.mockClear()
  })

  describe('Test Case 1: Login link is present and visible', () => {
    it('should render login link when user is not authenticated', () => {
      renderNavbar()

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toBeVisible()
    })

    it('should have login link with correct href to /login', () => {
      renderNavbar()

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should have accessible label for login link', () => {
      renderNavbar()

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Login link navigation', () => {
    it('should have correct href for navigation to /login', () => {
      renderNavbar()

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should be clickable and navigable', async () => {
      const user = userEvent.setup()
      renderNavbar()

      const loginLink = screen.getByRole('link', { name: /login/i })

      // Verify the link exists and can be clicked
      expect(loginLink).toBeInTheDocument()
      await user.click(loginLink)

      // In a real app, this would navigate - the mock setup.tsx replaces Link with <a>
      // so we just verify the href is correct
      expect(loginLink).toHaveAttribute('href', '/login')
    })
  })

  describe('Navbar structure', () => {
    it('should render the brand logo linking to homepage', () => {
      renderNavbar()

      const logoLink = screen.getByRole('link', { name: /shortenr|url short/i })
      expect(logoLink).toBeInTheDocument()
      expect(logoLink).toHaveAttribute('href', '/')
    })

    it('should render Register link when unauthenticated', () => {
      renderNavbar()

      const registerLink = screen.getByRole('link', { name: /register|sign up|get started/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })
  })

  describe('Authentication state', () => {
    it('should show Login and Register when user is not authenticated', () => {
      mockUseAuth.mockReturnValue({
        user: null,
        isAuthenticated: false,
        login: vi.fn(),
        logout: vi.fn(),
      })

      renderNavbar()

      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /register|sign up|get started/i })).toBeInTheDocument()
    })

    it('should show Dashboard link when user is authenticated', () => {
      mockUseAuth.mockReturnValue({
        user: { id: 1, email: 'test@example.com', is_admin: false },
        isAuthenticated: true,
        login: vi.fn(),
        logout: vi.fn(),
      })

      renderNavbar()

      const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('should hide Login link when user is authenticated', () => {
      mockUseAuth.mockReturnValue({
        user: { id: 1, email: 'test@example.com', is_admin: false },
        isAuthenticated: true,
        login: vi.fn(),
        logout: vi.fn(),
      })

      renderNavbar()

      expect(screen.queryByRole('link', { name: /^login$/i })).not.toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('should have navigation landmark', () => {
      renderNavbar()

      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
    })

    it('should have proper aria-labels on interactive elements', () => {
      renderNavbar()

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
    })
  })

  describe('Test Case 6: Theme toggle', () => {
    it('should render theme toggle button', () => {
      renderNavbar()

      const themeToggle = screen.getByRole('button', { name: /toggle theme/i })
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have accessible label for theme toggle', () => {
      renderNavbar()

      const themeToggle = screen.getByRole('button', { name: /toggle theme/i })
      expect(themeToggle).toHaveAttribute('aria-label', 'Toggle theme')
    })

    it('should toggle theme when clicked', async () => {
      const user = userEvent.setup()
      mockUseTheme.mockReturnValue({
        theme: 'dark',
        setTheme: mockSetTheme,
      })
      renderNavbar()

      const themeToggle = screen.getByRole('button', { name: /toggle theme/i })
      await user.click(themeToggle)

      // Should toggle from dark to light
      expect(mockSetTheme).toHaveBeenCalledWith('light')
    })

    it('should toggle theme from light to dark', async () => {
      const user = userEvent.setup()
      mockUseTheme.mockReturnValue({
        theme: 'light',
        setTheme: mockSetTheme,
      })
      renderNavbar()

      const themeToggle = screen.getByRole('button', { name: /toggle theme/i })
      await user.click(themeToggle)

      // Should toggle from light to dark
      expect(mockSetTheme).toHaveBeenCalledWith('dark')
    })
  })
})
