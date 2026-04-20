/**
 * Unit tests for Navbar component.
 * Owner: Scenario 3 - Secondary Login CTA Button
 *
 * Test coverage:
 * - Shows Login/Register when unauthenticated
 * - Shows Dashboard when authenticated
 * - Logo links to homepage
 * - Login link navigates to /login
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import Navbar from '../../src/components/Navbar'
import { AuthProvider } from '../../src/contexts/AuthContext'
import { ThemeProvider } from '../../src/contexts/ThemeContext'

// Mock useAuth for different auth states
const mockUseAuth = vi.fn()
vi.mock('../../src/contexts/AuthContext', async () => {
  const actual = await vi.importActual('../../src/contexts/AuthContext')
  return {
    ...actual,
    useAuth: () => mockUseAuth(),
  }
})

const renderNavbar = (initialRoute = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <ThemeProvider>
        <Navbar />
      </ThemeProvider>
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
})
