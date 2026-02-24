/**
 * Navbar Component Tests
 * Owner: Scenario 7 - Navigation Bar
 *
 * Tests for the Navbar component verifying:
 * - Login and Register links displayed when unauthenticated
 * - Dashboard link hidden when unauthenticated
 * - Dashboard link visible when authenticated
 * - Navigation links work correctly
 * - Logo links to homepage
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { Navbar } from '@/components/Navbar'

// Mock the AuthContext
const mockUseAuth = vi.fn()
vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => mockUseAuth(),
}))

// Mock the ThemeContext
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({
    theme: 'dark',
    setTheme: vi.fn(),
    themes: ['light', 'dark', 'cyberpunk', 'synthwave'],
  }),
}))

describe('Navbar', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Unauthenticated user', () => {
    beforeEach(() => {
      mockUseAuth.mockReturnValue({
        user: null,
        isAuthenticated: false,
        isLoading: false,
        login: vi.fn(),
        logout: vi.fn(),
        register: vi.fn(),
      })
    })

    it('displays Login and Register links when not authenticated', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      )

      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /register/i })).toBeInTheDocument()
    })

    it('does NOT display Dashboard link when not authenticated', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      )

      expect(screen.queryByRole('link', { name: /dashboard/i })).not.toBeInTheDocument()
    })

    it('Login link navigates to /login route', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Navbar />
        </MemoryRouter>
      )

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('Register link navigates to /register route', async () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Navbar />
        </MemoryRouter>
      )

      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toHaveAttribute('href', '/register')
    })
  })

  describe('Authenticated user', () => {
    beforeEach(() => {
      mockUseAuth.mockReturnValue({
        user: {
          id: 1,
          username: 'testuser',
          email: 'test@example.com',
          is_admin: false,
        },
        isAuthenticated: true,
        isLoading: false,
        login: vi.fn(),
        logout: vi.fn(),
        register: vi.fn(),
      })
    })

    it('displays Dashboard link when authenticated', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      )

      expect(screen.getByRole('link', { name: /dashboard/i })).toBeInTheDocument()
    })

    it('Dashboard link has correct href', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      )

      const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })
  })

  describe('Logo/Brand link', () => {
    beforeEach(() => {
      mockUseAuth.mockReturnValue({
        user: null,
        isAuthenticated: false,
        isLoading: false,
        login: vi.fn(),
        logout: vi.fn(),
        register: vi.fn(),
      })
    })

    it('Logo links to homepage (/)', () => {
      render(
        <BrowserRouter>
          <Navbar />
        </BrowserRouter>
      )

      const logoLink = screen.getByRole('link', { name: /url shortener|logo|brand/i })
      expect(logoLink).toHaveAttribute('href', '/')
    })
  })
})
