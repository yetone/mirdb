/**
 * Integration Tests
 * Owners:
 * - Scenario 14: AuthContext Integration
 * - Scenario 17: Page Load Performance
 * - Scenario 18: Smooth Scroll Behavior
 * - Scenario 19: Hover Effects
 *
 * Test coverage:
 * - AuthContext consumption and conditional rendering
 * - Render performance
 * - Scroll behavior
 * - Hover state interactions
 */
import { describe, it, expect, vi } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { ReactNode, useEffect, useState } from 'react'
import Home from '../../pages/Home'
import { AuthProvider, useAuth } from '../../contexts/AuthContext'
import { ThemeProvider } from '../../contexts/ThemeContext'

// Mock child components that might not exist yet
vi.mock('../../components/BackgroundEffect', () => ({
  default: () => <div data-testid="background-effect" />
}))

// Test wrapper with all providers
function TestWrapper({ children }: { children: ReactNode }) {
  return (
    <MemoryRouter>
      <ThemeProvider>
        <AuthProvider>{children}</AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('Scenario 14: AuthContext Integration', () => {
  describe('Test Case 1: Unauthenticated user view', () => {
    it('displays Login and Register buttons when user is not logged in', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Check for Login button in navbar (main nav without aria-label)
      const navbar = screen.getByRole('navigation', { name: '' })
      const loginButton = within(navbar).getByRole('link', { name: /login/i })
      expect(loginButton).toBeInTheDocument()

      // Check for Register button in navbar
      const registerButton = within(navbar).getByRole('link', { name: /register/i })
      expect(registerButton).toBeInTheDocument()
    })

    it('Login button in navbar navigates to /login', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const navbar = screen.getByRole('navigation', { name: '' })
      const loginLink = within(navbar).getByRole('link', { name: /login/i })
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('Register button in navbar navigates to /register', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const navbar = screen.getByRole('navigation', { name: '' })
      const registerLink = within(navbar).getByRole('link', { name: /register/i })
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('does not show Dashboard link when not authenticated', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const dashboardLink = screen.queryByRole('link', { name: /dashboard/i })
      expect(dashboardLink).not.toBeInTheDocument()
    })

    it('does not show Logout button when not authenticated', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const logoutButton = screen.queryByRole('button', { name: /logout/i })
      expect(logoutButton).not.toBeInTheDocument()
    })
  })

  describe('Test Case 2: Authenticated user view', () => {
    // Component that properly logs in using useEffect to avoid setState in render
    function AuthenticatedHome() {
      const { login, isAuthenticated } = useAuth()
      const [loginTriggered, setLoginTriggered] = useState(false)

      useEffect(() => {
        if (!isAuthenticated && !loginTriggered) {
          setLoginTriggered(true)
          login('test@example.com', 'password')
        }
      }, [isAuthenticated, login, loginTriggered])

      return <Home />
    }

    it('shows Dashboard link when user is logged in', async () => {
      render(
        <TestWrapper>
          <AuthenticatedHome />
        </TestWrapper>
      )

      // Wait for auth state to update
      await waitFor(() => {
        const dashboardLink = screen.queryByRole('link', { name: /dashboard/i })
        expect(dashboardLink).toBeInTheDocument()
      })
    })

    it('shows Logout button when user is logged in', async () => {
      render(
        <TestWrapper>
          <AuthenticatedHome />
        </TestWrapper>
      )

      await waitFor(() => {
        const logoutButton = screen.queryByRole('button', { name: /logout/i })
        expect(logoutButton).toBeInTheDocument()
      })
    })

    it('hides Login and Register buttons in navbar when user is logged in', async () => {
      render(
        <TestWrapper>
          <AuthenticatedHome />
        </TestWrapper>
      )

      await waitFor(() => {
        // Get the main navbar (not the footer navigation)
        const navbar = screen.getByRole('navigation', { name: '' })

        // Should not show Login or Register in navbar when authenticated
        const loginLink = within(navbar).queryByRole('link', { name: /login/i })
        const registerLink = within(navbar).queryByRole('link', { name: /register/i })
        expect(loginLink).not.toBeInTheDocument()
        expect(registerLink).not.toBeInTheDocument()
      })
    })

    it('Dashboard link navigates to /dashboard', async () => {
      render(
        <TestWrapper>
          <AuthenticatedHome />
        </TestWrapper>
      )

      await waitFor(() => {
        const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
        expect(dashboardLink).toHaveAttribute('href', '/dashboard')
      })
    })
  })

  describe('Test Case 3: AuthContext consumption in Home component', () => {
    it('Home component renders within AuthProvider without errors', () => {
      expect(() => {
        render(
          <TestWrapper>
            <Home />
          </TestWrapper>
        )
      }).not.toThrow()
    })

    it('Navbar uses useAuth hook for conditional rendering', () => {
      // This test verifies that the Navbar correctly uses AuthContext
      // by checking that the conditional rendering works
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // The navbar should be present
      const navbar = screen.getByRole('navigation', { name: '' })
      expect(navbar).toBeInTheDocument()

      // Unauthenticated state should show Login/Register in navbar
      expect(within(navbar).getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(within(navbar).getByRole('link', { name: /register/i })).toBeInTheDocument()
    })

    it('throws error when useAuth is used outside AuthProvider', () => {
      // Create a component that uses useAuth
      function ComponentUsingAuth() {
        const { isAuthenticated } = useAuth()
        return <div>{isAuthenticated ? 'Yes' : 'No'}</div>
      }

      // Should throw when rendered without AuthProvider
      expect(() => {
        // Suppress console.error for this test
        const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

        try {
          render(
            <MemoryRouter>
              <ComponentUsingAuth />
            </MemoryRouter>
          )
        } finally {
          consoleSpy.mockRestore()
        }
      }).toThrow('useAuth must be used within an AuthProvider')
    })

    it('authentication state changes are reflected in UI', async () => {
      function ToggleAuthHome() {
        const { login, logout, isAuthenticated } = useAuth()

        return (
          <div>
            <Home />
            <button
              data-testid="toggle-auth"
              onClick={() => isAuthenticated ? logout() : login('test@example.com', 'password')}
            >
              Toggle Auth
            </button>
          </div>
        )
      }

      const user = userEvent.setup()

      render(
        <TestWrapper>
          <ToggleAuthHome />
        </TestWrapper>
      )

      // Get navbar for scoped queries
      const navbar = screen.getByRole('navigation', { name: '' })

      // Initially unauthenticated - should show Login in navbar
      expect(within(navbar).getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.queryByRole('link', { name: /dashboard/i })).not.toBeInTheDocument()

      // Toggle to authenticated
      await user.click(screen.getByTestId('toggle-auth'))

      // Should now show Dashboard
      await waitFor(() => {
        expect(screen.getByRole('link', { name: /dashboard/i })).toBeInTheDocument()
      })

      // Toggle back to unauthenticated
      await user.click(screen.getByTestId('toggle-auth'))

      // Should show Login again in navbar
      await waitFor(() => {
        expect(within(navbar).getByRole('link', { name: /login/i })).toBeInTheDocument()
        expect(screen.queryByRole('link', { name: /dashboard/i })).not.toBeInTheDocument()
      })
    })
  })
})
