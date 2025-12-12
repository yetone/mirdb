import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { AuthProvider, useAuth, type User } from './AuthContext'

// Test component that uses the auth context
function TestComponent() {
  const { user, isAuthenticated, login, logout } = useAuth()

  return (
    <div>
      <span data-testid="auth-status">
        {isAuthenticated ? 'authenticated' : 'unauthenticated'}
      </span>
      {user && <span data-testid="user-name">{user.name}</span>}
      <button
        data-testid="login-btn"
        onClick={() => login({ id: '1', name: 'Test User', email: 'test@example.com' })}
      >
        Login
      </button>
      <button data-testid="logout-btn" onClick={logout}>
        Logout
      </button>
    </div>
  )
}

describe('AuthContext', () => {
  describe('Initial State', () => {
    it('should start as unauthenticated by default', () => {
      render(
        <AuthProvider>
          <TestComponent />
        </AuthProvider>
      )

      expect(screen.getByTestId('auth-status')).toHaveTextContent('unauthenticated')
      expect(screen.queryByTestId('user-name')).not.toBeInTheDocument()
    })

    it('should accept initial user for authenticated state', () => {
      const initialUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      render(
        <AuthProvider initialUser={initialUser}>
          <TestComponent />
        </AuthProvider>
      )

      expect(screen.getByTestId('auth-status')).toHaveTextContent('authenticated')
      expect(screen.getByTestId('user-name')).toHaveTextContent('John Doe')
    })
  })

  describe('Login functionality', () => {
    it('should update to authenticated state after login', async () => {
      const user = userEvent.setup()

      render(
        <AuthProvider>
          <TestComponent />
        </AuthProvider>
      )

      expect(screen.getByTestId('auth-status')).toHaveTextContent('unauthenticated')

      await user.click(screen.getByTestId('login-btn'))

      expect(screen.getByTestId('auth-status')).toHaveTextContent('authenticated')
      expect(screen.getByTestId('user-name')).toHaveTextContent('Test User')
    })
  })

  describe('Logout functionality', () => {
    it('should update to unauthenticated state after logout', async () => {
      const user = userEvent.setup()
      const initialUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      render(
        <AuthProvider initialUser={initialUser}>
          <TestComponent />
        </AuthProvider>
      )

      expect(screen.getByTestId('auth-status')).toHaveTextContent('authenticated')

      await user.click(screen.getByTestId('logout-btn'))

      expect(screen.getByTestId('auth-status')).toHaveTextContent('unauthenticated')
      expect(screen.queryByTestId('user-name')).not.toBeInTheDocument()
    })
  })

  describe('Error handling', () => {
    it('should throw error when useAuth is used outside provider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      expect(() => {
        render(<TestComponent />)
      }).toThrow('useAuth must be used within an AuthProvider')

      consoleSpy.mockRestore()
    })
  })
})
