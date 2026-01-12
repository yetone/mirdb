import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from './Home'
import Navbar from '../components/Navbar'
import { AuthProvider, AuthContext } from '../contexts/AuthContext'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

interface User {
  id: number
  username: string
  email: string
  is_admin: number
}

interface AuthContextValue {
  user: User | null
  loading: boolean
  isAuthenticated: boolean
  isAdmin: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => void
}

// Helper to render with custom auth state - includes Navbar like App.tsx
const renderWithAuthState = (
  authState: Partial<AuthContextValue>,
  initialEntries = ['/']
) => {
  const defaultAuthState: AuthContextValue = {
    user: null,
    loading: false,
    isAuthenticated: false,
    isAdmin: false,
    login: vi.fn(),
    logout: vi.fn(),
    ...authState,
  }

  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthContext.Provider value={defaultAuthState}>
        <Navbar />
        <div className="pt-16">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard</div>} />
            <Route path="/register" element={<div data-testid="register-page">Register</div>} />
            <Route path="/login" element={<div data-testid="login-page">Login</div>} />
          </Routes>
        </div>
      </AuthContext.Provider>
    </MemoryRouter>
  )
}

// Helper to render with default AuthProvider (unauthenticated)
const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthProvider>
        <Navbar />
        <div className="pt-16">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard</div>} />
            <Route path="/register" element={<div data-testid="register-page">Register</div>} />
            <Route path="/login" element={<div data-testid="login-page">Login</div>} />
          </Routes>
        </div>
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Authentication Context Integration - Landing Page', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  // Test Case 1: Unauthenticated user sees Login and Register buttons in header
  describe('Test Case 1: Unauthenticated User Header CTAs', () => {
    it('renders Login and Register buttons visible in header for unauthenticated user', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
      })

      // Check Login button is visible
      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveTextContent('Login')
      expect(loginButton).toHaveAttribute('href', '/login')

      // Check Register button is visible
      const registerButton = screen.getByTestId('nav-register')
      expect(registerButton).toBeInTheDocument()
      expect(registerButton).toHaveTextContent('Register')
      expect(registerButton).toHaveAttribute('href', '/register')
    })

    it('does not show Dashboard link or user menu for unauthenticated user', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
      })

      // Dashboard link should not exist for unauthenticated users
      const dashboardLink = screen.queryByTestId('nav-dashboard')
      expect(dashboardLink).not.toBeInTheDocument()

      // User menu should not exist for unauthenticated users
      const userMenu = screen.queryByTestId('nav-user-menu')
      expect(userMenu).not.toBeInTheDocument()
    })
  })

  // Test Case 2: Authenticated user sees different CTAs (Dashboard link or user menu)
  describe('Test Case 2: Authenticated User Header CTAs', () => {
    const authenticatedUser: User = {
      id: 1,
      username: 'testuser',
      email: 'test@example.com',
      is_admin: 0,
    }

    it('renders Go to Dashboard link for authenticated user', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
      })

      // Check Dashboard link is visible
      const dashboardLink = screen.getByTestId('nav-dashboard')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveTextContent(/dashboard/i)
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('hides Login and Register buttons for authenticated user', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
      })

      // Login button should not exist for authenticated users
      const loginButton = screen.queryByTestId('nav-login')
      expect(loginButton).not.toBeInTheDocument()

      // Register button should not exist for authenticated users
      const registerButton = screen.queryByTestId('nav-register')
      expect(registerButton).not.toBeInTheDocument()
    })
  })

  // Test Case 3: Hero CTA for unauthenticated user shows 'Get Started Free' linking to /register
  describe('Test Case 3: Unauthenticated User Hero CTA', () => {
    it('renders primary CTA as Get Started Free linking to /register for unauthenticated user', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
      })

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent('Get Started Free')
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })

    it('renders hero section with correct styling for unauthenticated user', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
      })

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toHaveClass('btn-primary')
    })
  })

  // Test Case 4: Hero CTA for authenticated user shows dashboard-related action
  describe('Test Case 4: Authenticated User Hero CTA', () => {
    const authenticatedUser: User = {
      id: 1,
      username: 'testuser',
      email: 'test@example.com',
      is_admin: 0,
    }

    it('renders primary CTA as Go to Dashboard linking to /dashboard for authenticated user', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
      })

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent(/go to dashboard/i)
      expect(primaryCTA).toHaveAttribute('href', '/dashboard')
    })

    it('does not show Get Started Free for authenticated user', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
      })

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).not.toHaveTextContent('Get Started Free')
    })
  })

  // Additional integration tests
  describe('Auth State Loading', () => {
    it('shows appropriate UI during auth loading state', () => {
      renderWithAuthState({
        loading: true,
        isAuthenticated: false,
        user: null,
      })

      // During loading, the page should still render (not block)
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })
  })

  describe('Auth Context Provider Integration', () => {
    it('works correctly with AuthProvider wrapper', () => {
      renderWithRouter()

      // Should render as unauthenticated by default
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Should show unauthenticated CTAs in navbar
      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toBeInTheDocument()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toHaveTextContent('Get Started Free')
    })
  })
})
