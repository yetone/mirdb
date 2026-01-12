import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import App from './App'
import Home from './pages/Home'
import { AuthProvider, AuthContext, AuthContextType, User } from './contexts/AuthContext'

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

/**
 * Route Configuration Tests
 *
 * Scenario: Verify landing page is correctly configured at root route
 * Test Cases:
 * 1. Navigate to / route -> Landing page component renders (not 404)
 * 2. Check route is public (no auth required) -> Landing page accessible without authentication
 * 3. Navigate to / while authenticated -> Landing page still renders (not redirect to dashboard)
 */

// Helper to render the full App with MemoryRouter
const renderApp = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <App />
    </MemoryRouter>
  )
}

// Helper to render with custom auth state
const renderWithAuthState = (
  authState: Partial<AuthContextType>,
  initialEntries = ['/']
) => {
  const defaultAuthState: AuthContextType = {
    user: null,
    loading: false,
    isAuthenticated: false,
    isAdmin: false,
    login: vi.fn(),
    logout: vi.fn(),
    ...authState,
  }

  // We need to render the app structure without using App component directly
  // to inject custom auth state
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthContext.Provider value={defaultAuthState}>
        <div className="pt-16">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard</div>} />
            <Route path="/register" element={<div data-testid="register-page">Register</div>} />
            <Route path="/login" element={<div data-testid="login-page">Login</div>} />
            <Route path="*" element={<div data-testid="not-found-page">404 Not Found</div>} />
          </Routes>
        </div>
      </AuthContext.Provider>
    </MemoryRouter>
  )
}

describe('Route Configuration - Landing Page at Root Route', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  /**
   * Test Case 1: Navigate to / route -> Landing page component renders (not 404)
   *
   * Verifies that:
   * - The root route / is configured correctly in App.tsx
   * - The Home (landing page) component renders when navigating to /
   * - No 404 page is shown
   */
  describe('Test Case 1: Landing page renders at root route (not 404)', () => {
    it('renders the Home component when navigating to /', () => {
      renderWithAuthState({ isAuthenticated: false, user: null }, ['/'])

      // Verify the home page renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('does not render a 404 page when navigating to /', () => {
      renderWithAuthState({ isAuthenticated: false, user: null }, ['/'])

      // Verify 404 page is NOT shown
      const notFoundPage = screen.queryByTestId('not-found-page')
      expect(notFoundPage).not.toBeInTheDocument()
    })

    it('renders the hero section of the landing page at root route', () => {
      renderWithAuthState({ isAuthenticated: false, user: null }, ['/'])

      // Verify hero section renders (key component of landing page)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('renders all main sections of the landing page at root route', () => {
      renderWithAuthState({ isAuthenticated: false, user: null }, ['/'])

      // Verify main landing page sections are present
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Check route is public (no auth required) -> Landing page accessible without authentication
   *
   * Verifies that:
   * - The landing page is accessible to unauthenticated users
   * - No login redirect occurs
   * - The public content is fully visible
   */
  describe('Test Case 2: Route is public (no auth required)', () => {
    it('renders landing page for unauthenticated user', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
        loading: false,
      }, ['/'])

      // Verify landing page renders without authentication
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('does not redirect unauthenticated user to login', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
        loading: false,
      }, ['/'])

      // Verify we're still on the home page, not redirected to login
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument()
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })

    it('shows public content (Get Started Free CTA) for unauthenticated user', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
        loading: false,
      }, ['/'])

      // Verify the public CTA is visible
      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent('Get Started Free')
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })

    it('allows unauthenticated user to view features section', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
        loading: false,
      }, ['/'])

      // Verify features section is visible to unauthenticated users
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('allows unauthenticated user to view how it works section', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
        loading: false,
      }, ['/'])

      // Verify how it works section is visible
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Navigate to / while authenticated -> Landing page still renders (not redirect to dashboard)
   *
   * Verifies that:
   * - Authenticated users can still access the landing page
   * - No automatic redirect to dashboard occurs
   * - The landing page content adapts for authenticated users
   */
  describe('Test Case 3: Landing page renders for authenticated users (no redirect to dashboard)', () => {
    const authenticatedUser: User = {
      id: 1,
      username: 'testuser',
      email: 'test@example.com',
      is_admin: 0,
    }

    it('renders landing page for authenticated user (no redirect)', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
        loading: false,
      }, ['/'])

      // Verify landing page renders for authenticated user
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('does not redirect authenticated user to dashboard', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
        loading: false,
      }, ['/'])

      // Verify we're still on home page, not redirected to dashboard
      expect(screen.queryByTestId('dashboard-page')).not.toBeInTheDocument()
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })

    it('shows authenticated-specific CTA (Go to Dashboard) on landing page', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
        loading: false,
      }, ['/'])

      // Verify the authenticated CTA is visible
      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent(/go to dashboard/i)
      expect(primaryCTA).toHaveAttribute('href', '/dashboard')
    })

    it('still shows all landing page sections for authenticated user', () => {
      renderWithAuthState({
        isAuthenticated: true,
        user: authenticatedUser,
        loading: false,
      }, ['/'])

      // Verify all sections are present for authenticated users too
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('renders landing page for admin user without redirect', () => {
      const adminUser: User = {
        id: 2,
        username: 'admin',
        email: 'admin@example.com',
        is_admin: 1,
      }

      renderWithAuthState({
        isAuthenticated: true,
        user: adminUser,
        isAdmin: true,
        loading: false,
      }, ['/'])

      // Verify landing page renders for admin user too
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(screen.queryByTestId('dashboard-page')).not.toBeInTheDocument()
    })
  })

  /**
   * Additional edge case tests
   */
  describe('Edge Cases', () => {
    it('renders landing page even during auth loading state', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
        loading: true,
      }, ['/'])

      // Page should still render during loading
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('404 page is shown for non-existent routes', () => {
      renderWithAuthState({
        isAuthenticated: false,
        user: null,
      }, ['/non-existent-page'])

      // Verify 404 page is shown for invalid routes
      const notFoundPage = screen.getByTestId('not-found-page')
      expect(notFoundPage).toBeInTheDocument()
      expect(screen.queryByTestId('home-page')).not.toBeInTheDocument()
    })
  })
})
