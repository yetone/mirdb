/**
 * Route navigation integration tests.
 * Owner: Scenario 16 - Route Configuration
 *
 * Tests React Router configuration for homepage:
 * - / route renders Home component
 * - Route is public (no authentication required)
 * - Navigation from homepage to /login works
 * - Navigation from homepage to /register works
 */

import React from 'react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'
import { Home } from '@/pages/Home'
import { Login } from '@/pages/Login'
import { Register } from '@/pages/Register'
import App from '@/App'

// Create a fresh QueryClient for each test
const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
    get store() {
      return store
    },
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Test wrapper with all providers
const TestWrapper = ({
  children,
  initialEntries = ['/'],
}: {
  children: React.ReactNode
  initialEntries?: string[]
}) => {
  const queryClient = createTestQueryClient()
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <AuthProvider>
          <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  )
}

// Full app test wrapper with routing
const FullAppWrapper = ({ initialEntries = ['/'] }: { initialEntries?: string[] }) => {
  const queryClient = createTestQueryClient()
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <AuthProvider>
          <MemoryRouter initialEntries={initialEntries}>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/login" element={<Login />} />
              <Route path="/register" element={<Register />} />
            </Routes>
          </MemoryRouter>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  )
}

describe('Route Configuration', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Navigate to / route', () => {
    /**
     * Integration Test: Home component renders at / route
     * Expected: Home component renders, not redirect to login
     */
    it('renders Home component at root path /', () => {
      render(<FullAppWrapper initialEntries={['/']} />)

      // Home page should render with hero section
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('displays homepage headline and value proposition', () => {
      render(<FullAppWrapper initialEntries={['/']} />)

      // Should have a main heading with the product value proposition
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toBeInTheDocument()
      expect(heading).toBeVisible()
    })

    it('does not redirect to login page when visiting root', () => {
      render(<FullAppWrapper initialEntries={['/']} />)

      // Should NOT see login page elements
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument()
      expect(screen.queryByTestId('login-form')).not.toBeInTheDocument()

      // Should see homepage elements
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('renders all homepage sections at root path', () => {
      render(<FullAppWrapper initialEntries={['/']} />)

      // Hero section
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Features grid
      expect(screen.getByTestId('features-grid')).toBeInTheDocument()

      // URL demo section
      expect(screen.getByTestId('url-demo-section')).toBeInTheDocument()

      // Footer
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Verify route is public', () => {
    /**
     * Integration Test: Homepage is accessible without authentication
     * Expected: Homepage accessible without authentication
     */
    it('renders homepage without authentication token', () => {
      // Ensure no token exists
      localStorageMock.removeItem('token')

      render(<FullAppWrapper initialEntries={['/']} />)

      // Homepage should render successfully
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-grid')).toBeInTheDocument()
    })

    it('homepage is accessible to unauthenticated users', () => {
      // No authentication setup
      localStorageMock.clear()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Should be able to see CTAs for registration
      expect(screen.getByTestId('hero-primary-cta')).toBeInTheDocument()
      expect(screen.getByTestId('hero-login-cta')).toBeInTheDocument()
    })

    it('does not require user context to render homepage', () => {
      render(<FullAppWrapper initialEntries={['/']} />)

      // All main sections should be visible
      expect(screen.getByTestId('hero-section')).toBeVisible()
      expect(screen.getByTestId('features-grid')).toBeVisible()
      expect(screen.getByTestId('url-demo-section')).toBeVisible()
    })

    it('interactive URL demo is available without login', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // URL input should be available
      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toBeInTheDocument()
      expect(urlInput).toBeEnabled()

      // User can interact with it
      await user.type(urlInput, 'https://example.com')
      expect(urlInput).toHaveValue('https://example.com')
    })
  })

  describe('Test Case 3: Navigate from homepage to /login', () => {
    /**
     * E2E-style Integration Test: Navigation to login works
     * Expected: Navigation works correctly via React Router
     */
    it('navigates to login page when clicking login CTA', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Find and click login link in hero section
      const loginCta = screen.getByTestId('hero-login-cta')
      await user.click(loginCta)

      // Should now be on login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })

    it('login page renders correctly after navigation', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      const loginCta = screen.getByTestId('hero-login-cta')
      await user.click(loginCta)

      await waitFor(() => {
        // Login heading should be visible
        expect(screen.getByTestId('login-heading')).toBeInTheDocument()
        expect(screen.getByTestId('login-heading')).toHaveTextContent('Login')

        // Login form should be present
        expect(screen.getByTestId('login-form')).toBeInTheDocument()
      })
    })

    it('navigates to login via navbar login link', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Navbar has login links (may have multiple for responsive design)
      const navLoginLinks = screen.getAllByRole('link', { name: /login/i })
      // Click the first visible one
      await user.click(navLoginLinks[0])

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })

    it('can return to homepage from login page', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Navigate to login
      const loginCta = screen.getByTestId('hero-login-cta')
      await user.click(loginCta)

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Click back to home link
      const backToHome = screen.getByRole('link', { name: /back to home/i })
      await user.click(backToHome)

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 4: Navigate from homepage to /register', () => {
    /**
     * E2E-style Integration Test: Navigation to register works
     * Expected: Navigation works correctly via React Router
     */
    it('navigates to register page when clicking primary CTA', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Primary CTA should navigate to registration
      const registerCta = screen.getByTestId('hero-primary-cta')
      await user.click(registerCta)

      // Should now be on register page
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument()
      })
    })

    it('register page renders correctly after navigation', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      const registerCta = screen.getByTestId('hero-primary-cta')
      await user.click(registerCta)

      await waitFor(() => {
        // Register heading should be visible
        const heading = screen.getByRole('heading', { name: /register/i })
        expect(heading).toBeInTheDocument()
        expect(heading).toBeVisible()
      })
    })

    it('navigates to register via navbar link', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Navbar should have register links
      const navRegisterLinks = screen.getAllByRole('link', { name: /register|sign up|get started/i })
      await user.click(navRegisterLinks[0])

      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument()
      })
    })

    it('can return to homepage from register page', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Navigate to register
      const registerCta = screen.getByTestId('hero-primary-cta')
      await user.click(registerCta)

      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument()
      })

      // Click back to home link
      const backToHome = screen.getByRole('link', { name: /back to home/i })
      await user.click(backToHome)

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })
    })
  })

  describe('Additional Route Configuration Tests', () => {
    /**
     * Additional coverage for route configuration edge cases
     */
    it('routes match lowercase paths correctly', () => {
      render(<FullAppWrapper initialEntries={['/login']} />)

      // Lowercase /login should match login route
      expect(screen.getByTestId('login-page')).toBeInTheDocument()
    })

    it('direct navigation to /login works', () => {
      render(<FullAppWrapper initialEntries={['/login']} />)

      expect(screen.getByTestId('login-page')).toBeInTheDocument()
      expect(screen.getByTestId('login-heading')).toHaveTextContent('Login')
    })

    it('direct navigation to /register works', () => {
      render(<FullAppWrapper initialEntries={['/register']} />)

      expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument()
    })

    it('homepage, login, and register all render within the same app context', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Start on homepage
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Navigate to login
      await user.click(screen.getByTestId('hero-login-cta'))
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Navigate back to home
      await user.click(screen.getByRole('link', { name: /back to home/i }))
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Navigate to register
      await user.click(screen.getByTestId('hero-primary-cta'))
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument()
      })
    })

    it('ThemeProvider context is available across all routes', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Theme selector should be in navbar on homepage
      const themeSelectors = screen.getAllByLabelText('Select theme')
      expect(themeSelectors.length).toBeGreaterThan(0)

      // Navigate to login and verify theme selector still works
      await user.click(screen.getByTestId('hero-login-cta'))

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Theme selector should still be available on login page
      const loginThemeSelectors = screen.getAllByLabelText('Select theme')
      expect(loginThemeSelectors.length).toBeGreaterThan(0)
    })

    it('AuthProvider context is available across all routes', async () => {
      const user = userEvent.setup()

      render(<FullAppWrapper initialEntries={['/']} />)

      // Homepage renders (uses AuthProvider)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Navigate to login
      await user.click(screen.getByTestId('hero-login-cta'))

      await waitFor(() => {
        // Login page should render without errors (AuthProvider available)
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
        expect(screen.getByTestId('login-form')).toBeInTheDocument()
      })
    })
  })
})
