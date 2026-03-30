/**
 * Integration tests for homepage authenticated user experience.
 * Owner: Scenario 11 - Authenticated User Personalized Experience
 *
 * Tests that authenticated users see personalized content including:
 * - Welcome message with their username
 * - Dashboard and Settings navigation links (instead of Login/Register)
 * - URL shortening redirects to dashboard (not register)
 * - Admin-specific navigation options
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import React, { createContext, useContext, ReactNode } from 'react'
import Navbar from '@/components/Navbar'
import Home from '@/pages/Home'
import BackgroundEffect from '@/components/BackgroundEffect'
import { HeroSection } from '@/components/homepage'
import { ThemeProvider } from '@/contexts/ThemeContext'

/**
 * Mock user data types
 */
interface MockUser {
  id: number
  email: string
  username: string
  is_admin: boolean
}

interface MockAuthContextType {
  user: MockUser | null
  isAuthenticated: boolean
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => void
  register: (email: string, username: string, password: string) => Promise<void>
}

/**
 * Create a mock AuthContext for testing with custom user data
 */
const MockAuthContext = createContext<MockAuthContextType | undefined>(undefined)

interface MockAuthProviderProps {
  children: ReactNode
  user?: MockUser | null
  isAuthenticated?: boolean
}

const MockAuthProvider: React.FC<MockAuthProviderProps> = ({
  children,
  user = null,
  isAuthenticated = false,
}) => {
  const mockAuthValue: MockAuthContextType = {
    user,
    isAuthenticated,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  }

  return (
    <MockAuthContext.Provider value={mockAuthValue}>
      {children}
    </MockAuthContext.Provider>
  )
}

/**
 * Mock useAuth hook that uses our test context
 */
vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => {
    const context = useContext(MockAuthContext)
    if (context === undefined) {
      throw new Error('useAuth must be used within an AuthProvider')
    }
    return context
  },
  AuthProvider: ({ children }: { children: ReactNode }) => <>{children}</>,
}))

/**
 * Location tracker component for testing navigation
 */
const LocationDisplay: React.FC = () => {
  const location = useLocation()
  return (
    <div data-testid="location-display" data-pathname={location.pathname}>
      {location.pathname}
    </div>
  )
}

/**
 * Custom render helper for auth tests using MemoryRouter
 */
const renderWithAuth = (
  ui: React.ReactElement,
  {
    initialEntries = ['/'],
    user = null,
    isAuthenticated = false,
  }: {
    initialEntries?: string[]
    user?: MockUser | null
    isAuthenticated?: boolean
  } = {}
) => {
  let currentPathname = initialEntries[0]

  const LocationTracker = () => {
    const location = useLocation()
    currentPathname = location.pathname
    return <LocationDisplay />
  }

  const result = render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <MockAuthProvider user={user} isAuthenticated={isAuthenticated}>
          {ui}
          <Routes>
            <Route path="*" element={<LocationTracker />} />
          </Routes>
        </MockAuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )

  return {
    ...result,
    getCurrentLocation: () => currentPathname,
  }
}

/**
 * Test user fixtures
 */
const testUser: MockUser = {
  id: 1,
  email: 'testuser@example.com',
  username: 'testuser',
  is_admin: false,
}

const adminUser: MockUser = {
  id: 2,
  email: 'admin@example.com',
  username: 'adminuser',
  is_admin: true,
}

describe('Homepage Authentication Integration Tests', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 1: Welcome message displays containing username', () => {
    it('should display welcome message with username for authenticated user', () => {
      renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      // Look for the welcome message containing the username
      const welcomeMessage = screen.getByText(/welcome/i)
      expect(welcomeMessage).toBeInTheDocument()
      expect(welcomeMessage).toHaveTextContent('testuser')
    })

    it('should display personalized welcome with exact username', () => {
      renderWithAuth(<Navbar />, {
        user: { ...testUser, username: 'johndoe' },
        isAuthenticated: true,
      })

      expect(screen.getByText(/johndoe/i)).toBeInTheDocument()
    })

    it('should not display welcome message for unauthenticated users', () => {
      renderWithAuth(<Navbar />, {
        user: null,
        isAuthenticated: false,
      })

      const welcomeMessage = screen.queryByText(/welcome/i)
      expect(welcomeMessage).not.toBeInTheDocument()
    })

    it('should display welcome message in the navbar area', () => {
      renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      const navbar = screen.getByRole('navigation')
      const welcomeMessage = within(navbar).getByText(/testuser/i)
      expect(welcomeMessage).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Dashboard link visible, Login/Register links hidden', () => {
    it('should display Dashboard link for authenticated users', () => {
      renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('should hide Login link for authenticated users', () => {
      renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      const loginLink = screen.queryByRole('link', { name: /^login$/i })
      expect(loginLink).not.toBeInTheDocument()
    })

    it('should hide Sign Up/Register link for authenticated users', () => {
      renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      const signUpLink = screen.queryByRole('link', { name: /sign up/i })
      const registerLink = screen.queryByRole('link', { name: /register/i })
      expect(signUpLink).not.toBeInTheDocument()
      expect(registerLink).not.toBeInTheDocument()
    })

    it('should show Logout button for authenticated users', () => {
      renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      const logoutButton = screen.getByRole('button', { name: /logout/i })
      expect(logoutButton).toBeInTheDocument()
    })

    it('should navigate to dashboard when Dashboard link is clicked', async () => {
      const user = userEvent.setup()
      const { getCurrentLocation } = renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
      await user.click(dashboardLink)

      expect(getCurrentLocation()).toBe('/dashboard')
    })
  })

  describe('Test Case 3: URL shortening redirects authenticated user to dashboard', () => {
    it('should redirect to /dashboard when authenticated user submits URL', async () => {
      const user = userEvent.setup()

      const { getCurrentLocation } = renderWithAuth(<Home />, {
        user: testUser,
        isAuthenticated: true,
      })

      // Find URL input and submit a URL
      const urlInput = screen.getByPlaceholderText(/enter.*url/i)
      await user.type(urlInput, 'https://example.com')

      // Click the shorten button
      const shortenButton = screen.getByRole('button', { name: /shorten/i })
      await user.click(shortenButton)

      // Should redirect to dashboard, not register
      await waitFor(() => {
        expect(getCurrentLocation()).toBe('/dashboard')
      })
    })

    it('should NOT redirect to /register for authenticated users', async () => {
      const user = userEvent.setup()

      const { getCurrentLocation } = renderWithAuth(<Home />, {
        user: testUser,
        isAuthenticated: true,
      })

      const urlInput = screen.getByPlaceholderText(/enter.*url/i)
      await user.type(urlInput, 'https://example.com')

      const shortenButton = screen.getByRole('button', { name: /shorten/i })
      await user.click(shortenButton)

      await waitFor(() => {
        const currentPath = getCurrentLocation()
        expect(currentPath).not.toBe('/register')
      })
    })

    it('should store URL in localStorage for later use', async () => {
      const user = userEvent.setup()

      renderWithAuth(<Home />, {
        user: testUser,
        isAuthenticated: true,
      })

      const urlInput = screen.getByPlaceholderText(/enter.*url/i)
      await user.type(urlInput, 'https://example.com')

      const shortenButton = screen.getByRole('button', { name: /shorten/i })
      await user.click(shortenButton)

      expect(localStorage.setItem).toHaveBeenCalledWith('pendingUrl', 'https://example.com')
    })

    it('should not show Get Started CTA for authenticated users', () => {
      renderWithAuth(<Home />, {
        user: testUser,
        isAuthenticated: true,
      })

      const getStartedLink = screen.queryByRole('link', { name: /get started/i })
      expect(getStartedLink).not.toBeInTheDocument()
    })
  })

  describe('Test Case 4: Settings link visible for admin users', () => {
    it('should display Settings link for admin users', () => {
      renderWithAuth(<Navbar />, {
        user: adminUser,
        isAuthenticated: true,
      })

      const settingsLink = screen.getByRole('link', { name: /settings/i })
      expect(settingsLink).toBeInTheDocument()
      expect(settingsLink).toHaveAttribute('href', '/settings')
    })

    it('should NOT display Settings link for non-admin users', () => {
      renderWithAuth(<Navbar />, {
        user: testUser,
        isAuthenticated: true,
      })

      const settingsLink = screen.queryByRole('link', { name: /settings/i })
      expect(settingsLink).not.toBeInTheDocument()
    })

    it('should still show Dashboard link for admin users', () => {
      renderWithAuth(<Navbar />, {
        user: adminUser,
        isAuthenticated: true,
      })

      const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
      expect(dashboardLink).toBeInTheDocument()
    })

    it('should navigate to settings when Settings link is clicked', async () => {
      const user = userEvent.setup()
      const { getCurrentLocation } = renderWithAuth(<Navbar />, {
        user: adminUser,
        isAuthenticated: true,
      })

      const settingsLink = screen.getByRole('link', { name: /settings/i })
      await user.click(settingsLink)

      expect(getCurrentLocation()).toBe('/settings')
    })
  })

  describe('Authentication state edge cases', () => {
    it('should handle user with empty username gracefully', () => {
      renderWithAuth(<Navbar />, {
        user: { ...testUser, username: '' },
        isAuthenticated: true,
      })

      // Should still show Dashboard link even with empty username
      const dashboardLink = screen.getByRole('link', { name: /dashboard/i })
      expect(dashboardLink).toBeInTheDocument()
    })

    it('should handle switching from authenticated to unauthenticated', () => {
      const { rerender } = render(
        <MemoryRouter>
          <ThemeProvider>
            <MockAuthProvider user={testUser} isAuthenticated={true}>
              <Navbar />
            </MockAuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      )

      // Initially authenticated
      expect(screen.getByRole('link', { name: /dashboard/i })).toBeInTheDocument()

      // Rerender as unauthenticated
      rerender(
        <MemoryRouter>
          <ThemeProvider>
            <MockAuthProvider user={null} isAuthenticated={false}>
              <Navbar />
            </MockAuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      )

      // Should now show Login/Sign Up
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.queryByRole('link', { name: /dashboard/i })).not.toBeInTheDocument()
    })
  })

  describe('Full homepage integration with auth', () => {
    it('should render complete homepage with authenticated navigation', () => {
      renderWithAuth(
        <>
          <Navbar />
          <Home />
        </>,
        {
          user: testUser,
          isAuthenticated: true,
        }
      )

      // Check navbar elements
      expect(screen.getByText(/testuser/i)).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /dashboard/i })).toBeInTheDocument()

      // Check homepage elements
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('should maintain authentication state across homepage components', () => {
      renderWithAuth(
        <>
          <Navbar />
          <Home />
        </>,
        {
          user: adminUser,
          isAuthenticated: true,
        }
      )

      // Admin should see Settings in navbar
      expect(screen.getByRole('link', { name: /settings/i })).toBeInTheDocument()

      // Get Started should be hidden for authenticated users
      expect(screen.queryByRole('link', { name: /get started/i })).not.toBeInTheDocument()
    })
  })
})

/**
 * Component Integration with Existing System Tests
 * Owner: Scenario 18 - Component Integration with Existing System
 *
 * Tests that verify homepage integrates correctly with:
 * - AuthContext for user state
 * - ThemeContext for theming
 * - React Router for routing
 * - Shared components (Navbar, BackgroundEffect)
 */
describe('Component Integration with Existing System (Scenario 18)', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 1: Render Homepage with mocked AuthContext', () => {
    it('should render homepage without errors when AuthContext is provided', () => {
      renderWithAuth(<Home />, {
        user: null,
        isAuthenticated: false,
      })

      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('should consume auth state correctly when user is authenticated', () => {
      renderWithAuth(<Home />, {
        user: testUser,
        isAuthenticated: true,
      })

      // Homepage should render
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      // Get Started button should NOT be visible for authenticated users
      const getStartedLink = screen.queryByRole('link', { name: /get started/i })
      expect(getStartedLink).not.toBeInTheDocument()
    })

    it('should consume auth state correctly when user is not authenticated', () => {
      renderWithAuth(<Home />, {
        user: null,
        isAuthenticated: false,
      })

      // Homepage should render
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      // Get Started button SHOULD be visible for unauthenticated users
      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(getStartedLink).toBeInTheDocument()
    })

    it('should access isAuthenticated from AuthContext without errors', () => {
      // Should not throw when rendering with different auth states
      expect(() => {
        renderWithAuth(<Home />, { user: null, isAuthenticated: false })
      }).not.toThrow()
    })
  })

  describe('Test Case 2: Render Homepage with mocked ThemeContext', () => {
    it('should render homepage without errors when ThemeContext is provided', () => {
      renderWithAuth(<Home />, {
        user: null,
        isAuthenticated: false,
      })

      // ThemeProvider is included in renderWithAuth
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should render with dark theme applied correctly', () => {
      // Clear any existing theme
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      renderWithAuth(<Home />, {
        user: null,
        isAuthenticated: false,
      })

      // Theme should be applied to document
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should render with cyberpunk theme applied correctly', () => {
      vi.mocked(localStorage.getItem).mockReturnValue('cyberpunk')

      renderWithAuth(<Home />, {
        user: null,
        isAuthenticated: false,
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should render with synthwave theme applied correctly', () => {
      vi.mocked(localStorage.getItem).mockReturnValue('synthwave')

      renderWithAuth(<Home />, {
        user: null,
        isAuthenticated: false,
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Navigate to / in Router', () => {
    it('should render Homepage component at root path', () => {
      renderWithAuth(<Home />, {
        initialEntries: ['/'],
        user: null,
        isAuthenticated: false,
      })

      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should verify location display shows root path', () => {
      const { getCurrentLocation } = renderWithAuth(<Home />, {
        initialEntries: ['/'],
        user: null,
        isAuthenticated: false,
      })

      expect(getCurrentLocation()).toBe('/')
    })

    it('should render all homepage sections at root path', () => {
      renderWithAuth(<Home />, {
        initialEntries: ['/'],
        user: null,
        isAuthenticated: false,
      })

      // Hero section should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Features section should be present (via id)
      expect(document.getElementById('features')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Check Homepage component imports (Navbar)', () => {
    it('should render Navbar component when rendering with full App structure', () => {
      renderWithAuth(<Navbar />, {
        user: null,
        isAuthenticated: false,
      })

      expect(screen.getByTestId('navbar')).toBeInTheDocument()
    })

    it('should show Navbar with correct navigation links', () => {
      renderWithAuth(<Navbar />, {
        user: null,
        isAuthenticated: false,
      })

      // Check for Login and Sign Up links in navbar
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /sign up/i })).toBeInTheDocument()
    })

    it('should integrate Navbar with Homepage correctly', () => {
      renderWithAuth(
        <>
          <Navbar />
          <Home />
        </>,
        {
          user: null,
          isAuthenticated: false,
        }
      )

      // Both Navbar and Homepage should render
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should have Navbar that links back to home route', () => {
      renderWithAuth(<Navbar />, {
        user: null,
        isAuthenticated: false,
      })

      const homeLink = screen.getByRole('link', { name: /url shortener/i })
      expect(homeLink).toHaveAttribute('href', '/')
    })
  })

  describe('Test Case 5: Render Homepage and check for BackgroundEffect', () => {
    it('should render BackgroundEffect component for visual consistency', () => {
      renderWithAuth(
        <>
          <BackgroundEffect />
          <Home />
        </>,
        {
          user: null,
          isAuthenticated: false,
        }
      )

      // BackgroundEffect renders a fixed positioned div
      const backgroundElement = document.querySelector('.fixed.inset-0.-z-10')
      expect(backgroundElement).toBeInTheDocument()
    })

    it('should render BackgroundEffect with gradient background', () => {
      renderWithAuth(
        <>
          <BackgroundEffect />
          <Home />
        </>,
        {
          user: null,
          isAuthenticated: false,
        }
      )

      // Check for gradient background
      const gradientElement = document.querySelector('.bg-gradient-to-br')
      expect(gradientElement).toBeInTheDocument()
    })

    it('should render BackgroundEffect with animated pulse elements', () => {
      renderWithAuth(
        <>
          <BackgroundEffect />
          <Home />
        </>,
        {
          user: null,
          isAuthenticated: false,
        }
      )

      // Check for pulse animation elements
      const pulseElements = document.querySelectorAll('.animate-pulse')
      expect(pulseElements.length).toBeGreaterThan(0)
    })

    it('should maintain visual consistency when Homepage is rendered with BackgroundEffect', () => {
      renderWithAuth(
        <>
          <BackgroundEffect />
          <Navbar />
          <Home />
        </>,
        {
          user: null,
          isAuthenticated: false,
        }
      )

      // All components should render without conflicts
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(document.querySelector('.fixed.inset-0.-z-10')).toBeInTheDocument()
    })
  })
})
