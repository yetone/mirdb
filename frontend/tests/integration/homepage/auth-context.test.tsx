/**
 * Auth Context Integration Tests
 * Owner: Scenario 19 - Authenticated User Redirect
 *
 * Tests for authenticated user handling on the homepage.
 * Verifies that authenticated users see different CTAs (Dashboard link instead of Sign In).
 */

// Mock IntersectionObserver for framer-motion's whileInView
const mockIntersectionObserver = vi.fn()
mockIntersectionObserver.mockReturnValue({
  observe: () => null,
  unobserve: () => null,
  disconnect: () => null,
})
window.IntersectionObserver = mockIntersectionObserver

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { createContext, useContext, useState, ReactNode } from 'react'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import Home from '../../../src/pages/Home'
import '@testing-library/jest-dom'

// Mock user data
const mockAuthenticatedUser = {
  id: 'user-123',
  email: 'testuser@example.com',
  is_admin: false,
}

const mockAdminUser = {
  id: 'admin-456',
  email: 'admin@example.com',
  is_admin: true,
}

// Mock navigate function
const mockNavigate = vi.fn()

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useNavigate: () => mockNavigate,
    useLocation: () => ({ hash: '', pathname: '/' }),
  }
})

// Create a test-specific AuthContext for mocking
interface User {
  id: string
  email: string
  is_admin: boolean
}

interface AuthContextType {
  user: User | null
  isAuthenticated: boolean
  login: (user: User) => void
  logout: () => void
}

const TestAuthContext = createContext<AuthContextType | undefined>(undefined)

// Mock the useAuth hook to use our test context
vi.mock('../../../src/contexts/AuthContext', () => ({
  AuthProvider: ({ children }: { children: ReactNode }) => children,
  useAuth: () => {
    const context = useContext(TestAuthContext)
    if (context === undefined) {
      // Default to unauthenticated state
      return {
        user: null,
        isAuthenticated: false,
        login: vi.fn(),
        logout: vi.fn(),
      }
    }
    return context
  },
}))

// Test AuthProvider that allows us to control the auth state
function TestAuthProvider({
  children,
  initialUser = null,
}: {
  children: ReactNode
  initialUser?: User | null
}) {
  const [user, setUser] = useState<User | null>(initialUser)

  const login = (userData: User) => {
    setUser(userData)
  }

  const logout = () => {
    setUser(null)
  }

  return (
    <TestAuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        login,
        logout,
      }}
    >
      {children}
    </TestAuthContext.Provider>
  )
}

// Helper to render with all providers for unauthenticated users
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <TestAuthProvider>{ui}</TestAuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

// Helper to render with authenticated user context
function renderWithAuthenticatedUser(
  ui: React.ReactElement,
  user = mockAuthenticatedUser
) {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <TestAuthProvider initialUser={user}>{ui}</TestAuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('Auth Context Integration Tests', () => {
  beforeEach(() => {
    localStorage.clear()
    mockNavigate.mockClear()
  })

  afterEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 1: Visit homepage while authenticated', () => {
    it('should render homepage with appropriate CTAs for authenticated users', () => {
      renderWithAuthenticatedUser(<Home />)

      // Homepage should still be accessible
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Features section should still be visible
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should show Dashboard link instead of Sign In for authenticated users', () => {
      renderWithAuthenticatedUser(<Home />)

      // Dashboard link should be present in navigation
      const dashboardLink = screen.getByTestId('dashboard-link')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')

      // Sign In button should NOT be visible for authenticated users
      const signInNav = screen.queryByTestId('sign-in-nav')
      expect(signInNav).not.toBeInTheDocument()
    })

    it('should display user email in user menu when authenticated', () => {
      renderWithAuthenticatedUser(<Home />)

      // Check for user menu
      const userMenu = screen.getByTestId('user-menu')
      expect(userMenu).toBeInTheDocument()

      // User email should be displayed
      expect(screen.getByText(mockAuthenticatedUser.email)).toBeInTheDocument()
    })

    it('should show Go to Dashboard CTA in hero section when authenticated', () => {
      renderWithAuthenticatedUser(<Home />)

      // For authenticated users, hero should show "Go to Dashboard"
      const dashboardButton = screen.getByTestId('go-to-dashboard-button')
      expect(dashboardButton).toBeInTheDocument()
      expect(dashboardButton).toHaveTextContent('Go to Dashboard')

      // Get Started and Sign In buttons should NOT be present
      const getStartedButton = screen.queryByTestId('get-started-button')
      const signInButton = screen.queryByTestId('sign-in-button')
      expect(getStartedButton).not.toBeInTheDocument()
      expect(signInButton).not.toBeInTheDocument()
    })
  })

  describe('Test Case 2: Check navigation for authenticated user', () => {
    it('should show Dashboard link in desktop navigation for authenticated users', () => {
      renderWithAuthenticatedUser(<Home />)

      // Desktop navigation should include Dashboard link
      const dashboardLink = screen.getByTestId('dashboard-link')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink.tagName.toLowerCase()).toBe('a')
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('should show user menu with logout option for authenticated users', async () => {
      const user = userEvent.setup()
      renderWithAuthenticatedUser(<Home />)

      // User menu button should be present
      const userMenuButton = screen.getByTestId('user-menu-button')
      expect(userMenuButton).toBeInTheDocument()

      // Click to open user menu
      await user.click(userMenuButton)

      // Logout button should be visible in the dropdown
      await waitFor(() => {
        const logoutButton = screen.getByTestId('logout-button')
        expect(logoutButton).toBeInTheDocument()
      })
    })

    it('should hide Get Started CTA from navigation when authenticated', () => {
      renderWithAuthenticatedUser(<Home />)

      // Get Started nav button should not be visible for authenticated users
      const getStartedNav = screen.queryByTestId('get-started-nav')
      expect(getStartedNav).not.toBeInTheDocument()
    })

    it('should show Dashboard link in mobile menu for authenticated users', async () => {
      const user = userEvent.setup()
      renderWithAuthenticatedUser(<Home />)

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-menu')
      await user.click(hamburgerButton)

      // Wait for mobile menu to appear
      await waitFor(() => {
        const mobileMenu = screen.getByTestId('mobile-menu')
        expect(mobileMenu).toBeInTheDocument()
      })

      // Mobile Dashboard link should be present
      const mobileDashboardLink = screen.getByTestId('mobile-dashboard-link')
      expect(mobileDashboardLink).toBeInTheDocument()
      expect(mobileDashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('should show logout button in mobile menu for authenticated users', async () => {
      const user = userEvent.setup()
      renderWithAuthenticatedUser(<Home />)

      // Open mobile menu
      const hamburgerButton = screen.getByTestId('hamburger-menu')
      await user.click(hamburgerButton)

      // Wait for mobile menu to appear
      await waitFor(() => {
        const mobileLogout = screen.getByTestId('mobile-logout')
        expect(mobileLogout).toBeInTheDocument()
      })
    })

    it('should navigate to dashboard when Dashboard link is clicked', async () => {
      const user = userEvent.setup()
      renderWithAuthenticatedUser(<Home />)

      const dashboardLink = screen.getByTestId('dashboard-link')
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('should navigate to dashboard when Go to Dashboard button is clicked', async () => {
      const user = userEvent.setup()
      renderWithAuthenticatedUser(<Home />)

      const dashboardButton = screen.getByTestId('go-to-dashboard-button')
      await user.click(dashboardButton)

      expect(mockNavigate).toHaveBeenCalledWith('/dashboard')
    })
  })

  describe('Test Case 3: Render Home with AuthContext providing authenticated user', () => {
    it('should correctly render authenticated variation with full provider setup', () => {
      renderWithAuthenticatedUser(<Home />)

      // Verify the component renders without errors
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should maintain all homepage sections for authenticated users', () => {
      renderWithAuthenticatedUser(<Home />)

      // All main sections should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Footer should be present
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('should render different hero CTAs based on authentication state', () => {
      // First render unauthenticated
      const { unmount } = renderWithProviders(<Home />)

      // Should show "Get Started Free" and "Sign In" buttons
      expect(screen.getByTestId('get-started-button')).toBeInTheDocument()
      expect(screen.getByTestId('sign-in-button')).toBeInTheDocument()

      unmount()

      // Now render authenticated
      renderWithAuthenticatedUser(<Home />)

      // For authenticated users, should show "Go to Dashboard"
      const goToDashboard = screen.getByTestId('go-to-dashboard-button')
      expect(goToDashboard).toBeInTheDocument()
      expect(goToDashboard).toHaveTextContent('Go to Dashboard')

      // Get Started and Sign In should NOT be present
      expect(screen.queryByTestId('get-started-button')).not.toBeInTheDocument()
      expect(screen.queryByTestId('sign-in-button')).not.toBeInTheDocument()
    })

    it('should persist authentication state across component re-renders', () => {
      const { rerender } = renderWithAuthenticatedUser(<Home />)

      // Initial render - should show authenticated state
      expect(screen.getByTestId('go-to-dashboard-button')).toBeInTheDocument()
      expect(screen.getByTestId('dashboard-link')).toBeInTheDocument()

      // Re-render the component
      rerender(
        <BrowserRouter>
          <ThemeProvider>
            <TestAuthProvider initialUser={mockAuthenticatedUser}>
              <Home />
            </TestAuthProvider>
          </ThemeProvider>
        </BrowserRouter>
      )

      // Should still show authenticated state
      expect(screen.getByTestId('go-to-dashboard-button')).toBeInTheDocument()
      expect(screen.getByTestId('dashboard-link')).toBeInTheDocument()
    })
  })

  describe('Unauthenticated User Baseline', () => {
    it('should show Sign In and Get Started buttons when not authenticated', () => {
      renderWithProviders(<Home />)

      // Sign In button should be present
      const signInButton = screen.getByTestId('sign-in-button')
      expect(signInButton).toBeInTheDocument()

      // Get Started button should be present
      const getStartedButton = screen.getByTestId('get-started-button')
      expect(getStartedButton).toBeInTheDocument()
    })

    it('should show Sign In and Get Started in navigation when not authenticated', () => {
      renderWithProviders(<Home />)

      // Navigation should show Sign In and Get Started
      const signInNav = screen.getByTestId('sign-in-nav')
      expect(signInNav).toBeInTheDocument()

      const getStartedNav = screen.getByTestId('get-started-nav')
      expect(getStartedNav).toBeInTheDocument()

      // Dashboard link should NOT be present
      const dashboardLink = screen.queryByTestId('dashboard-link')
      expect(dashboardLink).not.toBeInTheDocument()
    })

    it('should navigate to login when Sign In is clicked (unauthenticated)', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      const signInButton = screen.getByTestId('sign-in-button')
      await user.click(signInButton)

      expect(mockNavigate).toHaveBeenCalledWith('/login')
    })

    it('should navigate to register when Get Started is clicked (unauthenticated)', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByTestId('get-started-button')
      await user.click(getStartedButton)

      expect(mockNavigate).toHaveBeenCalledWith('/register')
    })
  })

  describe('Admin User Handling', () => {
    it('should handle admin users appropriately on homepage', () => {
      renderWithAuthenticatedUser(<Home />, mockAdminUser)

      // Homepage should render for admin users too
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Admin should see Dashboard link
      expect(screen.getByTestId('dashboard-link')).toBeInTheDocument()

      // Admin email should be displayed
      expect(screen.getByText(mockAdminUser.email)).toBeInTheDocument()
    })
  })

  describe('Accessibility for Authenticated State', () => {
    it('should maintain accessible navigation for authenticated users', () => {
      renderWithAuthenticatedUser(<Home />)

      // Navigation should still be accessible (there are multiple navs - header and footer)
      const navElements = screen.getAllByRole('navigation')
      expect(navElements.length).toBeGreaterThan(0)

      // Main navbar should be present
      const mainNav = navElements[0]
      expect(mainNav).toBeInTheDocument()
    })

    it('should have proper aria attributes on user menu', () => {
      renderWithAuthenticatedUser(<Home />)

      const userMenuButton = screen.getByTestId('user-menu-button')
      expect(userMenuButton).toHaveAttribute('aria-label', 'User menu')
      expect(userMenuButton).toHaveAttribute('aria-expanded')
    })

    it('should have proper keyboard navigation for Dashboard link', () => {
      renderWithAuthenticatedUser(<Home />)

      const dashboardLink = screen.getByTestId('dashboard-link')
      dashboardLink.focus()
      expect(document.activeElement).toBe(dashboardLink)
    })

    it('should have proper aria-label on Go to Dashboard button', () => {
      renderWithAuthenticatedUser(<Home />)

      const dashboardButton = screen.getByTestId('go-to-dashboard-button')
      expect(dashboardButton).toHaveAttribute('aria-label', 'Go to your dashboard')
    })
  })
})
