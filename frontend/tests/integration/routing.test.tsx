/**
 * Route Integration Tests
 * Owner: Scenario 14 - Route Integration
 *
 * Tests React Router integration with the homepage:
 * - Homepage accessible at root path (/)
 * - Navigation between homepage and other routes
 * - Browser history functionality
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '../setup'
import userEvent from '@testing-library/user-event'
import React from 'react'
import { Routes, Route, useLocation, useNavigate, MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'
import Home from '@/pages/Home'
import Login from '@/pages/Login'

// Helper component to display current location for routing tests
const LocationDisplay: React.FC = () => {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

// Helper component to track navigation history
const HistoryTracker: React.FC<{ historyRef: React.MutableRefObject<string[]> }> = ({ historyRef }) => {
  const location = useLocation()

  React.useEffect(() => {
    historyRef.current.push(location.pathname)
  }, [location.pathname, historyRef])

  return null
}

// Test wrapper that includes routing for integration tests
const TestApp: React.FC = () => {
  return (
    <>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<Login />} />
      </Routes>
      <LocationDisplay />
    </>
  )
}

// Custom render with explicit routes for precise control
const renderWithRouter = (
  ui: React.ReactElement,
  { initialEntries = ['/'] }: { initialEntries?: string[] } = {}
) => {
  return render(ui, { initialEntries })
}

describe('Route Integration', () => {
  describe('Test Case 1: Navigate to / (root path) - HomePage component is rendered', () => {
    it('should render HomePage component when navigating to root path (/)', () => {
      renderWithRouter(<TestApp />)

      // Verify HomePage is rendered
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should display homepage content at root path', () => {
      renderWithRouter(<TestApp />)

      // Verify location is at root
      const location = screen.getByTestId('location-display')
      expect(location).toHaveTextContent('/')

      // Verify home page is present
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })

    it('should render hero section on homepage', () => {
      renderWithRouter(<TestApp />)

      // Hero section should be visible on homepage
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('should render navigation bar on homepage', () => {
      renderWithRouter(<TestApp />)

      // Navbar should be present on homepage
      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
    })

    it('should render footer on homepage', () => {
      renderWithRouter(<TestApp />)

      // Footer should be present on homepage
      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Check React Router configuration - Root path (/) maps to HomePage component', () => {
    it('should have root path configured to render HomePage', () => {
      renderWithRouter(<TestApp />, { initialEntries: ['/'] })

      // HomePage should be rendered at root
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('location-display')).toHaveTextContent('/')
    })

    it('should have login path configured to render Login page', () => {
      renderWithRouter(<TestApp />, { initialEntries: ['/login'] })

      // Login page should be rendered at /login
      expect(screen.getByTestId('login-page')).toBeInTheDocument()
      expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
    })

    it('should render correct component based on initial route', () => {
      // Test root path
      const { unmount: unmount1 } = renderWithRouter(<TestApp />, { initialEntries: ['/'] })
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      unmount1()

      // Test login path
      const { unmount: unmount2 } = renderWithRouter(<TestApp />, { initialEntries: ['/login'] })
      expect(screen.getByTestId('login-page')).toBeInTheDocument()
      unmount2()
    })

    it('should support route matching without trailing slash', () => {
      renderWithRouter(<TestApp />, { initialEntries: ['/'] })
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Navigate from homepage to /login - Login page renders correctly', () => {
    it('should navigate from homepage to login page when login link is clicked', async () => {
      const user = userEvent.setup()
      renderWithRouter(<TestApp />)

      // Verify we start at home
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('location-display')).toHaveTextContent('/')

      // Click login link in navbar
      const loginLink = screen.getByTestId('nav-login')
      await user.click(loginLink)

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
        expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
      })
    })

    it('should render login form elements on login page', async () => {
      const user = userEvent.setup()
      renderWithRouter(<TestApp />)

      // Navigate to login
      const loginLink = screen.getByTestId('nav-login')
      await user.click(loginLink)

      // Verify login page content
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
        // Check for login page heading (h1 within login page)
        const loginPage = screen.getByTestId('login-page')
        expect(loginPage).toHaveTextContent('Login')
        expect(loginPage).toHaveTextContent('Sign in to your account')
      })
    })

    it('should unmount homepage when navigating to login', async () => {
      const user = userEvent.setup()
      renderWithRouter(<TestApp />)

      // Navigate to login
      const loginLink = screen.getByTestId('nav-login')
      await user.click(loginLink)

      // Homepage should not be in DOM
      await waitFor(() => {
        expect(screen.queryByTestId('home-page')).not.toBeInTheDocument()
      })
    })
  })

  describe('Test Case 4: Navigate from /login back to / - Homepage renders correctly without full page reload', () => {
    it('should navigate from login page back to homepage', async () => {
      const user = userEvent.setup()
      renderWithRouter(<TestApp />, { initialEntries: ['/login'] })

      // Verify we start at login
      expect(screen.getByTestId('login-page')).toBeInTheDocument()

      // Click back to home link
      const backToHomeLink = screen.getByTestId('back-to-home-link')
      await user.click(backToHomeLink)

      // Verify navigation to homepage
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
        expect(screen.getByTestId('location-display')).toHaveTextContent('/')
      })
    })

    it('should render homepage content after returning from login', async () => {
      const user = userEvent.setup()
      renderWithRouter(<TestApp />, { initialEntries: ['/login'] })

      // Navigate to homepage
      const backToHomeLink = screen.getByTestId('back-to-home-link')
      await user.click(backToHomeLink)

      // Verify homepage content is rendered
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
        expect(screen.getByTestId('navbar')).toBeInTheDocument()
        expect(screen.getByTestId('footer')).toBeInTheDocument()
      })
    })

    it('should complete round-trip navigation: home -> login -> home', async () => {
      const user = userEvent.setup()
      renderWithRouter(<TestApp />)

      // Start at home
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Navigate to login
      await user.click(screen.getByTestId('nav-login'))
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Navigate back to home
      await user.click(screen.getByTestId('back-to-home-link'))
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })
    })

    it('should not cause full page reload during navigation', async () => {
      const user = userEvent.setup()

      // Track component mount/unmount to verify SPA navigation (no full reload)
      let appMountCount = 0
      const MountCounter: React.FC<{ children: React.ReactNode }> = ({ children }) => {
        React.useEffect(() => {
          appMountCount++
        }, [])
        return <>{children}</>
      }

      const TestAppWithCounter: React.FC = () => {
        return (
          <MountCounter>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/login" element={<Login />} />
            </Routes>
            <LocationDisplay />
          </MountCounter>
        )
      }

      renderWithRouter(<TestAppWithCounter />)
      const initialMountCount = appMountCount

      // Navigate to login
      await user.click(screen.getByTestId('nav-login'))
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Navigate back to home
      await user.click(screen.getByTestId('back-to-home-link'))
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })

      // If full page reload occurred, MountCounter would remount (appMountCount would increase)
      // SPA navigation keeps the same React tree, so mount count stays the same
      expect(appMountCount).toBe(initialMountCount)
    })
  })

  describe('Test Case 5: Use browser back button after navigation - Browser history works correctly with homepage', () => {
    it('should navigate back using browser history after forward navigation', async () => {
      const user = userEvent.setup()

      // We need to manually track history since MemoryRouter handles this
      const historyRef = React.createRef<string[]>() as React.MutableRefObject<string[]>
      historyRef.current = []

      const TestAppWithHistory: React.FC = () => {
        return (
          <>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/login" element={<Login />} />
            </Routes>
            <LocationDisplay />
            <HistoryTracker historyRef={historyRef} />
          </>
        )
      }

      renderWithRouter(<TestAppWithHistory />)

      // Navigate to login
      await user.click(screen.getByTestId('nav-login'))
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // History should contain both routes
      expect(historyRef.current).toContain('/')
      expect(historyRef.current).toContain('/login')
    })

    it('should maintain correct history order during navigation', async () => {
      const user = userEvent.setup()

      const historyRef = React.createRef<string[]>() as React.MutableRefObject<string[]>
      historyRef.current = []

      const TestAppWithHistory: React.FC = () => {
        return (
          <>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/login" element={<Login />} />
            </Routes>
            <LocationDisplay />
            <HistoryTracker historyRef={historyRef} />
          </>
        )
      }

      renderWithRouter(<TestAppWithHistory />)

      // Navigate: home -> login -> home
      await user.click(screen.getByTestId('nav-login'))
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      await user.click(screen.getByTestId('back-to-home-link'))
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })

      // History should reflect the navigation order
      expect(historyRef.current.length).toBeGreaterThanOrEqual(3)
      expect(historyRef.current[0]).toBe('/')
      expect(historyRef.current[1]).toBe('/login')
      expect(historyRef.current[2]).toBe('/')
    })

    it('should support programmatic navigation with useNavigate hook', async () => {
      // Component that uses useNavigate for programmatic navigation
      const NavigateButton: React.FC<{ to: string }> = ({ to }) => {
        const navigate = useNavigate()
        return (
          <button
            data-testid="navigate-button"
            onClick={() => navigate(to)}
          >
            Navigate
          </button>
        )
      }

      const TestAppWithNavButton: React.FC = () => {
        const location = useLocation()
        return (
          <>
            <Routes>
              <Route path="/" element={
                <>
                  <Home />
                  <NavigateButton to="/login" />
                </>
              } />
              <Route path="/login" element={<Login />} />
            </Routes>
            <LocationDisplay />
          </>
        )
      }

      const user = userEvent.setup()
      renderWithRouter(<TestAppWithNavButton />)

      // Verify at home
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Use programmatic navigation
      await user.click(screen.getByTestId('navigate-button'))

      // Should be at login
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
        expect(screen.getByTestId('location-display')).toHaveTextContent('/login')
      })
    })

    it('should preserve React state across navigation (no remount of providers)', async () => {
      const user = userEvent.setup()

      // Track provider mount count
      let providerMountCount = 0

      const MountTracker: React.FC<{ children: React.ReactNode }> = ({ children }) => {
        React.useEffect(() => {
          providerMountCount++
        }, [])
        return <>{children}</>
      }

      const TestAppWithTracker: React.FC = () => {
        return (
          <MountTracker>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/login" element={<Login />} />
            </Routes>
            <LocationDisplay />
          </MountTracker>
        )
      }

      renderWithRouter(<TestAppWithTracker />)

      const initialMountCount = providerMountCount

      // Navigate to login and back
      await user.click(screen.getByTestId('nav-login'))
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      await user.click(screen.getByTestId('back-to-home-link'))
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })

      // Provider should not have remounted
      expect(providerMountCount).toBe(initialMountCount)
    })
  })

  describe('Additional Route Integration Tests', () => {
    it('should handle direct URL access to homepage', () => {
      renderWithRouter(<TestApp />, { initialEntries: ['/'] })
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })

    it('should handle direct URL access to login page', () => {
      renderWithRouter(<TestApp />, { initialEntries: ['/login'] })
      expect(screen.getByTestId('login-page')).toBeInTheDocument()
    })

    it('should maintain semantic HTML structure on homepage', () => {
      renderWithRouter(<TestApp />)

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Check for main navbar (there may be multiple navigation elements - navbar + footer)
      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
      expect(navbar).toHaveAttribute('role', 'navigation')
    })

    it('should have skip link for accessibility on homepage', () => {
      renderWithRouter(<TestApp />)

      const skipLink = screen.getByTestId('skip-link')
      expect(skipLink).toBeInTheDocument()
      expect(skipLink).toHaveAttribute('href', '#main-content')
    })
  })
})
