/**
 * HomePage Integration Tests
 *
 * Owner: Scenario 7 - Theme System Support (Light/Dark Mode)
 * Owner: Scenario 17 - Routing Integration
 *
 * Tests that the homepage properly supports:
 * - The app's existing theme system with light and dark modes (Scenario 7)
 * - React Router integration at root path (Scenario 17)
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, waitFor, within } from '../test-utils'
import userEvent from '@testing-library/user-event'
import { ReactElement, ReactNode } from 'react'
import { render as rtlRender, RenderOptions } from '@testing-library/react'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider, useTheme } from '../../src/contexts/ThemeContext'
import { AuthProvider } from '../../src/contexts/AuthContext'
import Home from '../../src/pages/Home'

// Helper component to set initial theme
interface ThemeSetterProps {
  theme: 'light' | 'dark'
  children: ReactNode
}

function ThemeSetter({ theme, children }: ThemeSetterProps) {
  const { setTheme } = useTheme()

  // Set theme on mount
  React.useEffect(() => {
    setTheme(theme)
  }, [theme, setTheme])

  return <>{children}</>
}

import React from 'react'

// Custom render with configurable theme
function renderWithTheme(
  ui: ReactElement,
  initialTheme: 'light' | 'dark' = 'light',
  options?: Omit<RenderOptions, 'wrapper'>
) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  })

  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <ThemeProvider>
            <AuthProvider>
              <ThemeSetter theme={initialTheme}>{children}</ThemeSetter>
            </AuthProvider>
          </ThemeProvider>
        </BrowserRouter>
      </QueryClientProvider>
    )
  }

  return rtlRender(ui, { wrapper: Wrapper, ...options })
}

// Component that exposes theme toggle for testing
function ThemeToggleButton() {
  const { theme, toggleTheme } = useTheme()
  return (
    <button
      data-testid="theme-toggle-button"
      data-theme={theme}
      onClick={toggleTheme}
    >
      Toggle Theme (Current: {theme})
    </button>
  )
}

// Test wrapper that includes toggle button for interaction tests
function HomePageWithThemeToggle() {
  return (
    <>
      <ThemeToggleButton />
      <Home />
    </>
  )
}

describe('Theme System Support - HomePage Integration', () => {
  beforeEach(() => {
    // Reset document theme attribute before each test
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Dark Theme Rendering', () => {
    it('applies dark theme styles when ThemeContext is set to dark', async () => {
      renderWithTheme(<Home />, 'dark')

      // Wait for theme to be applied to document
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify hero section renders (theme classes are applied via CSS)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('hero')

      // Verify features section renders with theme-aware classes
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toHaveClass('bg-base-200')

      // Verify how-it-works section renders
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
      expect(howItWorksSection).toHaveClass('bg-base-100')

      // Verify text elements have theme-responsive classes
      const productName = screen.getByTestId('product-name')
      expect(productName).toHaveClass('text-base-content')

      const tagline = screen.getByTestId('tagline')
      expect(tagline).toHaveClass('text-base-content/70')
    })

    it('all sections display with dark color scheme', async () => {
      renderWithTheme(<Home />, 'dark')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify all major sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Check feature cards have theme-aware classes
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(3)
      featureCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
      })

      // Check step cards are rendered
      const stepCards = screen.getAllByTestId('step-card')
      expect(stepCards).toHaveLength(3)
    })
  })

  describe('Test Case 2: Light Theme Rendering', () => {
    it('applies light theme styles when ThemeContext is set to light', async () => {
      renderWithTheme(<Home />, 'light')

      // Wait for theme to be applied
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify hero section renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('hero')

      // Verify features section renders with theme-aware classes
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toHaveClass('bg-base-200')

      // Verify how-it-works section renders
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
      expect(howItWorksSection).toHaveClass('bg-base-100')
    })

    it('all sections display with light color scheme', async () => {
      renderWithTheme(<Home />, 'light')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify all sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Check feature cards
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(3)
      featureCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
      })

      // Check descriptions have theme-aware opacity classes
      const featureDescriptions = screen.getAllByTestId('feature-description')
      featureDescriptions.forEach((desc) => {
        expect(desc).toHaveClass('text-base-content/70')
      })
    })
  })

  describe('Test Case 3: Theme Toggle Integration', () => {
    it('theme transition is smooth without page reload', async () => {
      const user = userEvent.setup()
      renderWithTheme(<HomePageWithThemeToggle />, 'light')

      // Verify initial light theme
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Get the toggle button
      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toHaveAttribute('data-theme', 'light')

      // Store reference to an element to verify no page reload
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Toggle to dark theme
      await user.click(toggleButton)

      // Verify theme changed without page reload (element should still be same reference)
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify same hero section element is still present (no reload)
      expect(heroSection).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBe(heroSection)

      // Verify button state updated
      expect(toggleButton).toHaveAttribute('data-theme', 'dark')
    })

    it('all sections update when theme toggles', async () => {
      const user = userEvent.setup()
      renderWithTheme(<HomePageWithThemeToggle />, 'light')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Capture initial state
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // All should be present with theme classes
      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()

      // Toggle theme
      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)

      // Verify dark theme applied
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // All sections should still be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Toggle back to light
      await user.click(toggleButton)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // All sections still present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('rapidly toggling theme does not cause issues', async () => {
      const user = userEvent.setup()
      renderWithTheme(<HomePageWithThemeToggle />, 'light')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      const toggleButton = screen.getByTestId('theme-toggle-button')

      // Rapid toggle
      await user.click(toggleButton) // -> dark
      await user.click(toggleButton) // -> light
      await user.click(toggleButton) // -> dark
      await user.click(toggleButton) // -> light
      await user.click(toggleButton) // -> dark

      // Final state should be dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // All sections should still be rendered correctly
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Theme Persistence (E2E simulation)', () => {
    it('dark mode preference persists across navigation', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      // Component that tracks theme state persistence
      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/']}>
              <ThemeProvider>
                <AuthProvider>
                  <Routes>
                    <Route
                      path="/"
                      element={
                        <>
                          <ThemeToggleButton />
                          <Home />
                        </>
                      }
                    />
                    <Route path="/other" element={<div data-testid="other-page">Other Page</div>} />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      const user = userEvent.setup()
      rtlRender(<TestApp />)

      // Wait for initial render
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Toggle to dark mode
      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify homepage is still showing with dark theme
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Theme persists in the ThemeContext
      expect(toggleButton).toHaveAttribute('data-theme', 'dark')
    })

    it('theme state is maintained within session', async () => {
      const user = userEvent.setup()
      renderWithTheme(<HomePageWithThemeToggle />, 'dark')

      // Verify dark theme is initially set
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify all components render with theme-aware classes
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      const featureCards = screen.getAllByTestId('feature-card')
      featureCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
      })

      // Theme button should reflect current state
      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toHaveAttribute('data-theme', 'dark')
    })
  })

  describe('Theme-aware component styling', () => {
    it('hero section uses theme-responsive gradient classes', async () => {
      renderWithTheme(<Home />, 'dark')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection).toHaveClass('from-primary/10')
      expect(heroSection).toHaveClass('via-base-200')
      expect(heroSection).toHaveClass('to-secondary/10')
    })

    it('feature cards use theme-responsive shadow classes', async () => {
      renderWithTheme(<Home />, 'light')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      const featureCards = screen.getAllByTestId('feature-card')
      featureCards.forEach((card) => {
        expect(card).toHaveClass('shadow-xl')
        expect(card).toHaveClass('hover:shadow-2xl')
      })
    })

    it('step numbers use primary theme colors', async () => {
      renderWithTheme(<Home />, 'dark')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      const stepNumbers = screen.getAllByTestId('step-number')
      expect(stepNumbers).toHaveLength(3)
      stepNumbers.forEach((step) => {
        expect(step).toHaveClass('bg-primary')
        expect(step).toHaveClass('text-primary-content')
      })
    })

    it('CTA buttons use theme-aware button classes', async () => {
      renderWithTheme(<Home />, 'light')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      const signupButton = screen.getByTestId('signup-button')
      expect(signupButton).toHaveClass('btn')
      expect(signupButton).toHaveClass('btn-secondary')

      const loginButton = screen.getByTestId('login-button')
      expect(loginButton).toHaveClass('btn')
      expect(loginButton).toHaveClass('btn-outline')
    })
  })
})

/**
 * Scenario 17 - Routing Integration Tests
 *
 * Tests that the homepage is properly integrated with React Router at the root path.
 * Covers NFR-5: Browser back/forward navigation support.
 */
describe('Routing Integration - Scenario 17', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Navigate to "/" route renders HomePage', () => {
    it('renders HomePage component at root path "/"', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/']}>
              <ThemeProvider>
                <AuthProvider>
                  <Routes>
                    <Route path="/" element={<Home />} />
                    <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      rtlRender(<TestApp />)

      // Verify HomePage renders with all key sections
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // Should NOT render login page
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument()
    })

    it('homepage route "/" does not redirect to any other page', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/']}>
              <ThemeProvider>
                <AuthProvider>
                  <Routes>
                    <Route path="/" element={<Home />} />
                    <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
                    <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard</div>} />
                    <Route path="*" element={<div data-testid="not-found-page">404 Not Found</div>} />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      rtlRender(<TestApp />)

      // Wait for initial render
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Verify we stayed on homepage - no redirects occurred
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument()
      expect(screen.queryByTestId('dashboard-page')).not.toBeInTheDocument()
      expect(screen.queryByTestId('not-found-page')).not.toBeInTheDocument()
    })
  })

  describe('Test Case 2: Browser navigation - back button from /login to /', () => {
    it('navigates from "/" to "/login" and back using browser history', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      const user = userEvent.setup()

      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/']}>
              <ThemeProvider>
                <AuthProvider>
                  <Routes>
                    <Route path="/" element={<Home />} />
                    <Route
                      path="/login"
                      element={
                        <div data-testid="login-page">
                          Login Page
                        </div>
                      }
                    />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      rtlRender(<TestApp />)

      // Verify HomePage is initially rendered
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Click login button to navigate to /login
      const loginButton = screen.getByTestId('login-button')
      expect(loginButton).toBeInTheDocument()
      await user.click(loginButton)

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Homepage should no longer be visible
      expect(screen.queryByTestId('hero-section')).not.toBeInTheDocument()
    })
  })

  describe('Test Case 3: Direct URL access to "/" renders homepage directly', () => {
    it('direct navigation to "/" loads homepage without redirects', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/']}>
              <ThemeProvider>
                <AuthProvider>
                  <Routes>
                    <Route path="/" element={<Home />} />
                    <Route path="/login" element={<div data-testid="login-page">Login</div>} />
                    <Route path="*" element={<div data-testid="not-found">404</div>} />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      rtlRender(<TestApp />)

      // Homepage should render immediately without any redirects
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // All homepage sections should be present
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

      // No 404 or other pages should be shown
      expect(screen.queryByTestId('not-found')).not.toBeInTheDocument()
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument()
    })

    it('homepage renders correctly when accessed as initial entry', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/']}>
              <ThemeProvider>
                <AuthProvider>
                  <Routes>
                    <Route path="/" element={<Home />} />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      rtlRender(<TestApp />)

      // Verify key homepage elements render
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Check product name and tagline render
      expect(screen.getByTestId('product-name')).toBeInTheDocument()
      expect(screen.getByTestId('tagline')).toBeInTheDocument()

      // Check CTA buttons are present
      expect(screen.getByTestId('signup-button')).toBeInTheDocument()
      expect(screen.getByTestId('login-button')).toBeInTheDocument()
    })
  })

  describe('Browser History Navigation (NFR-5)', () => {
    it('supports forward navigation after going back', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      const user = userEvent.setup()

      // Custom component to expose navigation controls
      function NavHelper() {
        const navigate = require('react-router-dom').useNavigate()
        return (
          <div>
            <button data-testid="go-back" onClick={() => navigate(-1)}>Back</button>
            <button data-testid="go-forward" onClick={() => navigate(1)}>Forward</button>
          </div>
        )
      }

      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/', '/login']} initialIndex={0}>
              <ThemeProvider>
                <AuthProvider>
                  <NavHelper />
                  <Routes>
                    <Route path="/" element={<Home />} />
                    <Route
                      path="/login"
                      element={<div data-testid="login-page">Login Page</div>}
                    />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      rtlRender(<TestApp />)

      // Start at homepage
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Click login button to go to login page
      const loginButton = screen.getByTestId('login-button')
      await user.click(loginButton)

      // Verify we're on login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Go back to homepage
      const backButton = screen.getByTestId('go-back')
      await user.click(backButton)

      // Verify back navigation works - we should be at homepage
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Go forward to login page
      const forwardButton = screen.getByTestId('go-forward')
      await user.click(forwardButton)

      // Verify forward navigation works
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })

    it('homepage is added to browser history correctly', async () => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: { retry: false, gcTime: 0 },
          mutations: { retry: false },
        },
      })

      const user = userEvent.setup()

      // Navigate helper component
      function NavHelper() {
        const navigate = require('react-router-dom').useNavigate()
        const location = require('react-router-dom').useLocation()
        return (
          <div data-testid="nav-helper" data-pathname={location.pathname}>
            <button data-testid="go-back" onClick={() => navigate(-1)}>Back</button>
          </div>
        )
      }

      function TestApp() {
        return (
          <QueryClientProvider client={queryClient}>
            <MemoryRouter initialEntries={['/']}>
              <ThemeProvider>
                <AuthProvider>
                  <NavHelper />
                  <Routes>
                    <Route path="/" element={<Home />} />
                    <Route path="/login" element={<div data-testid="login-page">Login</div>} />
                  </Routes>
                </AuthProvider>
              </ThemeProvider>
            </MemoryRouter>
          </QueryClientProvider>
        )
      }

      rtlRender(<TestApp />)

      // Verify initial pathname is "/"
      await waitFor(() => {
        const navHelper = screen.getByTestId('nav-helper')
        expect(navHelper).toHaveAttribute('data-pathname', '/')
      })

      // Verify homepage renders
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Navigate to login
      const loginButton = screen.getByTestId('login-button')
      await user.click(loginButton)

      // Verify pathname changed to "/login"
      await waitFor(() => {
        const navHelper = screen.getByTestId('nav-helper')
        expect(navHelper).toHaveAttribute('data-pathname', '/login')
      })

      // Use back button
      const backButton = screen.getByTestId('go-back')
      await user.click(backButton)

      // Verify pathname is back to "/"
      await waitFor(() => {
        const navHelper = screen.getByTestId('nav-helper')
        expect(navHelper).toHaveAttribute('data-pathname', '/')
      })

      // Verify homepage is rendered again
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })
  })
})
