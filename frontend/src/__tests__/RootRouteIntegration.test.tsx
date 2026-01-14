import { describe, it, expect } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import App from '../App'
import Home from '../pages/Home'
import { ThemeProvider } from '../contexts/ThemeContext'

/**
 * Root Route Integration Tests
 *
 * Scenario: Routing Integration - Root Route
 * Description: Verify homepage is correctly routed at / path
 *
 * Test Cases:
 * 1. Navigate to / route in React Router - Home page component is rendered
 * 2. Check route configuration in App.tsx - Root route (/) maps to Home component
 * 3. Navigate to / route when not authenticated - Homepage renders without redirect (public route)
 */

// Helper to render with router context
const renderWithRouter = (
  ui: React.ReactElement,
  { initialEntries = ['/'] } = {}
) => {
  return render(
    <ThemeProvider>
      <MemoryRouter initialEntries={initialEntries}>{ui}</MemoryRouter>
    </ThemeProvider>
  )
}

describe('Routing Integration - Root Route', () => {
  describe('Test Case 1: Navigate to / route - Home page component is rendered', () => {
    it('renders Home page component when navigating to / route', () => {
      renderWithRouter(<App />)

      // Verify the Home page's HeroSection is rendered
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('renders the main headline from Home page at root route', () => {
      renderWithRouter(<App />)

      // Verify the h1 headline is present
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline.textContent?.toLowerCase()).toContain('url')
    })

    it('renders all Home page sections at root route', () => {
      renderWithRouter(<App />)

      // Verify HeroSection
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify FeaturesSection
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Verify DemoSection
      const demoSection = screen.getByTestId('demo-section')
      expect(demoSection).toBeInTheDocument()

      // Verify FooterSection
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toBeInTheDocument()
    })

    it('renders main content area at root route', () => {
      renderWithRouter(<App />)

      // Verify main element exists
      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()
      expect(mainElement).toHaveClass('min-h-screen')
    })
  })

  describe('Test Case 2: Route configuration - Root route (/) maps to Home component', () => {
    it('App component renders Routes with / path mapping to Home', () => {
      renderWithRouter(<App />)

      // The presence of Home page specific elements confirms the route mapping
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify CTA buttons from Home/HeroSection are present
      const getStartedButton = screen.getByTestId('cta-get-started')
      expect(getStartedButton).toBeInTheDocument()

      const loginButton = screen.getByTestId('cta-login')
      expect(loginButton).toBeInTheDocument()
    })

    it('root route renders the correct Home page structure', () => {
      renderWithRouter(<App />)

      // Home page should have main + footer structure
      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()

      // Footer should be outside main
      const footer = screen.getByTestId('footer-section')
      expect(footer).toBeInTheDocument()
    })

    it('route at / uses the Home component from pages directory', () => {
      // Direct Home component render for comparison
      const { container: homeContainer } = renderWithRouter(<Home />)
      const homeContent = homeContainer.innerHTML

      // App route render
      const { container: appContainer } = renderWithRouter(<App />)
      const appContent = appContainer.innerHTML

      // Both should render the same content structure
      expect(appContent).toContain('hero-section')
      expect(homeContent).toContain('hero-section')
    })
  })

  describe('Test Case 3: Navigate to / when not authenticated - Homepage renders without redirect', () => {
    it('renders homepage at root route without authentication requirement', () => {
      renderWithRouter(<App />)

      // The page should render immediately without any redirect
      // No login page or redirect should occur
      expect(screen.queryByText(/login page/i)).not.toBeInTheDocument()
      expect(screen.queryByText(/redirecting/i)).not.toBeInTheDocument()

      // Home content should be visible
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('homepage is a public route - no authentication check blocks rendering', async () => {
      renderWithRouter(<App />)

      // Wait for any async operations
      await waitFor(() => {
        // Home page should be visible without waiting for auth
        const headline = screen.getByRole('heading', { level: 1 })
        expect(headline).toBeInTheDocument()
      })

      // Verify no redirect occurred
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('root route does not redirect to /login', () => {
      render(
        <ThemeProvider>
          <MemoryRouter initialEntries={['/']}>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route
                path="/login"
                element={<div data-testid="login-redirect">Login Page</div>}
              />
            </Routes>
          </MemoryRouter>
        </ThemeProvider>
      )

      // Verify Home is rendered, not login
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.queryByTestId('login-redirect')).not.toBeInTheDocument()
    })

    it('root route does not redirect to /dashboard', () => {
      render(
        <ThemeProvider>
          <MemoryRouter initialEntries={['/']}>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route
                path="/dashboard"
                element={
                  <div data-testid="dashboard-redirect">Dashboard Page</div>
                }
              />
            </Routes>
          </MemoryRouter>
        </ThemeProvider>
      )

      // Verify Home is rendered, not dashboard
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.queryByTestId('dashboard-redirect')).not.toBeInTheDocument()
    })

    it('unauthenticated user can access homepage content', () => {
      // Render without any auth context - simulating unauthenticated state
      renderWithRouter(<App />)

      // All home page sections should be accessible
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // CTA buttons should be present for unauthenticated users
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()
    })
  })
})
