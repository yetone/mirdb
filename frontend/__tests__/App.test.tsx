/**
 * App Component Route Integration Tests
 * Owner: Scenario 15 - Route Integration
 *
 * Tests for verifying React Router configuration in App.tsx
 * Ensures the '/' route renders the Homepage component correctly
 */
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from 'react-query'
import App from '../src/App'
import { ThemeProvider } from '../src/contexts/ThemeContext'
import { AuthProvider } from '../src/contexts/AuthContext'

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })

interface RenderAppOptions {
  initialRoute?: string
}

function renderApp({ initialRoute = '/' }: RenderAppOptions = {}) {
  const queryClient = createTestQueryClient()
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[initialRoute]}>
        <ThemeProvider>
          <AuthProvider>
            <App />
          </AuthProvider>
        </ThemeProvider>
      </MemoryRouter>
    </QueryClientProvider>
  )
}

describe('App - Route Integration (Scenario 15)', () => {
  // Test Case 1: Integration - Navigate to '/' route renders Homepage component
  it('should render Homepage component when navigating to "/" route', () => {
    renderApp({ initialRoute: '/' })

    // Verify Homepage component is rendered
    const mainElement = screen.getByRole('main')
    expect(mainElement).toBeInTheDocument()

    // Verify hero section headline (Homepage-specific)
    const heroHeading = screen.getByRole('heading', { level: 1 })
    expect(heroHeading).toBeInTheDocument()
    expect(heroHeading.textContent).toContain('Shorten URLs')
    expect(heroHeading.textContent).toContain('Track Clicks')
    expect(heroHeading.textContent).toContain('Grow Your Reach')
  })

  // Test Case 2: Unit - Verify route path='/' is configured in App.tsx
  it('should have route path="/" configured to render Home component', () => {
    renderApp({ initialRoute: '/' })

    // The App component with Routes should render Home at '/'
    // We verify this by checking for Homepage-specific content

    // Main element with homepage styling
    const mainElement = screen.getByRole('main')
    expect(mainElement).toHaveClass('min-h-screen')
    expect(mainElement).toHaveClass('bg-base-100')

    // Features section (unique to Homepage)
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Footer section (unique to Homepage)
    const footerSection = screen.getByTestId('footer-section')
    expect(footerSection).toBeInTheDocument()
  })

  // Test Case 3: Integration - No 404 or error page at '/' route
  it('should not display 404 or error page when navigating to "/" route', () => {
    renderApp({ initialRoute: '/' })

    // Ensure no error-related content is displayed
    expect(screen.queryByText(/404/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/not found/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/page not found/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/error/i)).not.toBeInTheDocument()

    // Verify actual homepage content is present
    expect(screen.getByText('URL Shortener')).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
  })

  // Additional test: App renders Routes component with correct structure
  it('should render App with Routes containing "/" path', () => {
    renderApp({ initialRoute: '/' })

    // App should contain the Routes configuration
    // Verified by successful rendering of Homepage at '/'
    const mainElement = screen.getByRole('main')
    expect(mainElement).toBeInTheDocument()

    // All homepage sections should render
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
    expect(screen.getByTestId('features-section')).toBeInTheDocument()
    expect(screen.getByTestId('footer-section')).toBeInTheDocument()
  })

  // Additional test: Homepage route is publicly accessible without auth
  it('should render Homepage at "/" without authentication requirements', () => {
    // Clear any auth state
    localStorage.removeItem('token')
    localStorage.removeItem('user')

    renderApp({ initialRoute: '/' })

    // Homepage should render without authentication blocking
    expect(screen.getByRole('main')).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 1 })).toBeVisible()

    // No loading states from auth checks
    expect(screen.queryByText(/loading/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/authenticating/i)).not.toBeInTheDocument()
  })

  // Additional test: CTA buttons have correct navigation targets
  it('should render CTA buttons with correct navigation targets at "/" route', () => {
    renderApp({ initialRoute: '/' })

    // Get Started link should point to /register
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toHaveAttribute('href', '/register')

    // Login links should point to /login
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    const heroLoginLink = loginLinks.find(link => link.querySelector('button'))
    expect(heroLoginLink).toHaveAttribute('href', '/login')
  })
})
