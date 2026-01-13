import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../../contexts/ThemeContext'
import { AppRoutes } from '../../App'

// Setup and teardown for animation tests
beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
})

afterEach(() => {
  vi.useRealTimers()
})

/**
 * Helper to render AppRoutes with ThemeProvider and MemoryRouter
 * @param initialEntries - Array of initial route paths
 */
function renderWithRouter(initialEntries: string[] = ['/']) {
  return render(
    <ThemeProvider>
      <MemoryRouter initialEntries={initialEntries}>
        <AppRoutes />
      </MemoryRouter>
    </ThemeProvider>
  )
}

/**
 * React Router Integration Tests
 * Scenario: Validate homepage integrates correctly with existing React Router at '/' route
 */
describe('React Router Integration', () => {
  /**
   * Test Case 1: Navigate to '/' route
   * Input: Navigate to '/' route
   * Expected: Homepage component is rendered
   */
  describe('Test Case 1: Navigate to "/" route renders homepage', () => {
    it('should render homepage component when navigating to root route', () => {
      renderWithRouter(['/'])

      // Verify homepage is rendered
      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()
    })

    it('should display the hero section on homepage', () => {
      renderWithRouter(['/'])

      // Verify hero section is present
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('should display the hero headline text', () => {
      renderWithRouter(['/'])

      // Verify headline content
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten. Track. Share.')
    })

    it('should display navigation elements on homepage', () => {
      renderWithRouter(['/'])

      // Verify login and register links are present
      const loginLink = screen.getByTestId('login-link')
      const registerLink = screen.getByTestId('register-link')

      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()
    })

    it('should display demo section for URL shortening', () => {
      renderWithRouter(['/'])

      // Verify demo section is present
      const demoSection = screen.getByTestId('demo-section')
      expect(demoSection).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Navigate from homepage to '/login' and back to '/'
   * Input: Navigate from homepage to '/login' and back to '/'
   * Expected: Navigation works correctly in both directions
   */
  describe('Test Case 2: Navigation from homepage to "/login" and back', () => {
    it('should navigate from homepage to login page when clicking login link', () => {
      renderWithRouter(['/'])

      // Click login link
      const loginLink = screen.getByTestId('login-link')
      fireEvent.click(loginLink)

      // Verify we are on login page
      const loginPage = screen.getByTestId('login-page')
      expect(loginPage).toBeInTheDocument()
    })

    it('should navigate from login page back to homepage', () => {
      renderWithRouter(['/login'])

      // Verify we start on login page
      expect(screen.getByTestId('login-page')).toBeInTheDocument()

      // Click back to home link
      const backLink = screen.getByTestId('back-to-home-link')
      fireEvent.click(backLink)

      // Verify we are back on homepage
      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()
    })

    it('should complete round trip navigation: homepage -> login -> homepage', () => {
      renderWithRouter(['/'])

      // Step 1: Verify we start on homepage
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      // Step 2: Navigate to login
      const loginLink = screen.getByTestId('login-link')
      fireEvent.click(loginLink)
      expect(screen.getByTestId('login-page')).toBeInTheDocument()

      // Step 3: Navigate back to homepage
      const backLink = screen.getByTestId('back-to-home-link')
      fireEvent.click(backLink)
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should preserve navigation history during round trip', async () => {
      renderWithRouter(['/'])

      // Navigate to login
      fireEvent.click(screen.getByTestId('login-link'))
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Navigate back to homepage
      fireEvent.click(screen.getByTestId('back-to-home-link'))
      await waitFor(() => {
        expect(screen.getByTestId('homepage')).toBeInTheDocument()
      })
    })
  })

  /**
   * Test Case 3: Direct URL access to '/'
   * Input: Direct URL access to '/'
   * Expected: Homepage loads correctly when accessing root URL directly
   */
  describe('Test Case 3: Direct URL access to "/"', () => {
    it('should load homepage correctly when accessing root URL directly', () => {
      renderWithRouter(['/'])

      // Verify homepage loads
      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()
    })

    it('should render all major homepage sections on direct access', () => {
      renderWithRouter(['/'])

      // Verify all major sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should display correct page content on direct access', () => {
      renderWithRouter(['/'])

      // Verify headline
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent('Shorten. Track. Share.')

      // Verify subheadline
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveTextContent(/Transform long URLs/)
    })

    it('should load homepage faster than 1 second on direct access', () => {
      const startTime = performance.now()

      renderWithRouter(['/'])

      // Verify homepage is rendered
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      const endTime = performance.now()
      const loadTime = endTime - startTime

      // Page should load in less than 1 second
      expect(loadTime).toBeLessThan(1000)
    })
  })

  /**
   * Test Case 4: Check homepage is accessible to unauthenticated users
   * Input: Check homepage is accessible to unauthenticated users
   * Expected: Homepage is a public route, no authentication required
   */
  describe('Test Case 4: Homepage is accessible without authentication', () => {
    it('should render homepage without any authentication context', () => {
      // Render without any auth provider - homepage should still work
      renderWithRouter(['/'])

      // Homepage should be accessible
      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()
    })

    it('should not redirect unauthenticated users away from homepage', () => {
      renderWithRouter(['/'])

      // Verify we stay on homepage (no redirect)
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      // Verify we are not on login page
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument()
    })

    it('should display all public content to unauthenticated users', () => {
      renderWithRouter(['/'])

      // Verify public navigation links are visible
      expect(screen.getByTestId('login-link')).toBeVisible()
      expect(screen.getByTestId('register-link')).toBeVisible()

      // Verify CTA buttons are visible
      expect(screen.getByTestId('get-started-btn')).toBeInTheDocument()
      expect(screen.getByTestId('login-btn')).toBeInTheDocument()

      // Verify demo section is accessible
      expect(screen.getByTestId('demo-url-input')).toBeInTheDocument()
      expect(screen.getByTestId('demo-shorten-button')).toBeInTheDocument()
    })

    it('should allow unauthenticated users to interact with demo section', async () => {
      renderWithRouter(['/'])

      // Verify demo input is interactive
      const demoInput = screen.getByTestId('demo-url-input')
      expect(demoInput).not.toBeDisabled()

      // Verify shorten button is clickable
      const shortenButton = screen.getByTestId('demo-shorten-button')
      expect(shortenButton).not.toBeDisabled()
    })

    it('should have working navigation links to login and register for unauthenticated users', () => {
      renderWithRouter(['/'])

      // Verify login link has correct href
      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toHaveAttribute('href', '/login')

      // Verify register link has correct href
      const registerLink = screen.getByTestId('register-link')
      expect(registerLink).toHaveAttribute('href', '/register')

      // Verify footer navigation links
      const footerLoginLink = screen.getByTestId('footer-login-link')
      expect(footerLoginLink).toHaveAttribute('href', '/login')

      const footerRegisterLink = screen.getByTestId('footer-register-link')
      expect(footerRegisterLink).toHaveAttribute('href', '/register')
    })
  })

  /**
   * Additional integration tests for route configuration
   */
  describe('Route Configuration Verification', () => {
    it('should have "/" route configured in React Router', () => {
      renderWithRouter(['/'])

      // If route is not configured, React Router would show nothing or 404
      // Homepage being present confirms route is configured
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should render Home component at "/" route', () => {
      renderWithRouter(['/'])

      // Verify the Home component's unique elements are present
      expect(screen.getByText('URL Shortener')).toBeInTheDocument()
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
    })

    it('should handle navigation between all main routes', () => {
      renderWithRouter(['/'])

      // Navigate to login
      fireEvent.click(screen.getByTestId('login-link'))
      expect(screen.getByTestId('login-page')).toBeInTheDocument()

      // Navigate back home
      fireEvent.click(screen.getByTestId('back-to-home-link'))
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      // Navigate to register
      fireEvent.click(screen.getByTestId('register-link'))
      expect(screen.getByTestId('register-page')).toBeInTheDocument()

      // Navigate back home
      fireEvent.click(screen.getByTestId('back-to-home-link'))
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })
  })
})
