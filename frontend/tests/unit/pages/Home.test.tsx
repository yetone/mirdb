/**
 * Home Page Tests
 * Owner: Scenario 1 - Homepage Accessibility and Routing
 *
 * Tests for the homepage component and route configuration.
 * Verifies that the homepage:
 * - Renders at the root path (/)
 * - Is accessible without authentication
 * - Displays Hero, Features, and Footer sections
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { renderWithProviders } from '../../utils/testHelpers'
import { Home } from '@/pages/Home'
import App from '@/App'

// Mock the API module to prevent network calls
vi.mock('@/api', () => ({
  default: {
    get: vi.fn().mockRejectedValue(new Error('Not authenticated')),
    post: vi.fn(),
    interceptors: {
      request: { use: vi.fn() },
      response: { use: vi.fn() },
    },
  },
}))

describe('Home Component', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 3: Render Home component in isolation', () => {
    it('renders without authentication context errors', () => {
      // Render Home component without any authentication
      renderWithProviders(<Home />, { authenticated: false })

      // Should render without throwing errors
      expect(screen.getByRole('main')).toBeInTheDocument()
    })

    it('displays Hero section with heading', () => {
      renderWithProviders(<Home />, { authenticated: false })

      // Hero section should be visible with the product name
      expect(screen.getByRole('heading', { name: /url shortener/i, level: 1 })).toBeInTheDocument()
    })

    it('displays Features section placeholder', () => {
      renderWithProviders(<Home />, { authenticated: false })

      // Features section should be present
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toHaveAttribute('aria-label', 'Features section')
    })

    it('displays Footer section', () => {
      renderWithProviders(<Home />, { authenticated: false })

      // Footer should be present
      const footer = screen.getByTestId('footer-section')
      expect(footer).toBeInTheDocument()
      expect(footer).toHaveAttribute('aria-label', 'Footer')
    })

    it('renders URL input field in Hero section', () => {
      renderWithProviders(<Home />, { authenticated: false })

      // URL input should be present and accessible
      expect(screen.getByRole('textbox', { name: /url to shorten/i })).toBeInTheDocument()
    })

    it('renders Shorten button', () => {
      renderWithProviders(<Home />, { authenticated: false })

      // Shorten button should be present
      expect(screen.getByRole('button', { name: /shorten/i })).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Route configuration', () => {
    it('renders Home at root path "/" as a public route', async () => {
      // Reset location to root path
      window.history.pushState({}, '', '/')

      // App includes its own BrowserRouter, so render directly
      render(<App />)

      // Wait for the Home component to render
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /url shortener/i, level: 1 })).toBeInTheDocument()
      })
    })

    it('does not require authentication to view homepage', () => {
      // Explicitly test without authentication token
      window.localStorage.getItem = vi.fn().mockReturnValue(null)

      renderWithProviders(<Home />, { authenticated: false })

      // Should render without authentication errors
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByText(/transform your long urls/i)).toBeInTheDocument()
    })

    it('homepage does not redirect unauthenticated users', async () => {
      // Reset location to root path
      window.history.pushState({}, '', '/')

      // App includes its own BrowserRouter, so render directly
      render(<App />)

      await waitFor(() => {
        // Should still be on homepage, not redirected
        expect(screen.getByRole('heading', { name: /url shortener/i })).toBeInTheDocument()
      })

      // Verify we're still at root path
      expect(window.location.pathname).toBe('/')
    })
  })

  describe('Accessibility', () => {
    it('has proper semantic structure', () => {
      renderWithProviders(<Home />, { authenticated: false })

      // Should have main landmark
      expect(screen.getByRole('main')).toBeInTheDocument()

      // Hero section should have aria-label
      expect(screen.getByLabelText(/hero section/i)).toBeInTheDocument()

      // Features section should be labeled
      expect(screen.getByLabelText(/features section/i)).toBeInTheDocument()

      // Footer should be present
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()
    })

    it('URL input has accessible label', () => {
      renderWithProviders(<Home />, { authenticated: false })

      const urlInput = screen.getByRole('textbox', { name: /url to shorten/i })
      expect(urlInput).toBeInTheDocument()
    })

    it('Shorten button has accessible label', () => {
      renderWithProviders(<Home />, { authenticated: false })

      const shortenButton = screen.getByRole('button', { name: /shorten url/i })
      expect(shortenButton).toBeInTheDocument()
    })
  })
})

describe('App Routing', () => {
  it('renders homepage at root path without ProtectedLayout wrapper', async () => {
    // Reset location to root path
    window.history.pushState({}, '', '/')

    // App includes its own BrowserRouter, so render directly
    render(<App />)

    // Wait for content to load
    await waitFor(() => {
      // Homepage should render with Hero section
      expect(screen.getByRole('heading', { name: /url shortener/i })).toBeInTheDocument()
    })

    // Should not show any login prompt or redirect
    expect(screen.queryByText(/please log in/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/unauthorized/i)).not.toBeInTheDocument()
  })

  it('route "/" is configured as public (no ProtectedLayout)', async () => {
    // This test verifies the route is public by checking that:
    // 1. The home page renders without auth
    // 2. No redirect occurs
    // 3. No auth error is shown
    window.history.pushState({}, '', '/')
    window.localStorage.getItem = vi.fn().mockReturnValue(null)

    render(<App />)

    await waitFor(() => {
      // Home page should render
      expect(screen.getByRole('heading', { name: /url shortener/i })).toBeInTheDocument()
    })

    // Verify no protected route behavior
    expect(screen.queryByText(/login/i)).not.toBeInTheDocument()
    expect(window.location.pathname).toBe('/')
  })
})
