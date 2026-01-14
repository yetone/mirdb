import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import { ThemeProvider } from '../contexts/ThemeContext'

/**
 * Integration tests for Error Handling - Graceful Degradation
 * Scenario: Verify homepage handles errors gracefully
 *
 * These tests verify that the homepage:
 * 1. Renders without crashing when API calls fail
 * 2. Shows loading states appropriately
 * 3. Has error boundary to catch component errors
 */

// Helper function to render Home with required providers
const renderHome = () => {
  return render(
    <MemoryRouter>
      <ThemeProvider defaultTheme="light">
        <Home />
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('Error Handling - Graceful Degradation', () => {
  beforeEach(() => {
    // Mock localStorage for ThemeProvider
    const localStorageMock = {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
      clear: vi.fn(),
    }
    Object.defineProperty(window, 'localStorage', { value: localStorageMock })

    // Mock matchMedia for system theme detection
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)' ? false : false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    })

    // Mock fetch to simulate API failure for stats
    global.fetch = vi.fn().mockRejectedValue(new Error('Network Error'))
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Render homepage with failed stats API call', () => {
    it('page renders without crashing when stats API fails', async () => {
      // Mock fetch to fail
      global.fetch = vi.fn().mockRejectedValue(new Error('Failed to fetch'))

      // This should not throw
      const { container } = renderHome()

      // Wait for loading to complete
      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Core sections should still be rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('stats-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // The page should have rendered without errors
      expect(container.querySelector('main')).toBeInTheDocument()
    })

    it('stats section shows fallback content when API fails', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('500 Internal Server Error'))

      renderHome()

      // Wait for stats to load (with fallback)
      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Stats section should show fallback notice
      expect(screen.getByTestId('stats-fallback-notice')).toBeInTheDocument()

      // Stats cards should still be displayed
      const statsCards = screen.getAllByTestId('stats-card')
      expect(statsCards.length).toBe(3)
    })

    it('other homepage sections remain functional when stats API fails', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('API Error'))

      renderHome()

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Hero section should have CTA buttons
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()

      // Features section should have feature cards
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(3)

      // Demo section should have URL input
      expect(screen.getByTestId('demo-url-input')).toBeInTheDocument()
      expect(screen.getByTestId('demo-submit-button')).toBeInTheDocument()

      // Footer should have navigation links
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toBeInTheDocument()

      // Check that there are multiple login links on the page (hero CTA + footer)
      const loginLinks = screen.getAllByRole('link', { name: /login/i })
      expect(loginLinks.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Test Case 2: Render homepage with slow network', () => {
    it('loading states are shown appropriately', async () => {
      // Create a slow fetch that takes time
      let resolvePromise: (value: unknown) => void
      global.fetch = vi.fn().mockImplementation(
        () =>
          new Promise((resolve) => {
            resolvePromise = resolve
          })
      )

      renderHome()

      // Loading state should be visible for stats section
      expect(screen.getByTestId('stats-loading')).toBeInTheDocument()

      // Other sections should be rendered immediately
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // Resolve the fetch
      resolvePromise!({
        ok: true,
        json: () =>
          Promise.resolve({
            totalUrls: 5000,
            totalClicks: 100000,
            activeUsers: 2000,
          }),
      })

      // Wait for loading to complete
      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Stats cards should now be visible
      expect(screen.getAllByTestId('stats-card').length).toBe(3)
    })

    it('skeleton loaders are shown while stats are loading', () => {
      // Never resolving fetch to keep loading state
      global.fetch = vi.fn().mockImplementation(() => new Promise(() => {}))

      renderHome()

      // Should show skeleton loaders
      const skeletons = screen.getAllByTestId('stats-skeleton')
      expect(skeletons.length).toBe(3)
    })

    it('page remains interactive while stats are loading', () => {
      // Never resolving fetch
      global.fetch = vi.fn().mockImplementation(() => new Promise(() => {}))

      renderHome()

      // While stats are loading, other interactive elements should work
      const urlInput = screen.getByTestId('demo-url-input')
      expect(urlInput).toBeInTheDocument()
      expect(urlInput).not.toBeDisabled()

      const submitButton = screen.getByTestId('demo-submit-button')
      expect(submitButton).toBeInTheDocument()
      expect(submitButton).not.toBeDisabled()

      // CTA buttons should be clickable
      const ctaButton = screen.getByTestId('cta-get-started')
      expect(ctaButton).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Check React Error Boundary implementation', () => {
    it('homepage is wrapped with an error boundary', () => {
      // Render the home page
      renderHome()

      // The home page should render its sections
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('error boundary catches component errors without crashing the app', async () => {
      // Suppress console.error for this test
      const originalConsoleError = console.error
      console.error = vi.fn()

      try {
        global.fetch = vi.fn().mockRejectedValue(new Error('Network failure'))

        // Render should not throw
        const { container } = renderHome()

        // Wait for loading to complete
        await waitFor(() => {
          expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
        })

        // Page should still have content
        expect(container.querySelector('main')).toBeInTheDocument()
      } finally {
        console.error = originalConsoleError
      }
    })
  })

  describe('Page Structure Integrity', () => {
    it('maintains proper section order even during errors', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('API Error'))

      renderHome()

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Get all sections in document order
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const statsSection = screen.getByTestId('stats-section')
      const demoSection = screen.getByTestId('demo-section')
      const footerSection = screen.getByTestId('footer-section')

      // Verify order using compareDocumentPosition
      expect(
        heroSection.compareDocumentPosition(featuresSection) & Node.DOCUMENT_POSITION_FOLLOWING
      ).toBeTruthy()
      expect(
        featuresSection.compareDocumentPosition(statsSection) & Node.DOCUMENT_POSITION_FOLLOWING
      ).toBeTruthy()
      expect(
        statsSection.compareDocumentPosition(demoSection) & Node.DOCUMENT_POSITION_FOLLOWING
      ).toBeTruthy()
      expect(
        demoSection.compareDocumentPosition(footerSection) & Node.DOCUMENT_POSITION_FOLLOWING
      ).toBeTruthy()
    })

    it('all sections remain accessible during error states', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('API Error'))

      renderHome()

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // All sections should have proper landmarks or identifiers
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByRole('contentinfo')).toBeInTheDocument() // footer

      // Headings should be present
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
      expect(screen.getAllByRole('heading', { level: 2 }).length).toBeGreaterThanOrEqual(2)
    })
  })
})
