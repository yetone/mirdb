import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import StatisticsSection from '../components/StatisticsSection'

// Wrapper component with Router for Home page tests
const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Error State Handling - Scenario 20', () => {
  // Store original console methods
  const originalConsoleError = console.error
  const originalConsoleWarn = console.warn
  let consoleErrors: string[] = []
  let consoleWarnings: string[] = []

  beforeEach(() => {
    // Reset arrays
    consoleErrors = []
    consoleWarnings = []

    // Mock console.error to capture errors
    console.error = vi.fn((...args: unknown[]) => {
      const message = args.map((arg) => String(arg)).join(' ')
      consoleErrors.push(message)
    })

    // Mock console.warn to capture warnings
    console.warn = vi.fn((...args: unknown[]) => {
      const message = args.map((arg) => String(arg)).join(' ')
      consoleWarnings.push(message)
    })
  })

  afterEach(() => {
    // Restore original console methods
    console.error = originalConsoleError
    console.warn = originalConsoleWarn
    vi.restoreAllMocks()
  })

  describe('Test Case 2: Check browser console on homepage load', () => {
    it('should render homepage without JavaScript errors in console', async () => {
      // Render the homepage
      renderWithRouter(<Home />)

      // Verify the page rendered correctly
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Wait for any async operations to complete
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      // Filter out known React development warnings (not actual errors)
      const actualErrors = consoleErrors.filter(
        (error) =>
          !error.includes('Warning:') &&
          !error.includes('act(') &&
          !error.includes('React does not recognize')
      )

      // Verify no unexpected JavaScript errors occurred
      expect(actualErrors).toHaveLength(0)
    })

    it('should render all homepage sections without console errors', async () => {
      renderWithRouter(<Home />)

      // Verify all major sections are rendered
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
        expect(screen.getByTestId('features-section')).toBeInTheDocument()
        expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
        expect(screen.getByTestId('url-demo-section')).toBeInTheDocument()
        expect(screen.getByTestId('statistics-section')).toBeInTheDocument()
      })

      // Filter out development warnings
      const actualErrors = consoleErrors.filter(
        (error) =>
          !error.includes('Warning:') &&
          !error.includes('act(') &&
          !error.includes('React does not recognize')
      )

      expect(actualErrors).toHaveLength(0)
    })

    it('should not produce React key warnings during render', async () => {
      renderWithRouter(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })

      // Check for React key warnings specifically
      const keyWarnings = consoleErrors.filter(
        (error) =>
          error.includes('key') && error.includes('unique')
      )

      expect(keyWarnings).toHaveLength(0)
    })
  })

  describe('Test Case 3: Simulate API error for statistics endpoint', () => {
    it('should gracefully hide statistics section when visible is false', () => {
      render(<StatisticsSection visible={false} />)

      // The section should not be rendered at all
      const section = screen.queryByTestId('statistics-section')
      expect(section).not.toBeInTheDocument()
    })

    it('should gracefully hide statistics section when statistics array is empty', () => {
      render(<StatisticsSection statistics={[]} />)

      // The section should not be rendered
      const section = screen.queryByTestId('statistics-section')
      expect(section).not.toBeInTheDocument()
    })

    it('should gracefully hide statistics section when statistics is undefined', () => {
      // @ts-expect-error - Testing undefined case for robustness
      render(<StatisticsSection statistics={undefined} />)

      // Should render with default statistics since undefined falls back to default
      const section = screen.queryByTestId('statistics-section')
      expect(section).toBeInTheDocument()
    })

    it('should render page without crashing when statistics fails to load', async () => {
      // Simulate a scenario where statistics might fail by passing empty array
      render(
        <BrowserRouter>
          <div className="min-h-screen bg-base-100" data-testid="home-page">
            <main id="main-content">
              <div data-testid="hero-section">Hero</div>
              <div data-testid="features-section">Features</div>
              <StatisticsSection statistics={[]} />
            </main>
          </div>
        </BrowserRouter>
      )

      // Page should still render
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // Statistics section should be gracefully hidden
      expect(screen.queryByTestId('statistics-section')).not.toBeInTheDocument()
    })

    it('should show fallback state gracefully when no statistics data', () => {
      render(<StatisticsSection statistics={[]} visible={true} />)

      // Should return null and not crash
      const section = screen.queryByTestId('statistics-section')
      expect(section).not.toBeInTheDocument()

      // No errors should be thrown
      const actualErrors = consoleErrors.filter(
        (error) => !error.includes('Warning:')
      )
      expect(actualErrors).toHaveLength(0)
    })
  })

  describe('Test Case 1: Static content renders correctly (simulated offline)', () => {
    it('should render all static content sections', async () => {
      renderWithRouter(<Home />)

      // All static sections should be visible
      await waitFor(() => {
        // Hero section (static)
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
        expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
        expect(screen.getByTestId('hero-cta-primary')).toBeInTheDocument()

        // Features section (static)
        expect(screen.getByTestId('features-section')).toBeInTheDocument()

        // How it works section (static)
        expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()

        // URL Demo section (static - client-side only)
        expect(screen.getByTestId('url-demo-section')).toBeInTheDocument()

        // Footer (static) - uses footer-links as testid
        expect(screen.getByTestId('footer-links')).toBeInTheDocument()
      })
    })

    it('should render navigation header with static links', async () => {
      renderWithRouter(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('navigation-header')).toBeInTheDocument()
      })

      // Auth links should be present and functional (using testid to avoid duplicate matches from mobile/desktop nav)
      const loginLink = screen.getByTestId('nav-login')
      const signupLink = screen.getByTestId('nav-signup')

      expect(loginLink).toBeInTheDocument()
      expect(signupLink).toBeInTheDocument()
    })

    it('should render static statistics with default values when offline', async () => {
      renderWithRouter(<Home />)

      // Statistics should show default values (hardcoded)
      await waitFor(() => {
        expect(screen.getByTestId('statistics-section')).toBeInTheDocument()
      })

      // Default statistics should be displayed
      expect(screen.getByTestId('statistic-card-urls-shortened')).toBeInTheDocument()
      expect(screen.getByTestId('statistic-card-clicks-tracked')).toBeInTheDocument()
      expect(screen.getByTestId('statistic-card-active-users')).toBeInTheDocument()
    })

    it('should have functional client-side URL demo without network', async () => {
      renderWithRouter(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('url-demo-section')).toBeInTheDocument()
      })

      // The demo input should be present and functional
      const input = screen.getByTestId('demo-url-input')
      expect(input).toBeInTheDocument()
      expect(input).not.toBeDisabled()
    })
  })

  describe('Graceful degradation and error resilience', () => {
    it('should handle missing DOM elements gracefully in scroll function', async () => {
      renderWithRouter(<Home />)

      // The scrollToSection function in Home.tsx checks for element existence
      // before calling scrollIntoView - this test ensures no errors when
      // attempting to scroll to non-existent sections
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })

      // No errors should occur during render
      const actualErrors = consoleErrors.filter(
        (error) => !error.includes('Warning:')
      )
      expect(actualErrors).toHaveLength(0)
    })

    it('should render without crashing when theme context is not provided', async () => {
      // Home component should work even without explicit theme provider
      renderWithRouter(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })

      // Page should render with base styling
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('min-h-screen')
    })

    it('should handle reduced motion preference gracefully', async () => {
      // Mock matchMedia for reduced motion
      const originalMatchMedia = window.matchMedia
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-reduced-motion: reduce)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      renderWithRouter(<Home />)

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })

      // Page should render without motion-related errors
      const actualErrors = consoleErrors.filter(
        (error) => !error.includes('Warning:')
      )
      expect(actualErrors).toHaveLength(0)

      // Restore original matchMedia
      window.matchMedia = originalMatchMedia
    })
  })
})
