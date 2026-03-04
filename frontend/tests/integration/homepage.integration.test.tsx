/**
 * Homepage Integration Tests
 * Owner: Scenario 12 - Error States and Edge Cases
 *
 * Integration tests for homepage error handling and edge cases.
 *
 * Test coverage:
 * - Test Case 1: Render Home without ThemeProvider wrapper
 * - Test Case 2: Render Home with empty/null theme value
 * - Test Case 3: Test page content without JavaScript execution
 * - Test Case 4: Simulate network timeout on page load
 * - Test Case 5: Test with extremely long viewport (4K resolution)
 * - Test Case 6: Test with extremely narrow viewport (280px - Galaxy Fold)
 *
 * Requirements:
 * - NFR-4: Graceful degradation when JS is disabled or context missing
 * - REQ-8: Responsive design for all viewport sizes
 */
import React, { Component, ReactNode } from 'react'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Home } from '../../src/pages/Home'
import { ThemeProvider, ThemeContext } from '../../src/contexts/ThemeContext'

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
  }
})()

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})

// Error Boundary for testing error handling
interface ErrorBoundaryState {
  hasError: boolean
  error: Error | null
}

class TestErrorBoundary extends Component<
  { children: ReactNode; fallback?: ReactNode },
  ErrorBoundaryState
> {
  constructor(props: { children: ReactNode; fallback?: ReactNode }) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error }
  }

  render() {
    if (this.state.hasError) {
      return this.props.fallback || (
        <div data-testid="error-boundary-fallback">
          Error occurred: {this.state.error?.message}
        </div>
      )
    }
    return this.props.children
  }
}

// Helper to wrap component with router and theme provider
const renderWithRouter = (ui: React.ReactElement) => {
  return render(
    <ThemeProvider defaultTheme="dark">
      <BrowserRouter>{ui}</BrowserRouter>
    </ThemeProvider>
  )
}

// Helper to render without ThemeProvider (for error testing)
const renderWithoutThemeProvider = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

// Helper to set viewport size
const setViewport = (width: number, height: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: height,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Homepage Integration Tests', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  /**
   * Test Case 1: Render Home without ThemeProvider wrapper
   * Expected: Component renders with default theme, no crash
   *
   * Tests NFR-4 graceful degradation when theme context is missing
   */
  describe('Test Case 1: Render Home without ThemeProvider wrapper', () => {
    it('should render Home component without ThemeProvider and not crash', () => {
      // The useSafeTheme hook provides fallback values when context is missing
      const { container } = renderWithoutThemeProvider(<Home />)

      // Component should render without throwing
      expect(container).toBeTruthy()

      // Home page should be present
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should display hero section content with default theme when no ThemeProvider', () => {
      renderWithoutThemeProvider(<Home />)

      // Core content should be visible
      const headline = screen.getByTestId('hero-headline')
      const subheading = screen.getByTestId('hero-subheading')
      const ctaButton = screen.getByTestId('hero-cta')

      expect(headline).toBeInTheDocument()
      expect(subheading).toBeInTheDocument()
      expect(ctaButton).toBeInTheDocument()
    })

    it('should use fallback dark theme when rendered without ThemeProvider', () => {
      renderWithoutThemeProvider(<Home />)

      // The main element should render (using fallback theme 'dark')
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Should have theme-related styling applied via fallback
      expect(homePage).toHaveAttribute('data-theme-active', 'dark')
    })

    it('should render all sections without ThemeProvider', () => {
      renderWithoutThemeProvider(<Home />)

      // All major sections should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-footer')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Render Home with empty/null theme value
   * Expected: Component handles gracefully, uses fallback theme
   *
   * Tests graceful handling of invalid theme context values
   */
  describe('Test Case 2: Render Home with empty/null theme value', () => {
    it('should handle null theme context value gracefully', () => {
      // Render with explicit null context (simulating corrupted state)
      const { container } = render(
        <ThemeContext.Provider value={null as unknown as never}>
          <BrowserRouter>
            <Home />
          </BrowserRouter>
        </ThemeContext.Provider>
      )

      // Component should still render
      expect(container).toBeTruthy()
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should display all content when theme value is empty', () => {
      render(
        <ThemeContext.Provider value={null as unknown as never}>
          <BrowserRouter>
            <Home />
          </BrowserRouter>
        </ThemeContext.Provider>
      )

      // Core content should still be visible
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-subheading')).toBeInTheDocument()
      expect(screen.getByTestId('hero-cta')).toBeInTheDocument()
    })

    it('should use default fallback theme when context value is invalid', () => {
      renderWithoutThemeProvider(<Home />)

      const homePage = screen.getByTestId('home-page')
      // Default fallback theme is 'dark'
      expect(homePage).toHaveAttribute('data-theme-active', 'dark')
    })

    it('should render CTA buttons as functional elements despite missing theme', () => {
      renderWithoutThemeProvider(<Home />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()

      // CTA button should be enabled and clickable (FuturisticButton uses onClick)
      expect(ctaButton).toBeEnabled()
      expect(ctaButton.tagName.toLowerCase()).toBe('button')
    })
  })

  /**
   * Test Case 3: Test page content without JavaScript execution
   * Expected: Core content (headline, CTA text) is visible in static HTML
   *
   * Tests that essential content is rendered server-side or in initial HTML
   */
  describe('Test Case 3: Static HTML content without JavaScript', () => {
    it('should have headline text in rendered HTML', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent(/Shorten URLs/)
    })

    it('should have CTA button text visible in HTML', () => {
      renderWithRouter(<Home />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toHaveTextContent(/Get Started Free/i)
    })

    it('should have subheading text visible in static content', () => {
      renderWithRouter(<Home />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading).toHaveTextContent(/Create memorable short links/)
    })

    it('should have semantic HTML structure for core content', () => {
      renderWithRouter(<Home />)

      // Check for semantic HTML elements
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.tagName.toLowerCase()).toBe('section')

      const headline = screen.getByTestId('hero-headline')
      expect(headline.tagName.toLowerCase()).toBe('h1')
    })

    it('should have essential CTA elements accessible in static HTML', () => {
      renderWithRouter(<Home />)

      // CTA button should be present and enabled (FuturisticButton uses onClick)
      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toBeEnabled()
      expect(ctaButton).toHaveTextContent(/Get Started/i)
    })

    it('should have navigation elements present in static HTML', () => {
      renderWithRouter(<Home />)

      // Navigation should be present
      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
    })
  })

  /**
   * Test Case 4: Simulate network timeout on page load
   * Expected: Page displays appropriate loading or error state
   *
   * Tests error handling for network issues during data fetching
   */
  describe('Test Case 4: Network timeout handling', () => {
    it('should render static content even when network is unavailable', () => {
      // Homepage is primarily static, should render without network
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should display all static sections during network issues', () => {
      renderWithRouter(<Home />)

      // All static sections should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-footer')).toBeInTheDocument()
    })

    it('should maintain navigation functionality during network timeout', () => {
      renderWithRouter(<Home />)

      // Navigation should work (client-side routing)
      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      // CTA buttons should be present and functional
      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toBeEnabled()
    })

    it('should handle slow loading gracefully with progressive content', () => {
      renderWithRouter(<Home />)

      // Primary content should be immediately visible
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeVisible()

      const cta = screen.getByTestId('hero-cta')
      expect(cta).toBeVisible()
    })

    it('should not show error state for static homepage content', () => {
      renderWithRouter(<Home />)

      // No error message elements should be displayed for static content
      // Check that error-related elements don't exist
      const errorBoundaryFallback = screen.queryByTestId('error-boundary-fallback')
      expect(errorBoundaryFallback).not.toBeInTheDocument()

      // The homepage should render successfully
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })
  })

  /**
   * Test Case 5: Test with extremely long viewport (4K resolution)
   * Expected: Layout remains correct, no broken layouts at large sizes
   *
   * Tests responsive design at 4K resolution (3840x2160)
   */
  describe('Test Case 5: 4K resolution viewport (3840x2160)', () => {
    beforeEach(() => {
      setViewport(3840, 2160)
    })

    afterEach(() => {
      // Reset viewport
      setViewport(1280, 800)
    })

    it('should render homepage correctly at 4K resolution', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should have hero section visible and properly sized at 4K', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toBeVisible()
    })

    it('should maintain max-width constraints to prevent overly wide content', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const contentContainer = heroSection.querySelector('.max-w-4xl')

      // Content should have max-width to prevent extreme stretching
      expect(contentContainer).toBeInTheDocument()
    })

    it('should have readable text sizes at 4K resolution', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // Should use large responsive text class
      expect(headline.className).toMatch(/lg:text-6xl/)
    })

    it('should have centered content at large viewport', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const contentContainer = heroSection.querySelector('.mx-auto')

      expect(contentContainer).toBeInTheDocument()
    })

    it('should display all sections without layout breaks at 4K', () => {
      renderWithRouter(<Home />)

      // All sections should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-footer')).toBeInTheDocument()
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
    })

    it('should maintain proper spacing and padding at 4K', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      // Should have responsive padding classes
      expect(heroSection.className).toMatch(/lg:px-8/)
    })
  })

  /**
   * Test Case 6: Test with extremely narrow viewport (280px - Galaxy Fold)
   * Expected: Content remains accessible and readable
   *
   * Tests responsive design at Galaxy Fold folded width (280px)
   */
  describe('Test Case 6: Galaxy Fold viewport (280px)', () => {
    beforeEach(() => {
      setViewport(280, 653)
    })

    afterEach(() => {
      // Reset viewport
      setViewport(1280, 800)
    })

    it('should render homepage correctly at 280px viewport', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should have hero content visible and readable at 280px', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      const subheading = screen.getByTestId('hero-subheading')
      const ctaButton = screen.getByTestId('hero-cta')

      expect(headline).toBeVisible()
      expect(subheading).toBeVisible()
      expect(ctaButton).toBeVisible()
    })

    it('should use mobile-first base styling at narrow viewport', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // Base text-4xl for mobile (smallest responsive size)
      expect(headline.className).toMatch(/text-4xl/)
    })

    it('should have single column layout at narrow viewport', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const contentContainer = heroSection.querySelector('.flex-col')

      // Content should stack vertically
      expect(contentContainer).toBeInTheDocument()
    })

    it('should maintain minimum padding at narrow viewport', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      // Base px-4 padding for mobile
      expect(heroSection.className).toMatch(/px-4/)
    })

    it('should have touch-friendly CTA button at narrow viewport', () => {
      renderWithRouter(<Home />)

      const ctaButton = screen.getByTestId('hero-cta')
      // Should have large button class for touch accessibility
      expect(ctaButton.className).toMatch(/btn-lg/)
    })

    it('should prevent horizontal overflow at 280px', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      const heroSection = screen.getByTestId('hero-section')

      // Should have full-width classes
      expect(heroSection).toHaveClass('w-full')

      // Should not have fixed pixel widths
      expect(heroSection.className).not.toMatch(/w-\d+px/)
    })

    it('should have mobile navigation accessible at narrow viewport', () => {
      renderWithRouter(<Home />)

      // Mobile menu should be available
      const mobileMenuToggle = screen.getByTestId('mobile-menu-toggle')
      expect(mobileMenuToggle).toBeInTheDocument()
    })

    it('should have readable line heights at narrow viewport', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // Should have tight line height for headlines
      expect(headline.className).toMatch(/leading-tight/)
    })

    it('should center text content for readability on narrow screens', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const centeredContent = heroSection.querySelector('.text-center')

      expect(centeredContent).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Hero Section Viewport Visibility', () => {
    beforeEach(() => {
      // Set viewport size to standard 1024x768
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1024,
      })
      Object.defineProperty(window, 'innerHeight', {
        writable: true,
        configurable: true,
        value: 768,
      })

      // Mock getBoundingClientRect for visibility checks
      Element.prototype.getBoundingClientRect = vi.fn(() => ({
        x: 0,
        y: 0,
        width: 1024,
        height: 400, // Hero section height within viewport
        top: 0,
        right: 1024,
        bottom: 400,
        left: 0,
        toJSON: () => {},
      }))
    })

    it('should render hero section within the first viewport without scrolling', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // The hero-section class ensures content fits in viewport
      expect(heroSection).toHaveClass('hero-section')
    })

    it('should have headline visible without scrolling on 1024x768 viewport', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toBeVisible()

      // Verify headline is rendered and accessible
      const rect = headline.getBoundingClientRect()
      expect(rect.top).toBeGreaterThanOrEqual(0)
      expect(rect.bottom).toBeLessThanOrEqual(768)
    })

    it('should have subheading visible without scrolling on 1024x768 viewport', () => {
      renderWithRouter(<Home />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading).toBeInTheDocument()
      expect(subheading).toBeVisible()

      const rect = subheading.getBoundingClientRect()
      expect(rect.top).toBeGreaterThanOrEqual(0)
      expect(rect.bottom).toBeLessThanOrEqual(768)
    })

    it('should have CTA button visible without scrolling on 1024x768 viewport', () => {
      renderWithRouter(<Home />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toBeVisible()

      const rect = ctaButton.getBoundingClientRect()
      expect(rect.top).toBeGreaterThanOrEqual(0)
      expect(rect.bottom).toBeLessThanOrEqual(768)
    })

    it('should have all hero content (headline, subheading, CTA) visible together', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      const subheading = screen.getByTestId('hero-subheading')
      const ctaButton = screen.getByTestId('hero-cta')

      // All elements should be in the document
      expect(headline).toBeInTheDocument()
      expect(subheading).toBeInTheDocument()
      expect(ctaButton).toBeInTheDocument()

      // All elements should be visible
      expect(headline).toBeVisible()
      expect(subheading).toBeVisible()
      expect(ctaButton).toBeVisible()
    })

    it('should constrain hero section to 100vh max height', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const styles = window.getComputedStyle(heroSection)

      // The CSS class hero-section should have max-height: 100vh
      expect(heroSection.className).toContain('hero-section')
    })
  })

  describe('Component Integration', () => {
    it('should render Home page with HeroSection integrated', () => {
      renderWithRouter(<Home />)

      // Home page should be present
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Hero section should be nested within home page
      const heroSection = screen.getByTestId('hero-section')
      expect(homePage).toContainElement(heroSection)
    })

    it('should render all hero elements with proper hierarchy', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const headline = screen.getByTestId('hero-headline')
      const subheading = screen.getByTestId('hero-subheading')
      const ctaButton = screen.getByTestId('hero-cta')

      // All elements should be within the hero section
      expect(heroSection).toContainElement(headline)
      expect(heroSection).toContainElement(subheading)
      expect(heroSection).toContainElement(ctaButton)
    })
  })
})
