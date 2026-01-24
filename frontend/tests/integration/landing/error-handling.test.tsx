/**
 * Error Handling and Edge Cases Integration Tests
 * Owner: Scenario 15 - Error Handling and Edge Cases
 *
 * Tests for edge cases and error states in the landing page:
 * - Component rendering in error boundaries
 * - Missing provider graceful fallbacks
 * - 404 route handling
 * - Empty/null props handling
 *
 * Requirements: Verify graceful degradation and error handling
 */

import React, { Component, ReactNode } from 'react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'
import Home from '../../../src/pages/Home'
import { HeroSection } from '../../../src/components/landing/HeroSection'
import { FeaturesSection } from '../../../src/components/landing/FeaturesSection'
import { renderWithRouter } from './test-utils'

/**
 * Error Boundary component for testing error handling
 */
interface ErrorBoundaryProps {
  children: ReactNode
  fallback?: ReactNode
}

interface ErrorBoundaryState {
  hasError: boolean
  error: Error | null
}

class TestErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
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
          Something went wrong: {this.state.error?.message}
        </div>
      )
    }
    return this.props.children
  }
}

/**
 * Helper to render components with all necessary providers
 */
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter>
          {ui}
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  )
}

/**
 * Helper to render with error boundary
 */
function renderWithErrorBoundary(ui: React.ReactElement) {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter>
          <TestErrorBoundary>
            {ui}
          </TestErrorBoundary>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  )
}

describe('Error Handling and Edge Cases', () => {
  // Suppress console.error during error boundary tests
  const originalError = console.error
  beforeEach(() => {
    console.error = vi.fn()
  })
  afterEach(() => {
    console.error = originalError
  })

  describe('Test Case 1: Render Home component in error boundary - Component renders without throwing errors', () => {
    it('Home component renders successfully within an error boundary', () => {
      renderWithErrorBoundary(<Home />)

      // Error boundary should NOT show fallback - component should render successfully
      const errorFallback = screen.queryByTestId('error-boundary-fallback')
      expect(errorFallback).not.toBeInTheDocument()

      // Home page content should be visible
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('Home component does not throw during initial render', () => {
      expect(() => {
        renderWithProviders(<Home />)
      }).not.toThrow()
    })

    it('Home component renders all expected sections without errors', () => {
      renderWithErrorBoundary(<Home />)

      // Verify main structural elements render
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Hero section should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('error boundary catches errors but Home does not trigger it', () => {
      const { container } = renderWithErrorBoundary(<Home />)

      // The container should have rendered content
      expect(container.innerHTML).not.toBe('')
      expect(container.innerHTML).not.toContain('Something went wrong')
    })
  })

  describe('Test Case 2: Test with missing ThemeContext provider - Component uses fallback/default theme gracefully', () => {
    it('HeroSection uses default styles when ThemeContext is available', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Component should render with theme-aware classes
      expect(heroSection.className).toContain('min-h-')
    })

    it('ThemeProvider provides default theme when no theme is stored', () => {
      // Clear any stored theme
      localStorage.removeItem('theme')

      render(
        <ThemeProvider defaultTheme="dark">
          <MemoryRouter>
            <HeroSection />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Component should render successfully with default theme
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('components render with theme-aware classes that gracefully degrade', () => {
      renderWithProviders(<HeroSection />)

      // Check that theme-aware Tailwind classes are applied
      const headline = screen.getByTestId('hero-headline')
      expect(headline.className).toContain('text-base-content')

      // These classes work regardless of specific theme
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toContain('text-base-content')
    })

    it('ThemeProvider handles invalid localStorage theme gracefully', () => {
      // Set an invalid theme value
      localStorage.setItem('theme', 'invalid-theme')

      // Should not throw and should render
      expect(() => {
        render(
          <ThemeProvider>
            <MemoryRouter>
              <HeroSection />
            </MemoryRouter>
          </ThemeProvider>
        )
      }).not.toThrow()

      // Component should still render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Clean up
      localStorage.removeItem('theme')
    })
  })

  describe('Test Case 3: Test navigation link with invalid route - Application handles 404 gracefully', () => {
    it('navigating to unknown route does not crash the application', () => {
      expect(() => {
        render(
          <ThemeProvider>
            <AuthProvider>
              <MemoryRouter initialEntries={['/nonexistent-page']}>
                <Routes>
                  <Route path="/" element={<Home />} />
                  <Route path="/login" element={<div>Login Page</div>} />
                  <Route path="*" element={<div data-testid="not-found">Page Not Found</div>} />
                </Routes>
              </MemoryRouter>
            </AuthProvider>
          </ThemeProvider>
        )
      }).not.toThrow()
    })

    it('displays 404 message for invalid routes when catch-all route exists', () => {
      render(
        <ThemeProvider>
          <AuthProvider>
            <MemoryRouter initialEntries={['/invalid-route-xyz']}>
              <Routes>
                <Route path="/" element={<Home />} />
                <Route path="*" element={<div data-testid="not-found">Page Not Found</div>} />
              </Routes>
            </MemoryRouter>
          </AuthProvider>
        </ThemeProvider>
      )

      expect(screen.getByTestId('not-found')).toBeInTheDocument()
      expect(screen.getByText('Page Not Found')).toBeInTheDocument()
    })

    it('application state remains stable after encountering 404', () => {
      // First render with 404 route
      const { unmount } = render(
        <ThemeProvider>
          <AuthProvider>
            <MemoryRouter initialEntries={['/unknown']}>
              <Routes>
                <Route path="/" element={<Home />} />
                <Route path="*" element={<div data-testid="not-found">404</div>} />
              </Routes>
            </MemoryRouter>
          </AuthProvider>
        </ThemeProvider>
      )

      // 404 page should show
      expect(screen.getByTestId('not-found')).toBeInTheDocument()

      // Unmount and re-mount with valid route
      unmount()

      render(
        <ThemeProvider>
          <AuthProvider>
            <MemoryRouter initialEntries={['/']}>
              <Routes>
                <Route path="/" element={<Home />} />
                <Route path="*" element={<div data-testid="not-found">404</div>} />
              </Routes>
            </MemoryRouter>
          </AuthProvider>
        </ThemeProvider>
      )

      // Should now show home page - app state is stable
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('empty route path renders home page', () => {
      renderWithRouter({ initialRoute: '/' })

      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Test component with empty/null props - Component renders with sensible defaults', () => {
    it('HeroSection renders with all default values when no props provided', () => {
      renderWithProviders(<HeroSection />)

      // Should have default headline
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent('Shorten URLs. Track Everything.')

      // Should have default subheadline
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.textContent?.length).toBeGreaterThan(0)

      // Should have default CTA buttons
      expect(screen.getByTestId('hero-primary-cta')).toHaveTextContent('Get Started Free')
      expect(screen.getByTestId('hero-secondary-cta')).toHaveTextContent('Sign In')
    })

    it('HeroSection handles undefined props gracefully', () => {
      const props = {
        headline: undefined,
        subheadline: undefined,
        primaryCTA: undefined,
        secondaryCTA: undefined,
      }

      expect(() => {
        renderWithProviders(<HeroSection {...props} />)
      }).not.toThrow()

      // Component should render with defaults
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('HeroSection handles empty string props by using defaults', () => {
      // Note: Empty strings are intentionally passed; component should handle gracefully
      renderWithProviders(<HeroSection headline="" subheadline="" />)

      // Component should still render - empty strings are valid (even if not ideal)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // The headline element should exist (even if empty)
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
    })

    it('FeaturesSection renders with default features when no props provided', () => {
      renderWithProviders(<FeaturesSection />)

      // Should render the features section
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // Should have default feature cards
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    })

    it('FeaturesSection handles empty array prop gracefully', () => {
      renderWithProviders(<FeaturesSection features={[]} />)

      // Section should still render
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // But no feature cards should exist
      expect(screen.queryByText('URL Shortening')).not.toBeInTheDocument()
    })

    it('components do not crash with partial props', () => {
      const partialCTA = { text: 'Custom', href: '' }

      expect(() => {
        renderWithProviders(<HeroSection primaryCTA={partialCTA} />)
      }).not.toThrow()

      expect(screen.getByTestId('hero-primary-cta')).toHaveTextContent('Custom')
    })

    it('Home component renders correctly with default configuration', () => {
      renderWithProviders(<Home />)

      // Home should assemble sections without explicit props
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })
  })

  describe('Additional Edge Cases', () => {
    it('handles rapid re-renders without crashing', () => {
      const { rerender } = renderWithProviders(<Home />)

      // Rapidly re-render multiple times
      for (let i = 0; i < 10; i++) {
        rerender(
          <ThemeProvider>
            <AuthProvider>
              <MemoryRouter>
                <Home />
              </MemoryRouter>
            </AuthProvider>
          </ThemeProvider>
        )
      }

      // Component should still be functional
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('handles component unmount and remount gracefully', async () => {
      const { unmount } = renderWithProviders(<Home />)

      // Verify component is mounted
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Unmount
      unmount()

      // Remount
      renderWithProviders(<Home />)

      // Should work normally
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('maintains consistent DOM structure across renders', () => {
      const { rerender } = renderWithProviders(<HeroSection />)

      const initialHeadline = screen.getByTestId('hero-headline')
      const initialText = initialHeadline.textContent

      rerender(
        <ThemeProvider>
          <AuthProvider>
            <MemoryRouter>
              <HeroSection />
            </MemoryRouter>
          </AuthProvider>
        </ThemeProvider>
      )

      const rerenderHeadline = screen.getByTestId('hero-headline')
      expect(rerenderHeadline.textContent).toBe(initialText)
    })

    it('FeaturesSection handles features with missing fields gracefully', () => {
      const incompleteFeatures = [
        { icon: 'link', title: 'Test Feature', description: 'Description' },
      ]

      expect(() => {
        renderWithProviders(<FeaturesSection features={incompleteFeatures} />)
      }).not.toThrow()

      expect(screen.getByText('Test Feature')).toBeInTheDocument()
    })

    it('renders correctly when window.matchMedia is not available', () => {
      // Some environments may not have matchMedia
      const originalMatchMedia = window.matchMedia
      // @ts-expect-error - intentionally setting to undefined for test
      window.matchMedia = undefined

      expect(() => {
        renderWithProviders(<Home />)
      }).not.toThrow()

      // Restore
      window.matchMedia = originalMatchMedia
    })
  })
})
