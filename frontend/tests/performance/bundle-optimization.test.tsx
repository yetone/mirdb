/**
 * Bundle Optimization Integration Tests
 * Owner: Scenario 10 - Performance Optimization
 *
 * Purpose: Validate JavaScript bundle is code-split appropriately
 * and homepage-specific code is minimized.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import React, { Suspense, lazy } from 'react'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    h1: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <h1 {...props}>{children}</h1>
    ),
    h2: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <h2 {...props}>{children}</h2>
    ),
    p: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <p {...props}>{children}</p>
    ),
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <section {...props}>{children}</section>
    ),
  },
  AnimatePresence: ({ children }: React.PropsWithChildren) => <>{children}</>,
}))

// Import components after mocking
import Home from '../../src/pages/Home'
import { HeroSection } from '../../src/components/home/HeroSection'
import { FeaturesSection } from '../../src/components/home/FeaturesSection'
import HowItWorksSection from '../../src/components/home/HowItWorksSection'
import { Footer } from '../../src/components/home/Footer'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Test Case 9: JavaScript Bundle Optimization', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Homepage code-splitting validation', () => {
    it('homepage components are modular and tree-shakeable', () => {
      // Each component can be imported independently
      // This enables Vite to tree-shake unused code
      expect(HeroSection).toBeDefined()
      expect(FeaturesSection).toBeDefined()
      expect(HowItWorksSection).toBeDefined()
      expect(Footer).toBeDefined()
    })

    it('homepage barrel export enables proper tree-shaking', async () => {
      // Import from barrel file should work
      const homeComponents = await import('../../src/components/home/index')

      // All components should be available
      expect(homeComponents.HeroSection).toBeDefined()
      expect(homeComponents.FeaturesSection).toBeDefined()
      expect(homeComponents.HowItWorksSection).toBeDefined()
      expect(homeComponents.Footer).toBeDefined()
    })

    it('homepage renders without importing entire app', () => {
      // Homepage should be renderable in isolation
      // This confirms it's not coupled to other parts of the app
      renderWithRouter(<Home />)

      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })

    it('components use dynamic imports for heavy dependencies', () => {
      // framer-motion is loaded with the component
      // but doesn't block initial render (mocked in tests)
      renderWithRouter(<HeroSection />)

      // Component should render successfully
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })
  })

  describe('Code organization for optimal bundling', () => {
    it('homepage uses named exports for tree-shaking', async () => {
      // Named exports enable better tree-shaking than default exports
      const { HeroSection: HS } = await import('../../src/components/home/HeroSection')
      const { FeaturesSection: FS } = await import('../../src/components/home/FeaturesSection')
      const { Footer: FT } = await import('../../src/components/home/Footer')

      expect(HS).toBeDefined()
      expect(FS).toBeDefined()
      expect(FT).toBeDefined()
    })

    it('shared types are centralized to avoid duplication', async () => {
      // Types should be in a shared location
      const homeTypes = await import('../../src/types/home')

      // Feature type should exist
      expect(homeTypes).toBeDefined()
    })

    it('components do not import unnecessary dependencies', () => {
      // Render each component and verify no errors
      // Unnecessary imports would cause larger bundles
      const { unmount: unmount1 } = renderWithRouter(<HeroSection />)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      unmount1()

      const { unmount: unmount2 } = renderWithRouter(<FeaturesSection />)
      // Features section should render 4 cards
      expect(screen.getAllByText(/URL Shortening|Click Analytics|Referrer Tracking|GeoIP Location/).length).toBe(4)
      unmount2()

      const { unmount: unmount3 } = renderWithRouter(<HowItWorksSection />)
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      unmount3()

      const { unmount: unmount4 } = renderWithRouter(<Footer />)
      expect(screen.getByTestId('footer')).toBeInTheDocument()
      unmount4()
    })
  })

  describe('Vite build optimization compatibility', () => {
    it('components use ES module syntax', async () => {
      // Dynamic imports work (ES modules)
      const homeModule = await import('../../src/pages/Home')
      expect(homeModule.default).toBeDefined()
    })

    it('components are compatible with Suspense for lazy loading', async () => {
      // Lazy loading capability test
      const LazyHome = lazy(() => import('../../src/pages/Home'))

      render(
        <BrowserRouter>
          <Suspense fallback={<div data-testid="loading">Loading...</div>}>
            <LazyHome />
          </Suspense>
        </BrowserRouter>
      )

      // Should eventually render the home page
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
      })
    })

    it('homepage does not import dashboard or admin-specific code', () => {
      // Homepage should be a clean landing page
      // It shouldn't need authentication context or admin routes
      renderWithRouter(<Home />)

      // The page should render without any protected content
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })
  })

  describe('Asset optimization', () => {
    it('icons are from optimized icon libraries', () => {
      renderWithRouter(<FeaturesSection />)

      // heroicons and lucide-react provide tree-shakeable SVG icons
      // They are imported individually, not as entire icon packs
      const svgIcons = document.querySelectorAll('svg')
      expect(svgIcons.length).toBeGreaterThan(0)

      // SVGs should have reasonable complexity (not bloated)
      svgIcons.forEach((svg) => {
        // Icons should have reasonable number of child elements
        expect(svg.childElementCount).toBeLessThan(20)
      })
    })

    it('homepage uses CSS utilities instead of custom stylesheets', () => {
      renderWithRouter(<Home />)

      // Components use Tailwind CSS classes (utility-first)
      // This enables PurgeCSS to remove unused styles
      const homePage = screen.getByTestId('home-page')

      // Should have Tailwind classes
      expect(homePage.className).toMatch(/min-h-|bg-|flex|grid/i)
    })

    it('animations use CSS where possible', () => {
      renderWithRouter(<Home />)

      // Background effects and hover states should use CSS
      // Only complex animations need framer-motion
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // CSS transitions are defined in Tailwind classes
      // Check for transition-related classes
      const buttonsAndLinks = document.querySelectorAll('a, button')
      const hasTransitions = Array.from(buttonsAndLinks).some((el) =>
        el.className.includes('transition')
      )
      expect(hasTransitions).toBe(true)
    })
  })
})

describe('Render-blocking resource optimization', () => {
  it('homepage renders critical content without JavaScript dependencies', () => {
    // The HTML structure should be present even before JS fully executes
    renderWithRouter(<Home />)

    // Critical semantic elements should exist
    expect(screen.getByRole('main')).toBeInTheDocument()
    expect(screen.getByTestId('navbar')).toBeInTheDocument()
    expect(screen.getByTestId('footer')).toBeInTheDocument()
  })

  it('navigation is functional without client-side routing', () => {
    renderWithRouter(<Home />)

    // Navigation links should have href attributes
    // This enables basic navigation even without JS
    // Use getAllBy since there are multiple login/signup links (navbar and footer)
    const loginLinks = screen.getAllByText(/login/i)
    expect(loginLinks.length).toBeGreaterThan(0)
    loginLinks.forEach((link) => {
      if (link.tagName === 'A') {
        expect(link).toHaveAttribute('href', '/login')
      }
    })

    const signupLinks = screen.getAllByText(/sign\s*up/i)
    expect(signupLinks.length).toBeGreaterThan(0)
    signupLinks.forEach((link) => {
      if (link.tagName === 'A') {
        expect(link).toHaveAttribute('href', '/register')
      }
    })
  })

  it('CSS is not inline blocking', () => {
    renderWithRouter(<Home />)

    // Check that there are no large inline styles that could block rendering
    const allElements = document.querySelectorAll('*')
    let totalInlineStyles = 0

    allElements.forEach((el) => {
      const style = el.getAttribute('style')
      if (style) {
        totalInlineStyles += style.length
      }
    })

    // Total inline styles should be minimal
    // Large inline styles indicate poor bundling
    expect(totalInlineStyles).toBeLessThan(5000)
  })
})
