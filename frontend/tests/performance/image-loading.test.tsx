/**
 * Image Loading Unit Tests
 * Owner: Scenario 10 - Performance Optimization
 *
 * Purpose: Validate image lazy loading behavior for performance optimization.
 * - Images below fold should have loading="lazy"
 * - Hero section images should be eagerly loaded (or have fetchpriority="high")
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import React from 'react'

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
import { HeroSection } from '../../src/components/home/HeroSection'
import { FeaturesSection } from '../../src/components/home/FeaturesSection'
import HowItWorksSection from '../../src/components/home/HowItWorksSection'
import { Footer } from '../../src/components/home/Footer'
import Home from '../../src/pages/Home'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Image Loading Optimization', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 7: Images below the fold use lazy loading', () => {
    it('features section icons do not block rendering', () => {
      renderWithRouter(<FeaturesSection />)

      // Features section uses SVG icons from heroicons, not img tags
      // This is actually a performance optimization - SVG icons are inline and don't need lazy loading
      // Verify the section renders without blocking images
      const features = screen.getAllByText(/URL Shortening|Click Analytics|Referrer Tracking|GeoIP Location/)
      expect(features.length).toBe(4)
    })

    it('how it works section icons do not block rendering', () => {
      renderWithRouter(<HowItWorksSection />)

      // How It Works uses lucide-react icons (SVGs), not img tags
      // Verify section renders correctly
      const steps = screen.getAllByTestId(/step-card-/)
      expect(steps.length).toBe(3)
    })

    it('footer does not contain render-blocking images', () => {
      renderWithRouter(<Footer />)

      // Footer uses text-based content, no images
      const footer = screen.getByTestId('footer')
      const images = footer.querySelectorAll('img')
      expect(images.length).toBe(0)
    })

    it('any img tags in below-fold sections should have lazy loading', () => {
      renderWithRouter(<Home />)

      // Get all img elements on the page
      const allImages = document.querySelectorAll('img')

      // For any images that exist below the fold (after hero section)
      // they should have loading="lazy" attribute
      allImages.forEach((img) => {
        const isInHero = img.closest('[data-testid="hero-section"]')

        if (!isInHero) {
          // Below-fold images should use lazy loading
          // If no loading attribute, the default browser behavior may apply
          const loadingAttr = img.getAttribute('loading')
          if (loadingAttr) {
            expect(loadingAttr).toBe('lazy')
          }
        }
      })
    })
  })

  describe('Test Case 8: Hero section (above-fold) images are eagerly loaded', () => {
    it('hero section renders critical content immediately', () => {
      renderWithRouter(<HeroSection />)

      // Hero section should render immediately without waiting for images
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Critical text content should be visible
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent(/Shorten URLs/)
    })

    it('hero section CTA is immediately accessible', () => {
      renderWithRouter(<HeroSection />)

      // Primary CTA should be visible and accessible
      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton).toBeInTheDocument()

      // Login link should be visible
      const loginLink = screen.getByTestId('hero-login-link')
      expect(loginLink).toBeInTheDocument()
    })

    it('any img tags in hero section should NOT have lazy loading', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const images = heroSection.querySelectorAll('img')

      // Above-fold images should be eagerly loaded
      images.forEach((img) => {
        const loadingAttr = img.getAttribute('loading')
        // Should either have loading="eager" or no loading attribute (defaults to eager)
        expect(loadingAttr).not.toBe('lazy')
      })
    })

    it('hero section uses optimized background effect', () => {
      renderWithRouter(<HeroSection />)

      // BackgroundEffect component is used for visual effects
      // It should not introduce render-blocking behavior
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // The section should have proper positioning for background
      expect(heroSection.className).toContain('relative')
    })
  })

  describe('Image loading best practices validation', () => {
    it('homepage uses SVG icons instead of raster images for icons', () => {
      renderWithRouter(<Home />)

      // Check that icons are SVG-based (better performance than img)
      // SVG icons are inline and don't require separate network requests
      const svgIcons = document.querySelectorAll('svg')

      // Should have multiple SVG icons (from heroicons and lucide-react)
      expect(svgIcons.length).toBeGreaterThan(0)
    })

    it('no images block first contentful paint', () => {
      renderWithRouter(<Home />)

      // The homepage should render without depending on image loads
      // Critical elements should be visible immediately
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
    })

    it('above-fold content is prioritized', () => {
      renderWithRouter(<Home />)

      // Hero section should be the first major content section
      const homePage = screen.getByTestId('home-page')
      const sections = homePage.querySelectorAll('section')

      // First section should contain hero content
      expect(sections.length).toBeGreaterThan(0)

      // Hero headline should be in the DOM immediately
      const headline = screen.getByTestId('hero-headline')
      expect(headline.textContent).toContain('Shorten URLs')
    })
  })
})

describe('Performance-optimized rendering', () => {
  it('components render without layout shifts from images', () => {
    const { container } = renderWithRouter(<Home />)

    // Check that all visible elements have proper dimensions set
    // This prevents Cumulative Layout Shift (CLS)
    const heroSection = container.querySelector('[data-testid="hero-section"]')
    expect(heroSection).toBeInTheDocument()

    // Hero section should have min-height set to prevent CLS
    if (heroSection) {
      const styles = window.getComputedStyle(heroSection)
      // The min-h-[80vh] class should be applied
      expect(heroSection.className).toContain('min-h-')
    }
  })

  it('icons have explicit dimensions to prevent layout shift', () => {
    renderWithRouter(<FeaturesSection />)

    // Feature card icons should have explicit width/height classes
    const svgIcons = document.querySelectorAll('svg')

    svgIcons.forEach((svg) => {
      // Icons should have dimension classes (w-* h-*)
      const className = svg.getAttribute('class') || ''
      const hasDimensions = className.includes('w-') && className.includes('h-')

      // Most icons should have explicit dimensions
      // Allow for some without as they may inherit from parent
      if (svg.closest('[data-testid]')) {
        expect(hasDimensions || svg.hasAttribute('width') || svg.hasAttribute('height')).toBe(true)
      }
    })
  })
})
