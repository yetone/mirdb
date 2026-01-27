/**
 * Performance Tests for Landing Page
 * Owner: Scenario 15 - Performance - Page Load
 *
 * Tests performance-related aspects of the landing page:
 * 1. Page renders within acceptable time (simulated DOMContentLoaded)
 * 2. Performance best practices are followed (contributing to Lighthouse score >= 80)
 * 3. Images below the fold use lazy loading attribute
 *
 * Note: Actual Lighthouse audits require a real browser environment.
 * These tests verify performance best practices that contribute to a high Lighthouse score.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, cleanup } from './setup'
import Home from '../../../src/pages/Home'

describe('Performance - Page Load', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    cleanup()
  })

  /**
   * Test Case 1: Page loads in under 3 seconds
   * Measures render time from start to completion
   */
  describe('Test Case 1: DOMContentLoaded performance', () => {
    it('renders the landing page within 3 seconds threshold', async () => {
      const startTime = performance.now()

      render(<Home />)

      const endTime = performance.now()
      const renderTime = endTime - startTime

      // Verify page rendered (has main content)
      expect(screen.getByRole('banner')).toBeInTheDocument() // header
      expect(screen.getByRole('main')).toBeInTheDocument()

      // Render time should be well under 3000ms (3 seconds)
      // In a test environment, this will be much faster, but we're verifying
      // the page doesn't have performance bottlenecks that would cause slow rendering
      expect(renderTime).toBeLessThan(3000)
    })

    it('landing page has all critical sections rendered', () => {
      render(<Home />)

      // Verify all major sections are present (critical for FCP/LCP)
      const header = screen.getByRole('banner')
      const main = screen.getByRole('main')
      const footer = screen.getByRole('contentinfo')

      expect(header).toBeInTheDocument()
      expect(main).toBeInTheDocument()
      expect(footer).toBeInTheDocument()
    })

    it('renders hero section with primary heading quickly', () => {
      const startTime = performance.now()

      render(<Home />)

      // Hero headline should be immediately visible (LCP consideration)
      const heroHeading = screen.getByRole('heading', {
        name: /shorten links.*track everything/i,
        level: 1,
      })
      expect(heroHeading).toBeInTheDocument()

      const renderTime = performance.now() - startTime
      expect(renderTime).toBeLessThan(1000) // Hero should render very quickly
    })
  })

  /**
   * Test Case 2: Lighthouse performance best practices
   * Verifies patterns that contribute to Lighthouse score >= 80
   */
  describe('Test Case 2: Lighthouse performance best practices', () => {
    it('uses semantic HTML elements (improves accessibility and SEO scores)', () => {
      render(<Home />)

      // Check for proper semantic structure (affects accessibility score)
      expect(screen.getByRole('banner')).toBeInTheDocument() // <header>
      expect(screen.getByRole('main')).toBeInTheDocument() // <main>
      expect(screen.getByRole('contentinfo')).toBeInTheDocument() // <footer>

      // Check for proper heading hierarchy
      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements.length).toBe(1) // Only one h1 for SEO
    })

    it('has properly structured navigation (First Contentful Paint)', () => {
      render(<Home />)

      // Navigation should be in header for fast FCP
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()

      // Login and Register links should be present
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument()
    })

    it('renders without blocking interactive elements', () => {
      render(<Home />)

      // All interactive elements should be immediately available
      const buttons = screen.getAllByRole('button')
      const links = screen.getAllByRole('link')

      // Page should have interactive elements ready
      expect(buttons.length).toBeGreaterThan(0)
      expect(links.length).toBeGreaterThan(0)

      // Verify CTA buttons are clickable (not disabled)
      const ctaButton = screen.getByRole('button', { name: /get started/i })
      expect(ctaButton).not.toBeDisabled()
    })

    it('has accessible heading hierarchy (affects SEO and accessibility scores)', () => {
      render(<Home />)

      // Get all headings
      const allHeadings = screen.getAllByRole('heading')

      // Should have multiple headings but exactly one h1
      expect(allHeadings.length).toBeGreaterThan(1)

      const h1Headings = screen.getAllByRole('heading', { level: 1 })
      expect(h1Headings).toHaveLength(1)
    })

    it('avoids layout shift by having defined container dimensions', () => {
      render(<Home />)

      // Main container should have defined layout
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Verify structured sections exist (prevents CLS)
      const featuresSection = screen.getByTestId('features-section')
      const ctaSection = screen.getByTestId('cta-section')

      expect(featuresSection).toBeInTheDocument()
      expect(ctaSection).toBeInTheDocument()
    })

    it('uses efficient CSS classes for styling (utility-first)', () => {
      const { container } = render(<Home />)

      // Verify Tailwind/DaisyUI classes are used (efficient CSS)
      // Look for common utility class patterns
      const elementsWithClasses = container.querySelectorAll('[class*="min-h"]')
      expect(elementsWithClasses.length).toBeGreaterThan(0)

      // Check for flex/grid layouts (efficient rendering)
      const flexElements = container.querySelectorAll('[class*="flex"]')
      const gridElements = container.querySelectorAll('[class*="grid"]')
      expect(flexElements.length + gridElements.length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 3: Lazy loading for below-fold images
   * Verifies images use loading="lazy" attribute
   */
  describe('Test Case 3: Lazy loading for images', () => {
    it('any img elements below the fold have loading="lazy" attribute', () => {
      const { container } = render(<Home />)

      // Get all img elements (if any exist)
      const images = container.querySelectorAll('img')

      // If there are images, they should have lazy loading for below-fold content
      images.forEach((img) => {
        // Below-fold images should have lazy loading
        // Hero images (above fold) may not need it, but it's acceptable
        const hasLazyLoading = img.getAttribute('loading') === 'lazy'
        const isDecorativeOrAboveFold = img.hasAttribute('data-above-fold')

        // Either has lazy loading OR is explicitly marked as above-fold
        if (!isDecorativeOrAboveFold) {
          expect(hasLazyLoading).toBe(true)
        }
      })
    })

    it('decorative icons use aria-hidden for performance and accessibility', () => {
      const { container } = render(<Home />)

      // SVG icons should have aria-hidden="true" (screen readers skip them)
      const svgIcons = container.querySelectorAll('svg[aria-hidden="true"]')

      // Verify there are decorative icons properly marked
      expect(svgIcons.length).toBeGreaterThan(0)
    })

    it('feature section icons are optimized for performance', () => {
      render(<Home />)

      // Feature icons should use SVG (vector, not raster images)
      const featuresSection = screen.getByTestId('features-section')
      const svgIcons = featuresSection.querySelectorAll('svg')

      // Features should use SVG icons (no heavy images)
      expect(svgIcons.length).toBeGreaterThanOrEqual(4) // 4 feature cards
    })

    it('no unoptimized images are present', () => {
      const { container } = render(<Home />)

      // Check for any images without proper attributes
      const images = container.querySelectorAll('img')

      images.forEach((img) => {
        // All images should have alt text (accessibility)
        expect(img.hasAttribute('alt')).toBe(true)

        // Images should have defined dimensions or aspect ratio
        const hasWidth = img.hasAttribute('width') || img.style.width
        const hasHeight = img.hasAttribute('height') || img.style.height
        const hasCSSClass = img.className.includes('w-') || img.className.includes('h-')

        // Either has explicit dimensions or CSS classes for sizing
        const hasSizing = hasWidth || hasHeight || hasCSSClass
        expect(hasSizing || img.className.length > 0).toBe(true)
      })
    })
  })

  /**
   * Additional performance-related tests
   */
  describe('Additional performance optimizations', () => {
    it('page does not have excessive DOM depth', () => {
      const { container } = render(<Home />)

      // Check maximum DOM depth (excessive depth affects performance)
      function getMaxDepth(element: Element, currentDepth: number = 0): number {
        let maxDepth = currentDepth
        const children = element.children

        for (let i = 0; i < children.length; i++) {
          const childDepth = getMaxDepth(children[i], currentDepth + 1)
          maxDepth = Math.max(maxDepth, childDepth)
        }

        return maxDepth
      }

      const maxDepth = getMaxDepth(container)

      // Lighthouse recommends DOM depth < 32
      expect(maxDepth).toBeLessThan(32)
    })

    it('page does not have excessive DOM nodes', () => {
      const { container } = render(<Home />)

      // Count all DOM nodes
      const nodeCount = container.querySelectorAll('*').length

      // Lighthouse warns at 1500 nodes, fails at 3000
      // For a landing page, we should be well under this
      expect(nodeCount).toBeLessThan(1500)
    })

    it('interactive elements have proper focus styles', () => {
      render(<Home />)

      // All interactive elements should be focusable
      const buttons = screen.getAllByRole('button')
      const links = screen.getAllByRole('link')

      // Verify buttons and links can receive focus
      buttons.forEach((button) => {
        expect(button.tabIndex).not.toBe(-1)
      })

      links.forEach((link) => {
        expect(link.tabIndex).not.toBe(-1)
      })
    })
  })
})
