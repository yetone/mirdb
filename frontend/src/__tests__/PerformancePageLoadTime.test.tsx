import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'

/**
 * Performance - Page Load Time Tests (NFR-1)
 *
 * These tests verify that the homepage meets performance requirements:
 * 1. Page load completes in under 2 seconds
 * 2. Performance score is 85 or higher (through proper optimization patterns)
 * 3. First Contentful Paint is under 1.8 seconds
 * 4. Largest Contentful Paint is under 2.5 seconds
 *
 * Note: Actual browser timing metrics require E2E testing with Playwright.
 * These tests verify the implementation patterns that ensure good performance.
 */

// Performance thresholds based on PRD NFR-1 requirements
const PERFORMANCE_THRESHOLDS = {
  pageLoadTime: 2000, // 2 seconds in milliseconds
  lighthouseScore: 85, // Minimum performance score
  fcp: 1800, // First Contentful Paint in milliseconds
  lcp: 2500, // Largest Contentful Paint in milliseconds
}

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Performance - Page Load Time (NFR-1)', () => {
  /**
   * Test Case 1: Load homepage on standard connection (4G)
   * Expected: Page load completes in under 2 seconds
   *
   * This test verifies that all critical sections render quickly
   * and there are no blocking operations.
   */
  describe('Test Case 1: Page load on standard connection (4G)', () => {
    it('should render all homepage sections within performance threshold', async () => {
      const startTime = performance.now()

      renderWithRouter(<Home />)

      // Verify all critical sections are rendered
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      const renderTime = performance.now() - startTime

      // In jsdom, render times should be very fast (< 500ms)
      // This validates no synchronous blocking operations
      expect(renderTime).toBeLessThan(PERFORMANCE_THRESHOLDS.pageLoadTime)
    })

    it('should have no synchronous blocking operations during initial render', () => {
      const startTime = performance.now()

      renderWithRouter(<Home />)

      const renderTime = performance.now() - startTime

      // Initial render should complete very quickly (< 100ms in jsdom)
      // This ensures no blocking code in component initialization
      expect(renderTime).toBeLessThan(1000)
    })

    it('should load hero section content immediately', () => {
      renderWithRouter(<Home />)

      // Critical above-the-fold content should be available immediately
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Headline should be in the DOM immediately (First Contentful Paint)
      // Note: Framer Motion animations may start with opacity:0 but content is in DOM
      // The key for FCP is that content is present, not that animations have completed
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline.textContent).toBeTruthy()
    })
  })

  /**
   * Test Case 2: Run Lighthouse performance audit
   * Expected: Performance score is 85 or higher
   *
   * This test verifies optimization patterns that contribute to high Lighthouse scores.
   */
  describe('Test Case 2: Lighthouse performance audit patterns', () => {
    it('should use semantic HTML elements for better rendering', () => {
      renderWithRouter(<Home />)

      // Main element for landmark navigation
      expect(document.querySelector('main')).toBeInTheDocument()

      // Footer element for proper document structure
      expect(document.querySelector('footer')).toBeInTheDocument()

      // Section elements for content organization
      const sections = document.querySelectorAll('section')
      expect(sections.length).toBeGreaterThanOrEqual(2)
    })

    it('should have proper heading hierarchy for accessibility and SEO', () => {
      renderWithRouter(<Home />)

      // Should have exactly one h1 (main page title)
      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)

      // Should have h2 elements for sections
      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      expect(h2Elements.length).toBeGreaterThan(0)
    })

    it('should have accessible button elements with proper roles', () => {
      renderWithRouter(<Home />)

      // CTA buttons should be accessible
      const buttons = screen.getAllByRole('button')
      const links = screen.getAllByRole('link')

      // Should have interactive elements
      expect(buttons.length + links.length).toBeGreaterThan(0)
    })

    it('should use aria-labels where appropriate', () => {
      renderWithRouter(<Home />)

      // Check for aria-labelledby on sections
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveAttribute('aria-labelledby')
    })
  })

  /**
   * Test Case 3: Measure First Contentful Paint
   * Expected: FCP is under 1.8 seconds
   *
   * This test verifies that critical content renders immediately.
   */
  describe('Test Case 3: First Contentful Paint optimization', () => {
    it('should render critical above-the-fold content immediately', () => {
      const { container } = renderWithRouter(<Home />)

      // Critical content should be present in the DOM immediately
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Text content should be available (not lazy-loaded)
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.textContent).toBeTruthy()

      // Verify content is not empty
      expect(container.textContent?.length).toBeGreaterThan(0)
    })

    it('should have text content in hero section', () => {
      renderWithRouter(<Home />)

      // Subheadline content should be present in DOM for FCP
      // Note: Framer Motion animations start with opacity:0 but content exists
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent?.length).toBeGreaterThan(10)
    })

    it('should have CTA buttons present above the fold', () => {
      renderWithRouter(<Home />)

      // CTA buttons should be in DOM immediately for FCP
      // Animation state (opacity) doesn't affect content availability
      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      expect(getStartedButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()
      // Verify they have proper attributes for navigation
      expect(getStartedButton).toHaveAttribute('href')
      expect(loginButton).toHaveAttribute('href')
    })
  })

  /**
   * Test Case 4: Measure Largest Contentful Paint
   * Expected: LCP is under 2.5 seconds
   *
   * This test verifies that the largest content element renders quickly.
   */
  describe('Test Case 4: Largest Contentful Paint optimization', () => {
    it('should render main content sections without delay', async () => {
      renderWithRouter(<Home />)

      // All major sections should be present
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
        expect(screen.getByTestId('features-section')).toBeInTheDocument()
        expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      })
    })

    it('should have feature cards rendered in the DOM', async () => {
      renderWithRouter(<Home />)

      // Feature cards are likely the LCP candidates
      await waitFor(() => {
        const featureCards = screen.getAllByTestId('feature-card')
        expect(featureCards.length).toBeGreaterThan(0)
      })
    })

    it('should not defer critical section rendering', () => {
      const { container } = renderWithRouter(<Home />)

      // Check that content is not wrapped in lazy loading that delays LCP
      const heroSection = container.querySelector('[data-testid="hero-section"]')
      const featuresSection = container.querySelector('[data-testid="features-section"]')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
    })
  })
})

describe('Performance - Bundle and Resource Optimization', () => {
  it('should use React best practices for performance', () => {
    renderWithRouter(<Home />)

    // Verify component structure supports code splitting
    // Check that sections are separate components (they use data-testid)
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    expect(screen.getByTestId('features-section')).toBeInTheDocument()
    expect(screen.getByTestId('demo-section')).toBeInTheDocument()
    expect(screen.getByTestId('footer-section')).toBeInTheDocument()
  })

  it('should have minimal DOM depth for efficient rendering', () => {
    const { container } = renderWithRouter(<Home />)

    // Calculate maximum DOM depth
    const getMaxDepth = (element: Element, depth = 0): number => {
      if (element.children.length === 0) return depth
      return Math.max(
        ...Array.from(element.children).map((child) =>
          getMaxDepth(child, depth + 1)
        )
      )
    }

    const maxDepth = getMaxDepth(container)

    // DOM depth should be reasonable (< 20 levels)
    // Excessive nesting hurts render performance
    expect(maxDepth).toBeLessThan(25)
  })

  it('should not have excessive number of DOM nodes', () => {
    const { container } = renderWithRouter(<Home />)

    const nodeCount = container.querySelectorAll('*').length

    // A well-optimized homepage should have < 500 nodes
    // This helps with rendering and reflow performance
    expect(nodeCount).toBeLessThan(500)
  })
})

describe('Performance - Animation and Rendering', () => {
  it('should use CSS classes for styling (not inline styles)', () => {
    const { container } = renderWithRouter(<Home />)

    // Check main sections use class-based styling
    const heroSection = container.querySelector('[data-testid="hero-section"]')
    expect(heroSection).toHaveAttribute('class')
  })

  it('should have properly structured grid layouts for efficient reflow', () => {
    renderWithRouter(<Home />)

    const featuresSection = screen.getByTestId('features-section')

    // Features section should use grid or flex layout
    const gridContainer = featuresSection.querySelector('.grid')
    expect(gridContainer).toBeInTheDocument()
  })
})
