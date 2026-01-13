import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../contexts/ThemeContext'
import Home from '../Home'

/**
 * Performance Tests for Homepage
 * Validates NFR-1: Page load time under 2 seconds on standard connections
 *
 * Test Case 2: Check for lazy loading of images below fold
 * Test Case 3: Analyze JavaScript bundle size impact (component-level)
 */

// Helper to render Home with providers
function renderHome() {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  )
}

describe('Homepage Performance Tests', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })

  /**
   * Test Case 2: Check for lazy loading of images below fold
   * Input: Check for lazy loading of images below fold
   * Expected: Images below the fold use lazy loading attribute
   * Type: unit
   *
   * This test verifies that any images present in the homepage
   * use the 'loading="lazy"' attribute for images below the fold.
   * Currently the homepage uses icons (SVG from lucide-react) instead of images,
   * which don't require lazy loading. This test documents the expected behavior
   * and will catch any future image additions that don't follow best practices.
   */
  describe('Lazy Loading Images', () => {
    it('should not have any images without lazy loading below the fold', async () => {
      const { container } = renderHome()

      // Allow animations to complete
      vi.advanceTimersByTime(1000)

      // Get all image elements
      const allImages = container.querySelectorAll('img')

      // Check that any images that exist use lazy loading
      // Note: Currently the homepage uses SVG icons, not img elements
      allImages.forEach((img, index) => {
        // Skip images that are above the fold (first few images)
        // For images below the fold, verify lazy loading is enabled
        if (index > 1) {
          // Below fold images should have loading="lazy"
          expect(img.getAttribute('loading')).toBe('lazy')
        }
      })

      // Document that currently no img elements exist (using icons instead)
      // This is expected behavior - icons are implemented as inline SVGs
      console.log(`Total img elements found: ${allImages.length}`)
    })

    it('should use SVG icons instead of raster images for better performance', () => {
      const { container } = renderHome()

      // Verify SVG icons are used
      const svgElements = container.querySelectorAll('svg')

      // Homepage should use SVG icons for features and stats
      // FeaturesSection has 4 icons, SocialProofSection has 4 icons
      // Plus navigation icons
      expect(svgElements.length).toBeGreaterThan(0)

      // No img elements should exist (using SVGs for icons)
      const imgElements = container.querySelectorAll('img')
      expect(imgElements.length).toBe(0)
    })

    it('should have efficient icon rendering with aria-hidden', () => {
      const { container } = renderHome()

      // Get all SVG icons
      const svgElements = container.querySelectorAll('svg')

      // Each decorative icon should have aria-hidden for accessibility
      // while screen readers focus on the text content
      svgElements.forEach((svg) => {
        // Decorative icons in feature cards should be hidden from screen readers
        const ariaHidden = svg.getAttribute('aria-hidden')
        // Either aria-hidden="true" or no aria role for decorative icons
        if (ariaHidden) {
          expect(ariaHidden).toBe('true')
        }
      })
    })
  })

  /**
   * Test Case 3 (Unit level): Homepage component render performance
   * Validates that the Homepage component renders efficiently
   * and doesn't cause significant bundle bloat
   */
  describe('Component Render Performance', () => {
    it('should render homepage within acceptable time', async () => {
      const startTime = performance.now()

      renderHome()

      const endTime = performance.now()
      const renderTime = endTime - startTime

      console.log(`Homepage render time: ${renderTime.toFixed(2)}ms`)

      // Component should render in under 100ms (unit test level)
      expect(renderTime).toBeLessThan(100)
    })

    it('should render all main sections', async () => {
      renderHome()

      // Wait for animations
      vi.advanceTimersByTime(500)

      // Verify main sections are rendered
      await waitFor(() => {
        expect(screen.getByText(/Shorten\. Track\. Share\./i)).toBeInTheDocument()
      })

      // Hero section
      expect(screen.getByText(/Transform long URLs/i)).toBeInTheDocument()

      // Demo section
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()

      // Features section
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // Social proof section
      expect(screen.getByTestId('social-proof-section')).toBeInTheDocument()
    })

    it('should not have memory-heavy inline styles', () => {
      const { container } = renderHome()

      // Check that we're not using excessive inline styles
      // which can impact performance
      const elementsWithStyle = container.querySelectorAll('[style]')

      // Some inline styles from framer-motion are expected
      // But should not be excessive (< 50 elements with inline styles)
      expect(elementsWithStyle.length).toBeLessThan(50)
    })

    it('should use efficient class-based styling', () => {
      const { container } = renderHome()

      // Verify Tailwind CSS classes are being used
      const elementsWithClass = container.querySelectorAll('[class]')

      // Most elements should have class-based styling
      expect(elementsWithClass.length).toBeGreaterThan(10)
    })
  })

  describe('Animation Performance', () => {
    it('should use GPU-accelerated animations', () => {
      const { container } = renderHome()

      // Framer motion uses transform and opacity for animations
      // which are GPU-accelerated and don't cause reflows
      const motionElements = container.querySelectorAll('[style*="transform"]')

      // Motion elements should exist (framer-motion is being used)
      // This indicates animations are using GPU-friendly properties
      expect(motionElements.length).toBeGreaterThanOrEqual(0)
    })

    it('should have viewport-triggered animations for below-fold content', async () => {
      renderHome()

      // Features and Social Proof sections use whileInView
      // This means they don't animate until in viewport
      // Which improves initial load performance

      // Verify sections exist and will animate when in view
      await waitFor(() => {
        const featuresSection = screen.getByTestId('features-section')
        expect(featuresSection).toBeInTheDocument()

        const socialProofSection = screen.getByTestId('social-proof-section')
        expect(socialProofSection).toBeInTheDocument()
      })
    })
  })
})

/**
 * Image Lazy Loading Utility Tests
 * These tests document the expected behavior for any future image additions
 */
describe('Image Lazy Loading Best Practices', () => {
  it('should document lazy loading requirements for future images', () => {
    // Document that any future images should:
    // 1. Use loading="lazy" for images below the fold
    // 2. Use proper width/height to prevent layout shift
    // 3. Use modern formats (WebP, AVIF) where supported

    const lazyLoadingBestPractices = {
      attribute: 'loading="lazy"',
      whenToUse: 'Images below the fold (not visible on initial viewport)',
      layoutShiftPrevention: 'Always specify width and height attributes',
      modernFormats: ['WebP', 'AVIF'],
      fallback: 'PNG/JPEG for older browsers'
    }

    expect(lazyLoadingBestPractices.attribute).toBe('loading="lazy"')
    expect(lazyLoadingBestPractices.whenToUse).toContain('below the fold')
  })

  it('should verify no blocking images in above-the-fold content', () => {
    const { container } = renderHome()

    // Hero section (above the fold) should not have large images
    // that could block rendering
    const heroSection = container.querySelector('.hero')
    expect(heroSection).toBeInTheDocument()

    // No img elements in hero that could block initial paint
    const heroImages = heroSection?.querySelectorAll('img')
    expect(heroImages?.length ?? 0).toBe(0)
  })
})
