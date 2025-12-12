import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { OptimizedImage } from './OptimizedImage'

/**
 * Test Suite for Image Optimization and Lazy Loading
 * Scenario: Verify images are optimized and lazy loaded for performance
 *
 * Test Cases:
 * 1. Below-fold images have loading='lazy' attribute
 * 2. Images use modern formats (WebP, AVIF) with fallbacks
 * 3. All images have explicit dimensions to prevent CLS
 */

describe('OptimizedImage Component - Lazy Loading', () => {
  /**
   * Test Case 1: Check image loading attribute on below-fold images
   * Expected: Below-fold images have loading='lazy' attribute
   */
  describe('Test Case 1: Lazy Loading Attribute', () => {
    it('renders with loading="lazy" attribute by default for below-fold images', () => {
      render(
        <OptimizedImage
          src="/images/below-fold.jpg"
          alt="Below fold image"
          width={800}
          height={600}
          data-testid="lazy-image"
        />
      )

      const image = screen.getByTestId('lazy-image')
      expect(image).toHaveAttribute('loading', 'lazy')
    })

    it('renders with loading="eager" when explicitly set for above-fold images', () => {
      render(
        <OptimizedImage
          src="/images/hero.jpg"
          alt="Hero image"
          width={1920}
          height={1080}
          loading="eager"
          data-testid="eager-image"
        />
      )

      const image = screen.getByTestId('eager-image')
      expect(image).toHaveAttribute('loading', 'eager')
    })

    it('below-fold images default to lazy loading strategy', () => {
      render(
        <OptimizedImage
          src="/images/content.jpg"
          alt="Content image"
          width={400}
          height={300}
          data-testid="default-loading"
        />
      )

      const image = screen.getByTestId('default-loading')
      // Default should be 'lazy' for below-fold content
      expect(image).toHaveAttribute('loading', 'lazy')
    })

    it('hero images can use eager loading', () => {
      render(
        <OptimizedImage
          src="/images/hero-banner.jpg"
          alt="Hero banner"
          width={1920}
          height={600}
          loading="eager"
          fetchPriority="high"
          data-testid="hero-image"
        />
      )

      const image = screen.getByTestId('hero-image')
      expect(image).toHaveAttribute('loading', 'eager')
      expect(image).toHaveAttribute('fetchPriority', 'high')
    })
  })

  /**
   * Test Case 3: Check image format optimization
   * Expected: Images use modern formats (WebP, AVIF) with fallbacks
   */
  describe('Test Case 3: Modern Format Optimization', () => {
    it('renders with WebP source when webpSrc is provided', () => {
      render(
        <OptimizedImage
          src="/images/fallback.jpg"
          alt="Optimized image"
          width={800}
          height={600}
          webpSrc="/images/optimized.webp"
          data-testid="webp-image"
        />
      )

      const webpSource = screen.getByTestId('webp-image-webp-source')
      expect(webpSource).toBeInTheDocument()
      expect(webpSource).toHaveAttribute('type', 'image/webp')
      expect(webpSource).toHaveAttribute('srcSet', '/images/optimized.webp')
    })

    it('renders with AVIF source when avifSrc is provided', () => {
      render(
        <OptimizedImage
          src="/images/fallback.jpg"
          alt="Optimized image"
          width={800}
          height={600}
          avifSrc="/images/optimized.avif"
          data-testid="avif-image"
        />
      )

      const avifSource = screen.getByTestId('avif-image-avif-source')
      expect(avifSource).toBeInTheDocument()
      expect(avifSource).toHaveAttribute('type', 'image/avif')
      expect(avifSource).toHaveAttribute('srcSet', '/images/optimized.avif')
    })

    it('renders picture element with both WebP and AVIF sources', () => {
      render(
        <OptimizedImage
          src="/images/fallback.jpg"
          alt="Multi-format image"
          width={800}
          height={600}
          webpSrc="/images/image.webp"
          avifSrc="/images/image.avif"
          data-testid="multi-format"
        />
      )

      // Should have picture element containing both sources
      const picture = screen.getByTestId('multi-format-picture')
      expect(picture).toBeInTheDocument()
      expect(picture.tagName.toLowerCase()).toBe('picture')

      const avifSource = screen.getByTestId('multi-format-avif-source')
      const webpSource = screen.getByTestId('multi-format-webp-source')

      expect(avifSource).toBeInTheDocument()
      expect(webpSource).toBeInTheDocument()
    })

    it('provides fallback image for browsers without modern format support', () => {
      render(
        <OptimizedImage
          src="/images/fallback.jpg"
          alt="Fallback image"
          width={800}
          height={600}
          webpSrc="/images/image.webp"
          avifSrc="/images/image.avif"
          data-testid="fallback-test"
        />
      )

      // The img element should have the fallback src
      const image = screen.getByTestId('fallback-test')
      expect(image).toHaveAttribute('src', '/images/fallback.jpg')
    })

    it('AVIF source comes before WebP for optimal compression selection', () => {
      render(
        <OptimizedImage
          src="/images/fallback.jpg"
          alt="Format order test"
          width={800}
          height={600}
          webpSrc="/images/image.webp"
          avifSrc="/images/image.avif"
          data-testid="format-order"
        />
      )

      const picture = screen.getByTestId('format-order-picture')
      const sources = picture.querySelectorAll('source')

      // AVIF should be first (better compression)
      expect(sources[0]).toHaveAttribute('type', 'image/avif')
      // WebP should be second
      expect(sources[1]).toHaveAttribute('type', 'image/webp')
    })

    it('renders without picture element when no modern formats provided', () => {
      render(
        <OptimizedImage
          src="/images/basic.jpg"
          alt="Basic image"
          width={800}
          height={600}
          data-testid="no-picture"
        />
      )

      // Should render just img without picture wrapper
      const image = screen.getByTestId('no-picture')
      expect(image.tagName.toLowerCase()).toBe('img')
      expect(screen.queryByTestId('no-picture-picture')).not.toBeInTheDocument()
    })
  })

  /**
   * Test Case 4: Verify images have width and height attributes
   * Expected: All images have explicit dimensions to prevent CLS
   */
  describe('Test Case 4: Image Dimensions for CLS Prevention', () => {
    it('renders with explicit width attribute', () => {
      render(
        <OptimizedImage
          src="/images/test.jpg"
          alt="Test image"
          width={800}
          height={600}
          data-testid="width-test"
        />
      )

      const image = screen.getByTestId('width-test')
      expect(image).toHaveAttribute('width', '800')
    })

    it('renders with explicit height attribute', () => {
      render(
        <OptimizedImage
          src="/images/test.jpg"
          alt="Test image"
          width={800}
          height={600}
          data-testid="height-test"
        />
      )

      const image = screen.getByTestId('height-test')
      expect(image).toHaveAttribute('height', '600')
    })

    it('renders with both width and height to prevent CLS', () => {
      render(
        <OptimizedImage
          src="/images/featured.jpg"
          alt="Featured content image"
          width={1200}
          height={675}
          data-testid="dimensions-test"
        />
      )

      const image = screen.getByTestId('dimensions-test')
      expect(image).toHaveAttribute('width', '1200')
      expect(image).toHaveAttribute('height', '675')
    })

    it('supports various aspect ratios with explicit dimensions', () => {
      const { rerender } = render(
        <OptimizedImage
          src="/images/16x9.jpg"
          alt="16:9 image"
          width={1920}
          height={1080}
          data-testid="aspect-test"
        />
      )

      let image = screen.getByTestId('aspect-test')
      expect(image).toHaveAttribute('width', '1920')
      expect(image).toHaveAttribute('height', '1080')

      // Test 4:3 aspect ratio
      rerender(
        <OptimizedImage
          src="/images/4x3.jpg"
          alt="4:3 image"
          width={800}
          height={600}
          data-testid="aspect-test"
        />
      )

      image = screen.getByTestId('aspect-test')
      expect(image).toHaveAttribute('width', '800')
      expect(image).toHaveAttribute('height', '600')

      // Test square aspect ratio
      rerender(
        <OptimizedImage
          src="/images/square.jpg"
          alt="Square image"
          width={500}
          height={500}
          data-testid="aspect-test"
        />
      )

      image = screen.getByTestId('aspect-test')
      expect(image).toHaveAttribute('width', '500')
      expect(image).toHaveAttribute('height', '500')
    })
  })

  /**
   * Additional Tests: Accessibility and Performance Attributes
   */
  describe('Accessibility and Performance', () => {
    it('renders with alt text for accessibility', () => {
      render(
        <OptimizedImage
          src="/images/accessible.jpg"
          alt="Descriptive alt text for screen readers"
          width={800}
          height={600}
          data-testid="alt-test"
        />
      )

      const image = screen.getByTestId('alt-test')
      expect(image).toHaveAttribute('alt', 'Descriptive alt text for screen readers')
    })

    it('supports async decoding for performance', () => {
      render(
        <OptimizedImage
          src="/images/async.jpg"
          alt="Async decoded image"
          width={800}
          height={600}
          decoding="async"
          data-testid="decoding-test"
        />
      )

      const image = screen.getByTestId('decoding-test')
      expect(image).toHaveAttribute('decoding', 'async')
    })

    it('defaults to async decoding', () => {
      render(
        <OptimizedImage
          src="/images/default-decode.jpg"
          alt="Default decoding"
          width={800}
          height={600}
          data-testid="default-decoding"
        />
      )

      const image = screen.getByTestId('default-decoding')
      expect(image).toHaveAttribute('decoding', 'async')
    })

    it('supports fetchPriority for LCP images', () => {
      render(
        <OptimizedImage
          src="/images/lcp.jpg"
          alt="LCP image"
          width={1920}
          height={1080}
          loading="eager"
          fetchPriority="high"
          data-testid="priority-test"
        />
      )

      const image = screen.getByTestId('priority-test')
      expect(image).toHaveAttribute('fetchPriority', 'high')
    })

    it('supports className for styling', () => {
      render(
        <OptimizedImage
          src="/images/styled.jpg"
          alt="Styled image"
          width={800}
          height={600}
          className="custom-image-class"
          data-testid="class-test"
        />
      )

      const image = screen.getByTestId('class-test')
      expect(image).toHaveClass('optimized-image')
      expect(image).toHaveClass('custom-image-class')
    })

    it('supports sizes attribute for responsive images', () => {
      render(
        <OptimizedImage
          src="/images/responsive.jpg"
          alt="Responsive image"
          width={800}
          height={600}
          sizes="(max-width: 600px) 100vw, 50vw"
          data-testid="sizes-test"
        />
      )

      const image = screen.getByTestId('sizes-test')
      expect(image).toHaveAttribute('sizes', '(max-width: 600px) 100vw, 50vw')
    })

    it('supports srcSet for responsive images', () => {
      render(
        <OptimizedImage
          src="/images/responsive.jpg"
          alt="Responsive image"
          width={800}
          height={600}
          srcSet="/images/responsive-400.jpg 400w, /images/responsive-800.jpg 800w"
          data-testid="srcset-test"
        />
      )

      const image = screen.getByTestId('srcset-test')
      expect(image).toHaveAttribute('srcSet', '/images/responsive-400.jpg 400w, /images/responsive-800.jpg 800w')
    })
  })

  /**
   * Event Handler Tests
   */
  describe('Event Handlers', () => {
    it('calls onLoad callback when image loads', () => {
      const handleLoad = vi.fn()

      render(
        <OptimizedImage
          src="/images/load-test.jpg"
          alt="Load test"
          width={800}
          height={600}
          onLoad={handleLoad}
          data-testid="load-event"
        />
      )

      const image = screen.getByTestId('load-event')
      fireEvent.load(image)

      expect(handleLoad).toHaveBeenCalledTimes(1)
    })

    it('calls onError callback when image fails to load', () => {
      const handleError = vi.fn()

      render(
        <OptimizedImage
          src="/images/error-test.jpg"
          alt="Error test"
          width={800}
          height={600}
          onError={handleError}
          data-testid="error-event"
        />
      )

      const image = screen.getByTestId('error-event')
      fireEvent.error(image)

      expect(handleError).toHaveBeenCalledTimes(1)
    })
  })
})

/**
 * Integration Tests: OptimizedImage with Picture Element
 */
describe('OptimizedImage Integration - Modern Formats with Lazy Loading', () => {
  it('combines lazy loading with modern formats correctly', () => {
    render(
      <OptimizedImage
        src="/images/fallback.jpg"
        alt="Optimized lazy image"
        width={800}
        height={600}
        loading="lazy"
        webpSrc="/images/image.webp"
        avifSrc="/images/image.avif"
        data-testid="combined-test"
      />
    )

    // Image should have lazy loading
    const image = screen.getByTestId('combined-test')
    expect(image).toHaveAttribute('loading', 'lazy')

    // Should also have modern format sources
    expect(screen.getByTestId('combined-test-avif-source')).toBeInTheDocument()
    expect(screen.getByTestId('combined-test-webp-source')).toBeInTheDocument()

    // And explicit dimensions
    expect(image).toHaveAttribute('width', '800')
    expect(image).toHaveAttribute('height', '600')
  })

  it('eager loaded hero images work with modern formats', () => {
    render(
      <OptimizedImage
        src="/images/hero-fallback.jpg"
        alt="Hero image"
        width={1920}
        height={1080}
        loading="eager"
        fetchPriority="high"
        webpSrc="/images/hero.webp"
        avifSrc="/images/hero.avif"
        data-testid="hero-optimized"
      />
    )

    const image = screen.getByTestId('hero-optimized')

    // Hero should load eagerly with high priority
    expect(image).toHaveAttribute('loading', 'eager')
    expect(image).toHaveAttribute('fetchPriority', 'high')

    // But still have modern format support
    expect(screen.getByTestId('hero-optimized-avif-source')).toBeInTheDocument()
    expect(screen.getByTestId('hero-optimized-webp-source')).toBeInTheDocument()
  })
})
