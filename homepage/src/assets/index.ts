/**
 * Asset utilities for performance optimization.
 * Owner: Scenario 9 - Performance Optimization
 *
 * Provides:
 * - Image component with lazy loading support
 * - WebP format detection and fallback
 * - Image preloading utilities
 */

/**
 * Checks if the browser supports WebP format
 * Uses feature detection to determine WebP support
 */
export function supportsWebP(): boolean {
  if (typeof window === 'undefined') return false

  try {
    const canvas = document.createElement('canvas')
    canvas.width = 1
    canvas.height = 1
    const dataUrl = canvas.toDataURL('image/webp')
    // toDataURL returns null in JSDOM/test environments
    if (!dataUrl) return false
    return dataUrl.indexOf('data:image/webp') === 0
  } catch {
    return false
  }
}

/**
 * Get the optimal image source with WebP fallback
 * @param webpSrc - WebP image source
 * @param fallbackSrc - Fallback image source (JPEG/PNG)
 * @returns The optimal image source based on browser support
 */
export function getOptimalImageSrc(webpSrc: string, fallbackSrc: string): string {
  if (typeof window === 'undefined') return fallbackSrc
  return supportsWebP() ? webpSrc : fallbackSrc
}

/**
 * Preload critical images for better LCP
 * @param imageUrls - Array of image URLs to preload
 */
export function preloadImages(imageUrls: string[]): void {
  if (typeof window === 'undefined') return

  imageUrls.forEach((url) => {
    const link = document.createElement('link')
    link.rel = 'preload'
    link.as = 'image'
    link.href = url
    document.head.appendChild(link)
  })
}

/**
 * Props for optimized image component
 */
export interface OptimizedImageProps {
  src: string
  alt: string
  webpSrc?: string
  width?: number
  height?: number
  loading?: 'lazy' | 'eager'
  className?: string
  priority?: boolean
}

/**
 * Image loading strategies
 */
export const ImageLoadingStrategy = {
  /**
   * Lazy load - for images below the fold
   */
  LAZY: 'lazy' as const,
  /**
   * Eager load - for images above the fold (LCP candidates)
   */
  EAGER: 'eager' as const,
}

/**
 * Performance thresholds based on NFR requirements
 */
export const PerformanceThresholds = {
  /**
   * Maximum page load time in ms (NFR-1: 2 seconds on 3G)
   */
  MAX_LOAD_TIME_MS: 2000,
  /**
   * Maximum First Contentful Paint in ms
   */
  MAX_FCP_MS: 1500,
  /**
   * Maximum Largest Contentful Paint in ms
   */
  MAX_LCP_MS: 2500,
  /**
   * Minimum Lighthouse performance score
   */
  MIN_LIGHTHOUSE_SCORE: 80,
}

/**
 * Font loading strategies
 */
export const FontDisplayStrategy = {
  /**
   * Swap - shows fallback font immediately, swaps when custom font loads
   */
  SWAP: 'swap',
  /**
   * Optional - brief block period, then uses fallback if font not loaded
   */
  OPTIONAL: 'optional',
}
