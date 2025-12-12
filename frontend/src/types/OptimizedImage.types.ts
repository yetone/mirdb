/**
 * Type definitions for OptimizedImage component
 * Supports image optimization, lazy loading, and modern formats
 */

/**
 * Supported image formats for optimization
 */
export type ImageFormat = 'webp' | 'avif' | 'jpeg' | 'png' | 'jpg'

/**
 * Image source configuration for a specific format
 */
export interface ImageSource {
  /** Source URL for this format */
  srcSet: string
  /** MIME type for the image format */
  type: `image/${ImageFormat}`
}

/**
 * Loading strategy for images
 * - 'lazy': Below-fold images that load when approaching viewport
 * - 'eager': Above-fold/hero images that load immediately
 */
export type LoadingStrategy = 'lazy' | 'eager'

/**
 * Props for the OptimizedImage component
 */
export interface OptimizedImageProps {
  /** Primary image source URL (fallback for browsers without modern format support) */
  src: string
  /** Alternative text for accessibility */
  alt: string
  /** Image width in pixels - required for preventing CLS */
  width: number
  /** Image height in pixels - required for preventing CLS */
  height: number
  /** Loading strategy - defaults to 'lazy' for below-fold images */
  loading?: LoadingStrategy
  /** WebP format source URL for optimization */
  webpSrc?: string
  /** AVIF format source URL for optimization */
  avifSrc?: string
  /** Additional CSS class names */
  className?: string
  /** Sizes attribute for responsive images */
  sizes?: string
  /** Source set for responsive images */
  srcSet?: string
  /** Priority loading hint (for LCP images) */
  fetchPriority?: 'high' | 'low' | 'auto'
  /** Decoding hint for the browser */
  decoding?: 'async' | 'sync' | 'auto'
  /** Test ID for testing purposes */
  'data-testid'?: string
  /** Callback when image loads successfully */
  onLoad?: () => void
  /** Callback when image fails to load */
  onError?: () => void
}

/**
 * Props for image container with aspect ratio preservation
 */
export interface ImageContainerProps {
  /** Aspect ratio as width/height (e.g., 16/9, 4/3) */
  aspectRatio?: number
  /** Children elements (typically the OptimizedImage) */
  children: React.ReactNode
  /** Additional CSS class names */
  className?: string
}
