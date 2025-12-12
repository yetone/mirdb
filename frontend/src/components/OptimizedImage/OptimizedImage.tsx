import type { OptimizedImageProps } from '../../types/OptimizedImage.types'

/**
 * OptimizedImage component provides image optimization features including:
 * - Native lazy loading for below-fold images
 * - Modern format support (WebP, AVIF) with fallbacks
 * - Explicit width/height to prevent Cumulative Layout Shift (CLS)
 * - Responsive image support via srcSet and sizes
 *
 * NFR-3: Page shall achieve Cumulative Layout Shift (CLS) under 0.1
 * REQ-3: Homepage shall display featured content with engaging visuals
 */
export const OptimizedImage: React.FC<OptimizedImageProps> = ({
  src,
  alt,
  width,
  height,
  loading = 'lazy',
  webpSrc,
  avifSrc,
  className = '',
  sizes,
  srcSet,
  fetchPriority = 'auto',
  decoding = 'async',
  'data-testid': testId,
  onLoad,
  onError,
}) => {
  // Determine if we have modern format sources
  const hasModernFormats = webpSrc || avifSrc

  // Build the image element with all optimization attributes
  const imageElement = (
    <img
      src={src}
      alt={alt}
      width={width}
      height={height}
      loading={loading}
      className={`optimized-image ${className}`.trim()}
      sizes={sizes}
      srcSet={srcSet}
      fetchPriority={fetchPriority}
      decoding={decoding}
      data-testid={testId}
      onLoad={onLoad}
      onError={onError}
    />
  )

  // If we have modern format sources, wrap in picture element for format fallback
  if (hasModernFormats) {
    return (
      <picture data-testid={testId ? `${testId}-picture` : undefined}>
        {/* AVIF source - best compression, newer format */}
        {avifSrc && (
          <source
            srcSet={avifSrc}
            type="image/avif"
            data-testid={testId ? `${testId}-avif-source` : undefined}
          />
        )}
        {/* WebP source - widely supported modern format */}
        {webpSrc && (
          <source
            srcSet={webpSrc}
            type="image/webp"
            data-testid={testId ? `${testId}-webp-source` : undefined}
          />
        )}
        {/* Fallback img element for older browsers */}
        {imageElement}
      </picture>
    )
  }

  // Return just the image element if no modern formats
  return imageElement
}
