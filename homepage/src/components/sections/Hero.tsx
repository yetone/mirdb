/**
 * Hero section component with value proposition.
 * Owner: Scenario 2 - Hero Section Implementation
 *
 * Requirements:
 * - Compelling headline (1-2 sentences) (REQ-2)
 * - Supporting subheadline (2-3 sentences)
 * - Primary CTA button (high contrast)
 * - Optional hero image/illustration
 * - Full-width or centered layout
 */

import type { CTAButton } from '../../types'

export interface HeroProps {
  headline: string
  subheadline: string
  ctaButton: CTAButton
  backgroundImage?: string
  heroImage?: string
}

export function Hero({
  headline,
  subheadline,
  ctaButton,
  backgroundImage,
  heroImage,
}: HeroProps) {
  const buttonSizeClasses = {
    sm: 'px-4 py-2 text-sm',
    md: 'px-6 py-3 text-base',
    lg: 'px-8 py-4 text-lg',
  }

  const buttonVariantClasses = {
    primary:
      'bg-primary-600 text-white hover:bg-primary-700 focus:ring-primary-500',
    secondary:
      'bg-secondary-600 text-white hover:bg-secondary-700 focus:ring-secondary-500',
    outline:
      'bg-transparent border-2 border-primary-600 text-primary-600 hover:bg-primary-50 focus:ring-primary-500',
  }

  const backgroundStyle = backgroundImage
    ? {
        backgroundImage: `url(${backgroundImage})`,
        backgroundSize: 'cover',
        backgroundPosition: 'center',
      }
    : undefined

  return (
    <section
      className="relative min-h-[70vh] flex items-center justify-center bg-gradient-to-br from-primary-50 via-white to-secondary-50"
      style={backgroundStyle}
      aria-labelledby="hero-headline"
      data-testid="hero-section"
    >
      {backgroundImage && (
        <div className="absolute inset-0 bg-black/30" aria-hidden="true" />
      )}

      <div className="container-main relative z-10 py-16 md:py-24 lg:py-32">
        <div className="max-w-4xl mx-auto text-center">
          <h1
            id="hero-headline"
            className="text-4xl md:text-5xl lg:text-6xl font-bold text-secondary-900 mb-6 leading-tight"
            data-testid="hero-headline"
          >
            {headline}
          </h1>

          <p
            className="text-lg md:text-xl lg:text-2xl text-secondary-600 mb-10 max-w-2xl mx-auto leading-relaxed"
            data-testid="hero-subheadline"
          >
            {subheadline}
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <a
              href={ctaButton.href}
              className={`
                inline-flex items-center justify-center
                font-semibold rounded-lg
                transition-all duration-200 ease-in-out
                transform hover:scale-105 hover:shadow-lg
                focus:outline-none focus:ring-4 focus:ring-offset-2
                ${buttonSizeClasses[ctaButton.size]}
                ${buttonVariantClasses[ctaButton.variant]}
              `}
              data-testid="hero-cta-button"
              role="button"
              aria-label={ctaButton.label}
            >
              {ctaButton.label}
            </a>
          </div>

          {heroImage && (
            <div className="mt-12 md:mt-16">
              <img
                src={heroImage}
                alt="Product illustration"
                className="mx-auto max-w-full h-auto rounded-xl shadow-2xl"
                data-testid="hero-image"
                loading="eager"
              />
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
