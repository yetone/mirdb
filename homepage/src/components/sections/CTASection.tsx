/**
 * Secondary CTA section component.
 * Owner: Scenario 5 - Secondary CTAs Implementation
 *
 * Requirements:
 * - Secondary CTAs throughout page (REQ-5)
 * - "Learn More", "Contact Sales" buttons
 * - Visible hover, focus, active states (NFR-5)
 * - WCAG 2.1 AA color contrast (NFR-6)
 */

import { Button } from '../common/Button'
import type { CTAButton } from '../../types'

export interface CTASectionProps {
  title?: string
  description?: string
  primaryCTA: CTAButton
  secondaryCTA?: CTAButton
  id?: string
}

export function CTASection({
  title = "Ready to Get Started?",
  description = "Join thousands of teams already using our platform to transform their workflow.",
  primaryCTA,
  secondaryCTA,
  id = "cta-section",
}: CTASectionProps) {
  return (
    <section
      id={id}
      className="py-16 md:py-24 bg-gradient-to-br from-primary-50 via-white to-secondary-50"
      aria-labelledby={`${id}-title`}
      data-testid="cta-section"
    >
      <div className="container-main">
        <div className="max-w-3xl mx-auto text-center">
          {title && (
            <h2
              id={`${id}-title`}
              className="text-3xl md:text-4xl lg:text-5xl font-bold text-secondary-900 mb-6"
              data-testid="cta-section-title"
            >
              {title}
            </h2>
          )}

          {description && (
            <p
              className="text-lg md:text-xl text-secondary-600 mb-10 max-w-2xl mx-auto leading-relaxed"
              data-testid="cta-section-description"
            >
              {description}
            </p>
          )}

          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <Button
              href={primaryCTA.href}
              variant={primaryCTA.variant}
              size={primaryCTA.size}
              aria-label={primaryCTA.label}
              data-testid="cta-primary-button"
            >
              {primaryCTA.label}
            </Button>

            {secondaryCTA && (
              <Button
                href={secondaryCTA.href}
                variant={secondaryCTA.variant}
                size={secondaryCTA.size}
                aria-label={secondaryCTA.label}
                data-testid="cta-secondary-button"
              >
                {secondaryCTA.label}
              </Button>
            )}
          </div>
        </div>
      </div>
    </section>
  )
}

/**
 * Inline CTA component for placement within other sections
 * Used for "Learn More" and other contextual CTAs
 */
export interface InlineCTAProps {
  label: string
  href: string
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
}

export function InlineCTA({
  label,
  href,
  variant = 'outline',
  size = 'md',
}: InlineCTAProps) {
  return (
    <Button
      href={href}
      variant={variant}
      size={size}
      aria-label={label}
      data-testid="inline-cta"
    >
      {label}
    </Button>
  )
}

/**
 * Contact Sales CTA component
 * Used for sales-oriented CTAs throughout the page
 */
export interface ContactSalesCTAProps {
  label?: string
  href?: string
  size?: 'sm' | 'md' | 'lg'
}

export function ContactSalesCTA({
  label = "Contact Sales",
  href = "#contact",
  size = 'md',
}: ContactSalesCTAProps) {
  return (
    <Button
      href={href}
      variant="secondary"
      size={size}
      aria-label={label}
      data-testid="contact-sales-cta"
    >
      {label}
    </Button>
  )
}

/**
 * Learn More CTA component
 * Used for informational CTAs throughout the page
 */
export interface LearnMoreCTAProps {
  label?: string
  href?: string
  size?: 'sm' | 'md' | 'lg'
}

export function LearnMoreCTA({
  label = "Learn More",
  href = "#features",
  size = 'md',
}: LearnMoreCTAProps) {
  return (
    <Button
      href={href}
      variant="outline"
      size={size}
      aria-label={label}
      data-testid="learn-more-cta"
    >
      {label}
    </Button>
  )
}
