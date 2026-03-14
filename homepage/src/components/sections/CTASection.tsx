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
  background?: 'light' | 'dark' | 'gradient'
}

/**
 * Background variant styles.
 * All text colors meet WCAG 2.1 AA contrast requirements.
 */
const backgroundClasses = {
  light: 'bg-white',
  dark: 'bg-secondary-900',
  gradient: 'bg-gradient-to-r from-primary-600 to-primary-700',
}

const textClasses = {
  light: {
    title: 'text-secondary-900',
    description: 'text-secondary-600',
  },
  dark: {
    title: 'text-white',
    description: 'text-secondary-200',
  },
  gradient: {
    title: 'text-white',
    description: 'text-primary-100',
  },
}

export function CTASection({
  title = 'Ready to Get Started?',
  description = "Join thousands of teams already using our platform. Start your free trial today and transform the way you work.",
  primaryCTA,
  secondaryCTA,
  background = 'gradient',
}: CTASectionProps) {
  const bgClass = backgroundClasses[background]
  const textClass = textClasses[background]

  return (
    <section
      className={`py-16 md:py-24 ${bgClass}`}
      aria-labelledby="cta-section-title"
      data-testid="cta-section"
    >
      <div className="container-main">
        <div className="max-w-3xl mx-auto text-center">
          <h2
            id="cta-section-title"
            className={`text-3xl md:text-4xl lg:text-5xl font-bold mb-6 ${textClass.title}`}
            data-testid="cta-section-title"
          >
            {title}
          </h2>

          <p
            className={`text-lg md:text-xl mb-10 max-w-2xl mx-auto leading-relaxed ${textClass.description}`}
            data-testid="cta-section-description"
          >
            {description}
          </p>

          <div
            className="flex flex-col sm:flex-row gap-4 justify-center items-center"
            data-testid="cta-buttons-container"
          >
            <Button
              href={primaryCTA.href}
              variant={background === 'light' ? primaryCTA.variant : 'white'}
              size={primaryCTA.size}
              data-testid="cta-primary-button"
              aria-label={primaryCTA.label}
            >
              {primaryCTA.label}
            </Button>

            {secondaryCTA && (
              <Button
                href={secondaryCTA.href}
                variant={background === 'light' ? secondaryCTA.variant : 'outline-white'}
                size={secondaryCTA.size}
                data-testid="cta-secondary-button"
                aria-label={secondaryCTA.label}
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
 * Inline CTA component for use within other sections.
 * Useful for placing CTAs after features, testimonials, etc.
 */
export interface InlineCTAProps {
  text: string
  ctaButton: CTAButton
  align?: 'left' | 'center' | 'right'
}

export function InlineCTA({
  text,
  ctaButton,
  align = 'center',
}: InlineCTAProps) {
  const alignClasses = {
    left: 'text-left',
    center: 'text-center',
    right: 'text-right',
  }

  const flexAlignClasses = {
    left: 'justify-start',
    center: 'justify-center',
    right: 'justify-end',
  }

  return (
    <div
      className={`py-8 md:py-12 ${alignClasses[align]}`}
      data-testid="inline-cta"
    >
      <p className="text-lg text-secondary-600 mb-6" data-testid="inline-cta-text">
        {text}
      </p>
      <div className={`flex ${flexAlignClasses[align]}`}>
        <Button
          href={ctaButton.href}
          variant={ctaButton.variant}
          size={ctaButton.size}
          data-testid="inline-cta-button"
          aria-label={ctaButton.label}
        >
          {ctaButton.label}
        </Button>
      </div>
    </div>
  )
}

/**
 * Contact Sales CTA component - specific implementation for sales inquiries.
 */
export interface ContactSalesCTAProps {
  title?: string
  description?: string
  buttonText?: string
  href?: string
}

export function ContactSalesCTA({
  title = 'Need a Custom Solution?',
  description = 'Our sales team is here to help you find the perfect plan for your organization.',
  buttonText = 'Contact Sales',
  href = '#contact',
}: ContactSalesCTAProps) {
  return (
    <div
      className="bg-secondary-50 rounded-xl p-8 md:p-12 text-center"
      data-testid="contact-sales-cta"
    >
      <h3
        className="text-2xl md:text-3xl font-bold text-secondary-900 mb-4"
        data-testid="contact-sales-title"
      >
        {title}
      </h3>
      <p
        className="text-secondary-600 mb-6 max-w-xl mx-auto"
        data-testid="contact-sales-description"
      >
        {description}
      </p>
      <Button
        href={href}
        variant="secondary"
        size="lg"
        data-testid="contact-sales-button"
        aria-label={buttonText}
      >
        {buttonText}
      </Button>
    </div>
  )
}

/**
 * Learn More CTA component - for linking to additional information.
 */
export interface LearnMoreCTAProps {
  text?: string
  href?: string
  variant?: 'link' | 'button'
}

export function LearnMoreCTA({
  text = 'Learn More',
  href = '#features',
  variant = 'link',
}: LearnMoreCTAProps) {
  if (variant === 'button') {
    return (
      <Button
        href={href}
        variant="outline"
        size="md"
        data-testid="learn-more-button"
        aria-label={text}
      >
        {text}
        <svg
          className="ml-2 w-5 h-5"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M17 8l4 4m0 0l-4 4m4-4H3"
          />
        </svg>
      </Button>
    )
  }

  return (
    <a
      href={href}
      className="
        inline-flex items-center
        text-primary-600 font-semibold
        hover:text-primary-700
        focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 rounded
        transition-colors duration-200
      "
      data-testid="learn-more-link"
      aria-label={text}
    >
      {text}
      <svg
        className="ml-2 w-5 h-5"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M17 8l4 4m0 0l-4 4m4-4H3"
        />
      </svg>
    </a>
  )
}
