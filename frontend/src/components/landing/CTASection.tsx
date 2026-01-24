/**
 * Secondary CTA Section Component
 * Owner: Scenario 6 - Secondary CTA Section
 *
 * Displays a final call-to-action before the footer:
 * - Compelling message encouraging signup
 * - Primary CTA button linking to registration
 *
 * Requirements: REQ-6
 */

import React from 'react'
import { CTASectionProps } from '../../types/landing'
import FuturisticButton from '../FuturisticButton'

const DEFAULT_MESSAGE = 'Join thousands of users shortening URLs today'
const DEFAULT_CTA_TEXT = 'Create Free Account'
const DEFAULT_CTA_HREF = '/register'

export function CTASection({
  message = DEFAULT_MESSAGE,
  ctaText = DEFAULT_CTA_TEXT,
  ctaHref = DEFAULT_CTA_HREF,
}: CTASectionProps) {
  return (
    <section
      data-testid="cta-section"
      className="py-16 px-4 bg-base-200"
      aria-labelledby="cta-heading"
    >
      <div className="max-w-4xl mx-auto text-center">
        <h2
          id="cta-heading"
          data-testid="cta-message"
          className="text-2xl md:text-3xl lg:text-4xl font-bold text-base-content mb-8"
        >
          {message}
        </h2>

        <FuturisticButton
          href={ctaHref}
          variant="primary"
          size="lg"
          data-testid="cta-button"
        >
          {ctaText}
        </FuturisticButton>
      </div>
    </section>
  )
}

export default CTASection
