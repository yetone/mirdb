/**
 * CTA Section Component
 * Owner: Scenario 4 - Secondary CTA Section
 *
 * Secondary call-to-action section with:
 * - Reinforcement of value proposition
 * - Secondary sign-up CTA button
 * - Visually distinct styling from other sections
 *
 * Expected props:
 * - headline: string (optional, defaults to 'Ready to get started?')
 * - ctaLabel: string (optional, defaults to 'Create Your Free Account')
 * - ctaHref: string (optional, defaults to '/register')
 *
 * Requirements: REQ-5, US-2
 */

import React from 'react';
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import { FuturisticButton } from '../FuturisticButton';
import type { CTAContent } from '../../types/homepage';

export interface CTASectionProps {
  content?: CTAContent;
}

const defaultContent: CTAContent = {
  headline: 'Ready to get started?',
  ctaLabel: 'Create Your Free Account',
  ctaHref: '/register',
};

/**
 * CTASection - Secondary call-to-action reinforcement
 * Displays a visually distinct section encouraging users to sign up.
 */
export const CTASection: React.FC<CTASectionProps> = ({ content = defaultContent }) => {
  const { headline, ctaLabel, ctaHref } = content;

  return (
    <section
      data-testid="cta-section"
      className="py-16 md:py-24 relative"
      aria-labelledby="cta-heading"
    >
      {/* Background gradient for visual distinction */}
      <div className="absolute inset-0 bg-gradient-to-b from-transparent via-primary/5 to-primary/10 pointer-events-none" />

      <motion.div
        className="relative max-w-3xl mx-auto text-center px-4"
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
      >
        {/* CTA Headline */}
        <h2
          id="cta-heading"
          data-testid="cta-headline"
          className="text-3xl md:text-4xl font-bold text-white mb-6"
        >
          {headline}
        </h2>

        {/* Supporting text */}
        <p
          data-testid="cta-description"
          className="text-gray-300 text-lg mb-8 max-w-xl mx-auto"
        >
          Join thousands of users who trust our service to shorten and track their links.
        </p>

        {/* CTA Button */}
        <Link to={ctaHref} data-testid="cta-button">
          <FuturisticButton variant="primary" size="lg">
            {ctaLabel}
          </FuturisticButton>
        </Link>
      </motion.div>
    </section>
  );
};

export default CTASection;
