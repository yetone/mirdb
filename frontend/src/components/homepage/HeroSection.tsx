/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero area with:
 * - Prominent headline communicating core value
 * - Supporting subheadline explaining benefits
 * - Primary CTA button (e.g., "Get Started Free")
 * - Secondary CTA link (e.g., "Sign In")
 *
 * Requirements: REQ-1, US-1, US-2
 */

import React from 'react';
import { motion } from 'framer-motion';
import FuturisticButton from '../FuturisticButton';
import type { HeroContent } from '../../types/homepage';

interface HeroSectionProps {
  content?: HeroContent;
}

const defaultContent: HeroContent = {
  headline: 'Shorten URLs. Track Every Click.',
  subheadline: 'Create memorable, shortened links with powerful analytics in seconds. Know exactly where your traffic comes from.',
  primaryCTA: {
    label: 'Get Started Free',
    href: '/register',
  },
  secondaryCTA: {
    label: 'Sign In',
    href: '/login',
  },
};

const HeroSection: React.FC<HeroSectionProps> = ({ content = defaultContent }) => {
  const { headline, subheadline, primaryCTA, secondaryCTA } = content;

  return (
    <section
      className="min-h-[80vh] flex items-center justify-center px-4 py-16"
      data-testid="hero-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        <motion.h1
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 bg-gradient-to-r from-purple-400 via-pink-500 to-red-500 bg-clip-text text-transparent"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          data-testid="hero-headline"
        >
          {headline}
        </motion.h1>

        <motion.p
          className="text-lg md:text-xl text-gray-300 mb-10 max-w-2xl mx-auto"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          data-testid="hero-subheadline"
        >
          {subheadline}
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row items-center justify-center gap-4"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
        >
          <FuturisticButton
            href={primaryCTA.href}
            variant="primary"
            size="lg"
            data-testid="hero-primary-cta"
          >
            {primaryCTA.label}
          </FuturisticButton>

          {secondaryCTA && (
            <FuturisticButton
              href={secondaryCTA.href}
              variant="outline"
              size="lg"
              data-testid="hero-secondary-cta"
            >
              {secondaryCTA.label}
            </FuturisticButton>
          )}
        </motion.div>
      </div>
    </section>
  );
};

export default HeroSection;
