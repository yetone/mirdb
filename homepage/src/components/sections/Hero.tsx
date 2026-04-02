/**
 * Hero section component.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays:
 * - MirDB logo/branding
 * - Headline: "MirDB: Persistent Key-Value Store"
 * - Tagline: "Memcached protocol with disk persistence"
 * - Primary CTA: "Get Started" button
 */
import React from 'react';
import { Container } from '@/components/common/Container';
import { Button } from '@/components/common/Button';
import { HeroProps } from '@/types';
import { HERO_CONTENT } from '@/utils/constants';

export function Hero({
  title = HERO_CONTENT.title,
  tagline = HERO_CONTENT.tagline,
  ctaText = HERO_CONTENT.ctaText,
  ctaHref = HERO_CONTENT.ctaHref,
}: HeroProps) {
  return (
    <section
      id="hero"
      data-testid="hero-section"
      className="min-h-screen flex items-center justify-center bg-gradient-to-b from-blue-50 to-white dark:from-gray-900 dark:to-gray-800"
    >
      <Container className="py-20 text-center">
        {/* Logo/Branding */}
        <div
          data-testid="hero-logo"
          className="mb-8 inline-flex items-center justify-center w-20 h-20 rounded-2xl bg-blue-600 text-white text-3xl font-bold shadow-lg"
        >
          M
        </div>

        {/* Headline */}
        <h1
          data-testid="hero-headline"
          className="text-4xl md:text-5xl lg:text-6xl font-bold text-gray-900 dark:text-white mb-6 leading-tight"
        >
          {title}
        </h1>

        {/* Tagline */}
        <p
          data-testid="hero-tagline"
          className="text-xl md:text-2xl text-gray-600 dark:text-gray-300 mb-10 max-w-2xl mx-auto"
        >
          {tagline}
        </p>

        {/* Primary CTA */}
        <Button
          href={ctaHref}
          size="lg"
          variant="primary"
          data-testid="hero-cta"
          external
        >
          {ctaText}
        </Button>
      </Container>
    </section>
  );
}
