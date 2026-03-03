/**
 * Hero Section Component
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Displays the main hero content at the top of the homepage.
 * Contains the service name, tagline, and CTA buttons for navigation.
 */
import React from 'react';
import { FuturisticButton } from './FuturisticButton';
import type { HeroSectionProps } from '../types/home.types';

export function HeroSection({
  serviceName = 'URL Shortener',
  tagline = 'Shorten URLs, track clicks, analyze your audience'
}: HeroSectionProps) {
  return (
    <section className="hero min-h-[70vh] bg-transparent" data-testid="hero-section">
      <div className="hero-content text-center">
        <div className="max-w-2xl">
          <h1 className="text-5xl md:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
            {serviceName}
          </h1>
          <p className="text-lg md:text-xl mb-8 text-base-content/80">
            {tagline}
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <FuturisticButton
              to="/register"
              variant="primary"
              size="lg"
              data-testid="get-started-button"
            >
              Get Started
            </FuturisticButton>
            <FuturisticButton
              to="/login"
              variant="secondary"
              size="lg"
              data-testid="sign-in-button"
            >
              Sign In
            </FuturisticButton>
          </div>
        </div>
      </div>
    </section>
  );
}
