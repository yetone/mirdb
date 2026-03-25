/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Displays the main hero area with:
 * - Product headline and tagline
 * - Primary CTA (Get Started / Go to Dashboard)
 * - Secondary CTA (Sign In)
 * - BackgroundEffect animation
 */

import React from 'react';
import { Link } from 'react-router-dom';
import { BackgroundEffect } from '../BackgroundEffect';
import { FuturisticButton } from '../FuturisticButton';
import { HeroSectionProps } from '../../types/homepage';

export function HeroSection({ isAuthenticated = false }: HeroSectionProps) {
  return (
    <section
      className="relative min-h-[80vh] flex items-center justify-center px-4"
      aria-label="Hero section"
    >
      <BackgroundEffect variant="hero" />
      <div className="relative z-10 text-center max-w-4xl mx-auto">
        <h1 className="text-4xl md:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
          Shorten URLs, Amplify Reach
        </h1>
        <p className="text-lg md:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto">
          Transform long, unwieldy URLs into concise, trackable links. Get powerful analytics
          to understand your audience and optimize your campaigns.
        </p>
        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
          {isAuthenticated ? (
            <FuturisticButton to="/dashboard" variant="primary" size="lg" data-testid="hero-dashboard-button">
              Go to Dashboard
            </FuturisticButton>
          ) : (
            <>
              <FuturisticButton to="/register" variant="primary" size="lg" data-testid="hero-get-started-button">
                Get Started Free
              </FuturisticButton>
              <Link
                to="/login"
                className="text-primary hover:text-primary-focus underline underline-offset-4 transition-colors"
                data-testid="hero-sign-in-link"
                aria-label="Sign in to your existing account"
              >
                Sign In
              </Link>
            </>
          )}
        </div>
      </div>
    </section>
  );
}
