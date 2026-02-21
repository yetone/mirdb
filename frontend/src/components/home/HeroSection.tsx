import React from 'react';
import { FuturisticButton } from '../common/FuturisticButton';

export function HeroSection() {
  return (
    <section
      className="min-h-[80vh] flex items-center justify-center px-4 py-16"
      data-testid="hero-section"
      aria-labelledby="hero-headline"
    >
      <div className="text-center max-w-4xl mx-auto">
        {/* Headline */}
        <h1
          id="hero-headline"
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 leading-tight"
          data-testid="hero-headline"
        >
          Shorten Your URLs,{' '}
          <span className="text-primary">Amplify Your Insights</span>
        </h1>

        {/* Subheadline */}
        <p
          className="text-lg md:text-xl text-base-content/70 mb-10 max-w-2xl mx-auto"
          data-testid="hero-subheadline"
        >
          Transform long URLs into powerful short links with comprehensive analytics.
          Track clicks, analyze traffic sources, and optimize your marketing campaigns
          with real-time data and actionable insights.
        </p>

        {/* CTA Buttons */}
        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
          <FuturisticButton
            to="/register"
            variant="primary"
            aria-label="Get started with URL shortening"
            data-testid="cta-get-started"
          >
            Get Started
          </FuturisticButton>

          <FuturisticButton
            to="/login"
            variant="outline"
            aria-label="Sign in to your account"
            data-testid="cta-sign-in"
          >
            Sign In
          </FuturisticButton>
        </div>
      </div>
    </section>
  );
}
