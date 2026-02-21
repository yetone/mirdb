import React from 'react';
import { Link } from 'react-router-dom';
import { FuturisticButton } from '../common/FuturisticButton';

export function HeroSection() {
  return (
    <section className="min-h-[60vh] flex flex-col items-center justify-center text-center px-4 py-16" data-testid="hero-section">
      <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6">
        Shorten Links, <span className="text-primary">Track Results</span>
      </h1>
      <p className="text-lg md:text-xl text-base-content/70 max-w-2xl mb-8">
        Transform long URLs into short, powerful links with detailed analytics.
        Track every click and optimize your marketing campaigns.
      </p>
      <div className="flex flex-col sm:flex-row gap-4">
        <Link to="/register" data-testid="cta-get-started">
          <FuturisticButton variant="primary" aria-label="Get started with URL shortening">
            Get Started
          </FuturisticButton>
        </Link>
        <Link to="/login" data-testid="cta-sign-in">
          <FuturisticButton variant="outline" aria-label="Sign in to your account">
            Sign In
          </FuturisticButton>
        </Link>
      </div>
    </section>
  );
}
