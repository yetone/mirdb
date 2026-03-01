import React from 'react';
import FuturisticButton from '../FuturisticButton';

/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Displays the primary value proposition of the URL shortening service.
 *
 * Expected exports:
 * - HeroSection: React.FC - The hero section component
 *
 * Requirements:
 * - Product name/branding
 * - Compelling tagline
 * - Sign Up CTA button (links to /register)
 * - Log In CTA button (links to /login)
 * - Uses FuturisticButton for CTAs
 */
export const HeroSection: React.FC = () => {
  return (
    <section
      className="min-h-[80vh] flex flex-col items-center justify-center text-center px-4"
      aria-label="Hero section"
      data-testid="hero-section"
    >
      <div className="max-w-4xl mx-auto">
        {/* Product Name */}
        <h1 className="text-5xl md:text-7xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
          URL Shortener
        </h1>

        {/* Tagline */}
        <p className="text-xl md:text-2xl text-base-content/80 mb-4" data-testid="hero-tagline">
          Shorten, share, and track your links with ease
        </p>

        {/* Value Proposition */}
        <p className="text-lg text-base-content/60 mb-12 max-w-2xl mx-auto" data-testid="hero-value-prop">
          Create concise, powerful links in seconds. Monitor engagement, analyze
          performance, and grow your online presence with our comprehensive URL
          management platform.
        </p>

        {/* CTA Buttons */}
        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
          <FuturisticButton to="/register" variant="primary" size="lg" data-testid="hero-signup-btn">
            Sign Up
          </FuturisticButton>
          <FuturisticButton to="/login" variant="outline" size="lg" data-testid="hero-login-btn">
            Log In
          </FuturisticButton>
        </div>
      </div>
    </section>
  );
};

export default HeroSection;
