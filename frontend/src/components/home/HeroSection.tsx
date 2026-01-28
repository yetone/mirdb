/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Functionality
 *
 * Requirements covered:
 * - REQ-1: Display hero section with headline, value proposition, and CTAs
 * - REQ-10: Link CTAs to registration (/register) and login (/login) routes
 *
 * Expected exports:
 * - HeroSection: React.FC - Main hero section component
 *
 * Props: None (self-contained component)
 *
 * Features:
 * - Large gradient text headline
 * - Supporting description paragraph
 * - Primary CTA: "Get Started" -> /register
 * - Secondary CTA: "Log In" -> /login
 * - Responsive layout (stacked mobile, side-by-side desktop)
 */

import { Link } from 'react-router-dom';

export function HeroSection() {
  return (
    <section
      className="min-h-screen flex items-center justify-center px-4 py-12 md:py-0"
      aria-labelledby="hero-headline"
    >
      <div className="max-w-4xl mx-auto text-center">
        {/* Content container - stacked on mobile, centered on desktop */}
        <div className="flex flex-col items-center space-y-8">
          {/* Headline with gradient text */}
          <h1
            id="hero-headline"
            className="text-4xl md:text-5xl lg:text-6xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          >
            Shorten URLs, Amplify Your Reach
          </h1>

          {/* Description paragraph */}
          <p className="text-lg md:text-xl text-base-content/80 max-w-2xl">
            Transform long, unwieldy links into short, memorable URLs. Track clicks,
            analyze traffic, and manage all your links in one powerful dashboard.
          </p>

          {/* CTA Buttons - stacked on mobile, side-by-side on larger screens */}
          <div className="flex flex-col sm:flex-row gap-4 w-full sm:w-auto justify-center">
            <Link
              to="/register"
              className="btn btn-primary btn-lg"
              role="button"
            >
              Get Started
            </Link>
            <Link
              to="/login"
              className="btn btn-outline btn-lg"
              role="button"
            >
              Log In
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}
