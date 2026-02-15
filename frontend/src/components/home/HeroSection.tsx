/**
 * Hero Section Component
 * Owner: Scenario 1 - Homepage Structure and Layout
 *
 * Attention-grabbing hero section with:
 * - Product name headline
 * - Value proposition tagline
 * - CTA buttons (Get Started Free, Learn More)
 * - Background visual effect
 */

import { Link } from 'react-router-dom';
import { Link2 } from 'lucide-react';
import type { HeroSectionProps } from '../../types/home';

export default function HeroSection({ onGetStarted, onLearnMore }: HeroSectionProps) {
  const handleGetStarted = () => {
    if (onGetStarted) {
      onGetStarted();
    }
  };

  const handleLearnMore = () => {
    if (onLearnMore) {
      onLearnMore();
    }
  };

  return (
    <section className="hero min-h-[70vh] bg-gradient-to-br from-primary/10 via-base-100 to-secondary/10" data-testid="hero-section">
      <div className="hero-content text-center py-16">
        <div className="max-w-2xl">
          <div className="flex items-center justify-center mb-6">
            <Link2 className="w-16 h-16 text-primary" />
          </div>
          <h1 className="text-5xl font-bold mb-4" data-testid="hero-title">
            <span className="text-primary">LinkSnip</span>
          </h1>
          <p className="text-xl mb-2 text-base-content/80" data-testid="hero-tagline">
            Shorten. Share. Track.
          </p>
          <p className="text-lg mb-8 text-base-content/70">
            Transform long URLs into short, shareable links. Get detailed analytics
            and insights on every click.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              to="/register"
              className="btn btn-primary btn-lg"
              onClick={handleGetStarted}
              data-testid="get-started-button"
            >
              Get Started Free
            </Link>
            <button
              className="btn btn-outline btn-lg"
              onClick={handleLearnMore}
              data-testid="learn-more-button"
            >
              Learn More
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}
