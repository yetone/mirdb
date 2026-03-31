/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero section with:
 * - MirDB logo (animated GIF)
 * - Product name and tagline
 * - Value proposition description
 * - Primary CTA: "Get Started" button
 * - Secondary CTA: "View on GitHub" button
 *
 * Requirements: REQ-1, Story 1 (Product Discovery)
 */

import { ExternalLink, ArrowRight } from 'lucide-react';
import { Button } from '../ui/Button';
import { SITE_TITLE, SITE_TAGLINE, SITE_DESCRIPTION, GITHUB_URL } from '../../constants/content';

export interface HeroSectionProps {
  onGetStarted?: () => void;
  onViewGitHub?: () => void;
}

export function HeroSection({ onGetStarted, onViewGitHub }: HeroSectionProps) {
  const handleGetStarted = () => {
    if (onGetStarted) {
      onGetStarted();
    } else {
      const quickStartSection = document.getElementById('quick-start');
      if (quickStartSection) {
        quickStartSection.scrollIntoView({ behavior: 'smooth' });
      }
    }
  };

  const handleViewGitHub = () => {
    if (onViewGitHub) {
      onViewGitHub();
    } else {
      window.open(GITHUB_URL, '_blank', 'noopener,noreferrer');
    }
  };

  return (
    <section
      id="hero"
      className="min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-800 py-16 px-4 sm:px-6 lg:px-8"
      aria-labelledby="hero-heading"
      data-testid="hero-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        <div className="mb-8">
          <img
            src="/assets/logo.gif"
            alt="MirDB Logo"
            className="mx-auto h-32 w-auto sm:h-40 lg:h-48"
            data-testid="hero-logo"
          />
        </div>

        <h1
          id="hero-heading"
          className="text-4xl font-extrabold text-gray-900 dark:text-white sm:text-5xl lg:text-6xl mb-4"
          data-testid="hero-title"
        >
          {SITE_TITLE}
        </h1>

        <p
          className="text-xl sm:text-2xl text-primary-600 dark:text-primary-400 font-medium mb-6"
          data-testid="hero-tagline"
        >
          {SITE_TAGLINE}
        </p>

        <p
          className="text-lg text-gray-600 dark:text-gray-300 max-w-2xl mx-auto mb-8 leading-relaxed"
          data-testid="hero-description"
        >
          {SITE_DESCRIPTION}
        </p>

        <div
          className="flex flex-col sm:flex-row gap-4 justify-center"
          data-testid="hero-cta-buttons"
        >
          <Button
            variant="primary"
            size="lg"
            onClick={handleGetStarted}
            data-testid="get-started-button"
            className="group"
          >
            Get Started
            <ArrowRight className="ml-2 w-5 h-5 group-hover:translate-x-1 transition-transform" aria-hidden="true" />
          </Button>

          <Button
            variant="outline"
            size="lg"
            onClick={handleViewGitHub}
            data-testid="view-github-button"
            className="group"
          >
            <ExternalLink className="mr-2 w-5 h-5" aria-hidden="true" />
            View on GitHub
          </Button>
        </div>
      </div>
    </section>
  );
}
