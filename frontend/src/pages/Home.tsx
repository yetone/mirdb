/**
 * Landing Page (Home) Component.
 * Owner: First builder (shared)
 *
 * Main landing page that composes all section components.
 * This component integrates:
 * - HeroSection
 * - FeaturesSection
 * - HowItWorksSection
 * - SocialProofSection
 * - Footer
 *
 * Must use:
 * - ThemeContext for theme support
 * - Framer Motion for scroll animations
 * - Semantic HTML structure
 *
 * Expected exports:
 * - Home: React.FC (default export)
 */

import React from 'react';
import { FeaturesSection, Footer, HeroSection, HowItWorksSection, SocialProofSection } from '../components/landing';

export const Home: React.FC = () => {
  return (
    <>
      {/* Skip to content link for keyboard/screen reader users */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-content focus:rounded-md focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary"
      >
        Skip to content
      </a>

      <main id="main-content" className="min-h-screen" data-testid="landing-page" tabIndex={-1}>
        {/* Hero Section */}
        <HeroSection />

        {/* Features Section */}
        <FeaturesSection />

        {/* How It Works Section */}
        <HowItWorksSection />

        {/* Social Proof Section */}
        <SocialProofSection />

        {/* Footer Section */}
        <Footer />
      </main>
    </>
  );
};

export default Home;
