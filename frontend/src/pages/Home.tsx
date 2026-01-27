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
    <main className="min-h-screen" data-testid="landing-page">
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
  );
};

export default Home;
