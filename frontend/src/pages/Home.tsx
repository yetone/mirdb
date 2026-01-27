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
import { HeroSection, HowItWorksSection } from '../components/landing';

export const Home: React.FC = () => {
  return (
    <main className="min-h-screen">
      {/* Hero Section */}
      <HeroSection />

      {/* Placeholder for FeaturesSection - to be implemented by Scenario 2 */}

      {/* How It Works Section */}
      <HowItWorksSection />

      {/* Placeholder for SocialProofSection - to be implemented by Scenario 5 */}
      {/* Placeholder for Footer - to be implemented by Scenario 6 */}
    </main>
  );
};

export default Home;
