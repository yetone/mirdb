/**
 * Homepage Component
 * Owner: Scenario 8 - Homepage Integration
 *
 * Main homepage that assembles all sections:
 * - Navbar with Login/Register links
 * - HeroSection
 * - FeaturesSection
 * - CTASection
 * - Footer
 * - BackgroundEffect
 *
 * Integrates with:
 * - ThemeContext for theme support
 * - Router for navigation
 *
 * Requirements: All REQ-*, All US-*
 */

import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import {
  HeroSection,
  FeaturesSection,
  CTASection,
  Footer,
} from '../components/homepage';

/**
 * Home - Main homepage component
 * Assembles all homepage sections into a cohesive landing page.
 */
const Home = () => {
  return (
    <div className="min-h-screen flex flex-col" data-testid="homepage">
      {/* Background visual effects */}
      <BackgroundEffect />

      {/* Navigation bar with logo, login, register, and theme toggle */}
      <Navbar />

      {/* Main content area */}
      <main className="flex-1">
        {/* Hero Section - Above the fold with headline, subheadline, and CTAs */}
        <HeroSection />

        {/* Features Section - Product capabilities grid */}
        <div className="container mx-auto px-4">
          <FeaturesSection />
        </div>

        {/* CTA Section - Secondary call-to-action for conversion reinforcement */}
        <CTASection />
      </main>

      {/* Footer - Page footer with links and copyright */}
      <Footer />
    </div>
  );
};

export default Home;
