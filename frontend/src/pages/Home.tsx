/**
 * Homepage Component.
 * Owner: Scenario 18 - React Router Integration
 *
 * Main homepage that composes all sections:
 * - SkipLink (accessibility)
 * - HomeNavbar
 * - HeroSection
 * - HowItWorksSection
 * - FeaturesSection
 * - CTASection
 * - HomeFooter
 *
 * Integrates with:
 * - React Router (root route "/")
 * - AuthContext (shows different nav for authenticated users)
 * - ThemeContext (supports all themes)
 *
 * Requirements: All REQs consolidated
 */
import { HeroSection, HowItWorksSection, FeaturesSection, CTASection } from '../components/home';
import { HomeNavbar, HomeFooter } from '../components/layout';
import { SkipLink } from '../components/ui';

export function Home() {
  return (
    <div className="min-h-screen flex flex-col" data-testid="home-page">
      <SkipLink targetId="main-content" />
      <HomeNavbar />
      <main id="main-content" className="flex-1">
        <HeroSection />
        <HowItWorksSection />
        <FeaturesSection />
        <CTASection />
      </main>
      <HomeFooter />
    </div>
  );
}
