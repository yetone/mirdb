/**
 * Homepage - Main landing page for URL Shortening Service
 * Owner: First scenario builder
 *
 * This component assembles all homepage sections:
 * - HeroSection
 * - FeaturesSection
 * - SocialProofSection
 * - Footer
 */
import { HeroSection } from '../components/homepage/HeroSection';
import { FeaturesSection } from '../components/homepage/FeaturesSection';
import { Footer } from '../components/homepage/Footer';

export function Home() {
  return (
    <div className="min-h-screen bg-base-100" data-testid="homepage">
      {/* Hero Section */}
      <HeroSection />

      {/* Features Section */}
      <FeaturesSection />

      {/* Social Proof Section - to be added by Scenario 7 */}

      {/* Footer */}
      <Footer />
    </div>
  );
}
