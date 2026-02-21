/**
 * Main Homepage Component.
 * Owner: Scenario 6 - Main HomePage Assembly
 *
 * Expected behavior:
 * - Assembles all homepage sections in correct order:
 *   1. PublicNavbar
 *   2. HeroSection
 *   3. FeaturesSection
 *   4. DemoSection
 *   5. SocialProofSection
 *   6. CTASection
 *   7. Footer
 * - Handles scroll behavior for internal navigation
 * - Integrates with theme context
 * - SEO-friendly with proper meta tags
 */

import { useEffect } from 'react';
import PublicNavbar from '../components/homepage/PublicNavbar';
import HeroSection from '../components/homepage/HeroSection';
import FeaturesSection from '../components/homepage/FeaturesSection';
import DemoSection from '../components/homepage/DemoSection';
import SocialProofSection from '../components/homepage/SocialProofSection';
import CTASection from '../components/homepage/CTASection';
import Footer from '../components/homepage/Footer';

function HomePage() {
  // Enable smooth scrolling for the entire page
  useEffect(() => {
    // Set smooth scroll behavior on mount
    document.documentElement.style.scrollBehavior = 'smooth';

    return () => {
      // Clean up on unmount
      document.documentElement.style.scrollBehavior = 'auto';
    };
  }, []);

  return (
    <div className="min-h-screen bg-base-100" data-testid="homepage">
      <PublicNavbar />
      <main>
        <HeroSection />
        <FeaturesSection />
        <DemoSection />
        <SocialProofSection />
        <CTASection />
      </main>
      <Footer />
    </div>
  );
}

export default HomePage;
