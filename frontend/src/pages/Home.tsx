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

import { HeroSection } from '../components/landing';

function Home() {
  return (
    <main className="min-h-screen bg-base-100">
      <HeroSection />
      {/* Future sections will be added by other scenarios:
          <FeaturesSection />
          <HowItWorksSection />
          <SocialProofSection />
          <Footer />
      */}
    </main>
  );
}

export default Home;
