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
 * Requirements: All REQ-*, All US-*
 */

import { FeaturesSection } from '../components/homepage';

function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      {/* Placeholder for other sections */}
      <main className="container mx-auto px-4 py-16">
        <FeaturesSection />
      </main>
    </div>
  );
}

export default Home;
