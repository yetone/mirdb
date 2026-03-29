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

export default function Home() {
  return (
    <main className="min-h-screen">
      <HeroSection />
      {/* Other sections will be added by their respective scenario owners */}
    </main>
  );
}
