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
import { FeaturesSection } from '../components/homepage/FeaturesSection'

export function Home() {
  return (
    <div className="min-h-screen bg-base-100" data-testid="homepage">
      {/* Hero Section - to be added by Scenario 1 */}

      {/* Features Section */}
      <FeaturesSection />

      {/* Social Proof Section - to be added by Scenario 7 */}

      {/* Footer - to be added by Scenario 6 */}
    </div>
  )
}
