/**
 * Homepage Component
 * Owner: Primary - Scenario 1 (Hero Section), Secondary - Scenarios 15, 17
 *
 * Main landing page for the URL Shortening Service.
 * Composes HeroSection, FeaturesSection, HowItWorksSection, and Footer.
 */

import HeroSection from '../components/homepage/HeroSection'
import FeaturesSection from '../components/homepage/FeaturesSection'
import HowItWorksSection from '../components/homepage/HowItWorksSection'
import Footer from '../components/Footer'

export default function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      <main>
        {/* Hero Section - Scenario 1 */}
        <HeroSection />

        {/* Features Section - Scenario 3 */}
        <FeaturesSection />

        {/* How It Works Section - Scenario 4 */}
        <HowItWorksSection />
      </main>

      {/* Footer - Scenario 6 */}
      <Footer />
    </div>
  )
}
