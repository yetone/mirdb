/**
 * Homepage Component
 * Owner: Primary - Scenario 1 (Hero Section), Secondary - Scenarios 15, 17
 *
 * Main landing page for the URL Shortening Service.
 * Composes HeroSection, FeaturesSection, HowItWorksSection, and Footer.
 */

import FeaturesSection from '../components/homepage/FeaturesSection'

export default function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      <main>
        {/* Hero Section - to be implemented by Scenario 1 */}
        <section className="hero min-h-[60vh] bg-base-200">
          <div className="hero-content text-center">
            <div className="max-w-md">
              <h1 className="text-5xl font-bold">URL Shortening Service</h1>
              <p className="py-6">Shorten Links. Track Insights. Share Smarter.</p>
            </div>
          </div>
        </section>

        {/* Features Section - Scenario 3 */}
        <FeaturesSection />

        {/* How It Works Section - to be implemented by Scenario 4 */}

        {/* Footer - to be implemented by Scenario 6 */}
      </main>
    </div>
  )
}
