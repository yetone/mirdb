/**
 * Homepage Page Component
 * Owner: Scenario 8 - Homepage Integration
 *
 * Purpose: Main landing page assembling all homepage sections.
 * This is a placeholder that other scenarios will extend.
 */

import { HeroSection, HowItWorksSection } from '../components/home'

const Home = () => {
  return (
    <div className="min-h-screen bg-base-200">
      <main>
        {/* Hero Section - Scenario 1 */}
        <HeroSection />
        {/* How It Works Section - Scenario 3 */}
        <HowItWorksSection />
      </main>
    </div>
  )
}

export default Home
