/**
 * Home Page Component
 * Owner: Scenario 8 - Homepage Integration
 *
 * Main homepage that composes all sections:
 * - Navbar (existing component)
 * - HeroSection
 * - FeaturesSection
 * - HowItWorksSection
 * - Footer
 *
 * Integrates with:
 * - ThemeContext for theme switching
 * - React Router for navigation
 *
 * Requirements: All homepage requirements
 */

import { Navbar } from '../components/Navbar'
import { HeroSection, FeaturesSection, HowItWorksSection } from '../components/homepage'

export function Home() {
  return (
    <div className="min-h-screen bg-base-100" data-testid="home-page">
      <Navbar />
      <main className="pt-16">
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
      </main>
    </div>
  )
}

export default Home
