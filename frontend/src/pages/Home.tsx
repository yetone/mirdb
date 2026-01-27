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
import { HeroSection, FeaturesSection, HowItWorksSection, Footer } from '../components/homepage'

export function Home() {
  return (
    <div className="min-h-screen bg-base-100 flex flex-col" data-testid="home-page">
      <Navbar />
      <main className="pt-16 flex-1">
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
      </main>
      <Footer />
    </div>
  )
}

export default Home
