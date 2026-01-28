/**
 * Homepage - Main landing page for URL Shortening Service
 * Owner: Scenario 15 - Component Integration
 *
 * This is the primary homepage component that composes all sections.
 * Currently includes HeroSection. Other sections will be added by their respective scenarios.
 */
import { BackgroundEffect } from '../components/BackgroundEffect'
import { HeroSection, FeaturesSection, HowItWorks } from '../components/home'

export default function Home() {
  return (
    <main className="relative min-h-screen">
      <BackgroundEffect />
      <HeroSection />
      <FeaturesSection />
      <HowItWorks />
      {/* Footer - to be added by Scenario 6 */}
    </main>
  )
}
