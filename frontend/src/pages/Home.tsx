/**
 * Homepage - Main landing page for URL Shortening Service
 * Owner: Scenario 15 - Component Integration
 *
 * This is the primary homepage component that composes all sections.
 * Currently includes HeroSection. Other sections will be added by their respective scenarios.
 */
import { BackgroundEffect } from '../components/BackgroundEffect'
import { HeroSection, FeaturesSection, Footer } from '../components/home'

export default function Home() {
  return (
    <div className="flex flex-col min-h-screen">
      <main className="relative flex-grow">
        <BackgroundEffect />
        <HeroSection />
        <FeaturesSection />
        {/* HowItWorks - to be added by Scenario 5 */}
      </main>
      <Footer />
    </div>
  )
}
