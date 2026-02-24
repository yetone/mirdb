/**
 * Homepage Component
 * Owner: Scenario 1 - Homepage Accessibility and Routing
 *
 * This is the main landing page component that renders at the root path (/).
 * It composes HeroSection, FeaturesSection, and Footer components.
 *
 * Expected exports:
 * - Home: React.FC - Main homepage component
 *
 * Dependencies:
 * - HeroSection from '../components/home/HeroSection'
 * - FeaturesSection from '../components/home/FeaturesSection' (when available)
 * - Footer from '../components/home/Footer' (when available)
 */

import { HeroSection } from '../components/home/HeroSection'
import { FeaturesSection } from '../components/home/FeaturesSection'
import { Footer } from '../components/home/Footer'
import { Navbar } from '../components/Navbar'
import { BackgroundEffect } from '../components/BackgroundEffect'

export function Home() {
  return (
    <div className="min-h-screen bg-base-100 relative">
      {/* Animated Background Effect */}
      <BackgroundEffect />

      {/* Navigation Bar with Theme Toggle */}
      <Navbar />

      <main className="relative z-10">
        {/* Hero Section */}
        <HeroSection />

        {/* Features Section */}
        <FeaturesSection />

        {/* Footer */}
        <Footer />
      </main>
    </div>
  )
}

export default Home
