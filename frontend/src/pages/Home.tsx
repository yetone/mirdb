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
      {/* Skip to Content Link for Keyboard Users */}
      <a
        href="#main-content"
        data-testid="skip-to-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-content focus:rounded-md focus:outline-none focus:ring-2 focus:ring-primary-focus"
      >
        Skip to content
      </a>

      {/* Animated Background Effect */}
      <BackgroundEffect />

      {/* Navigation Bar with Theme Toggle wrapped in header */}
      <header>
        <Navbar />
      </header>

      <main id="main-content" className="relative z-10" tabIndex={-1}>
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
