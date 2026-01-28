/**
 * Homepage - Main landing page for URL Shortening Service
 * Owner: Scenario 15 - Component Integration
 *
 * This is the primary homepage component that composes all sections.
 * Currently includes HeroSection. Other sections will be added by their respective scenarios.
 *
 * Accessibility: Skip-to-content link added by Scenario 12 for keyboard navigation.
 */
import { BackgroundEffect } from '../components/BackgroundEffect'
import { HeroSection, FeaturesSection, HowItWorks, Footer } from '../components/home'

export default function Home() {
  return (
    <div className="flex flex-col min-h-screen">
      {/* Skip-to-content link for keyboard and screen reader users */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-content focus:rounded-md focus:outline-none focus:ring-2 focus:ring-primary-focus"
      >
        Skip to main content
      </a>
      <main id="main-content" className="relative flex-grow" tabIndex={-1}>
        <BackgroundEffect />
        <HeroSection />
        <FeaturesSection />
        <HowItWorks />
      </main>
      <Footer />
    </div>
  )
}
