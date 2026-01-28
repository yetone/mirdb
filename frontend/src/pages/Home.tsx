/**
 * Homepage - Main landing page for URL Shortening Service
 * Owner: Scenario 15 - Component Integration
 *
 * This is the primary homepage component that composes all sections.
 *
 * Expected exports:
 * - Home: React.FC - Main homepage component
 *
 * Required sections:
 * - HeroSection: Headline, description, and CTAs
 * - FeaturesSection: Three feature cards
 * - HowItWorks: Three-step process
 * - Footer: Navigation links and copyright
 *
 * Integration points:
 * - Uses ThemeContext for theme support
 * - Uses React Router for navigation (Link components)
 * - Uses existing components: BackgroundEffect, GlassMorphismCard, FuturisticButton, Navbar, ThemeToggle
 *
 * Accessibility: Skip-to-content link added by Scenario 12 for keyboard navigation.
 * SEO: Header element added by Scenario 16 for semantic HTML structure.
 */
import { BackgroundEffect } from '../components/BackgroundEffect'
import { Navbar } from '../components/Navbar'
import { HeroSection, FeaturesSection, HowItWorks, Footer } from '../components/home'

export default function Home() {
  return (
    <div className="flex flex-col min-h-screen">
      {/* Header element for SEO-friendly semantic structure - contains skip link and navigation */}
      <header>
        {/* Skip-to-content link for keyboard and screen reader users */}
        <a
          href="#main-content"
          className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-content focus:rounded-md focus:outline-none focus:ring-2 focus:ring-primary-focus"
        >
          Skip to main content
        </a>
        <Navbar />
      </header>
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
