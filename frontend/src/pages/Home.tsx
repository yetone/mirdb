/**
 * Home Page
 * Owner: Scenario 7 - Theme Support and Consistency
 *
 * Landing page that displays the homepage sections.
 * Integrates with theme system and routes to other pages.
 *
 * Theme Support:
 * - Uses DaisyUI theme classes (bg-base-100, text-base-content, etc.)
 * - Responds to ThemeContext changes via data-theme attribute
 * - All sections inherit theme from document root
 *
 * Related requirements: REQ-9, NFR-5, US-6
 */

import {
  HeroSection,
  HowItWorksSection,
  FeaturesSection,
  SocialProofSection,
  Footer,
} from '../components/homepage'
import { ThemeToggle } from '../components/ThemeToggle'

export default function Home() {
  return (
    <main
      className="min-h-screen bg-base-100"
      data-testid="home-page"
    >
      {/* Theme Toggle - Fixed position for easy access */}
      <div className="fixed top-4 right-4 z-50" data-testid="theme-toggle-container">
        <ThemeToggle />
      </div>

      {/* Hero Section - Above the fold */}
      <HeroSection />

      {/* Features Section */}
      <FeaturesSection />

      {/* How It Works Section */}
      <HowItWorksSection />

      {/* Social Proof Section */}
      <SocialProofSection />

      {/* Footer */}
      <Footer />
    </main>
  )
}
