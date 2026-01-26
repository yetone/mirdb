/**
 * Homepage - Main landing page component
 * Owner: Scenario 15 - Component Composition and Reusability
 *
 * This is the primary container component that composes all homepage sections.
 *
 * Expected composition:
 * - Navbar (existing component)
 * - HeroSection
 * - FeaturesSection
 * - HowItWorksSection
 * - AnalyticsPreviewSection
 * - Footer
 *
 * Integrations:
 * - AuthContext for conditional rendering based on auth state
 * - ThemeContext for theme-aware styling
 * - React Router for navigation
 */
import Navbar from '../components/Navbar'
import {
  HeroSection,
  FeaturesSection,
  HowItWorksSection,
  AnalyticsPreviewSection,
  Footer,
} from '../components/home'

export default function Home() {
  return (
    <div className="min-h-screen bg-base-200">
      <Navbar />
      <main>
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        <AnalyticsPreviewSection />
        <Footer />
      </main>
    </div>
  )
}
