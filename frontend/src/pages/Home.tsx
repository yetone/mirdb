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
import { HeroSection, HowItWorksSection, AnalyticsPreviewSection, Footer } from '../components/home'

export default function Home() {
  return (
    <div className="min-h-screen bg-base-200">
      <Navbar />
      <main>
        {/* HeroSection - built by Scenario 1, BackgroundEffect integration by Scenario 13 */}
        <HeroSection />

        {/* FeaturesSection placeholder - to be built by Scenario 2 */}

        {/* HowItWorksSection - built by Scenario 3 */}
        <HowItWorksSection />

        {/* AnalyticsPreviewSection - built by Scenario 9 */}
        <AnalyticsPreviewSection />

        {/* Footer - built by Scenario 10 */}
        <Footer />
      </main>
    </div>
  )
}
