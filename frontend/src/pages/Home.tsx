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
import BackgroundEffect from '../components/BackgroundEffect'
import { HowItWorksSection, AnalyticsPreviewSection } from '../components/home'

export default function Home() {
  return (
    <div className="min-h-screen bg-base-200">
      <BackgroundEffect />
      <Navbar />
      <main>
        {/* HeroSection placeholder - to be built by Scenario 1 */}
        <section className="hero min-h-[60vh] bg-base-200">
          <div className="hero-content text-center">
            <div className="max-w-md md:max-w-2xl">
              <h1 className="text-5xl md:text-6xl font-bold">URL Shortener</h1>
              <p className="py-6">Shorten URLs. Track Results. Grow Smarter.</p>
            </div>
          </div>
        </section>

        {/* FeaturesSection placeholder - to be built by Scenario 2 */}

        {/* HowItWorksSection - built by Scenario 3 */}
        <HowItWorksSection />

        {/* AnalyticsPreviewSection - built by Scenario 9 */}
        <AnalyticsPreviewSection />

        {/* Footer placeholder - to be built by Scenario 10 */}
      </main>
    </div>
  )
}
