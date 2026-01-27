/**
 * Homepage Page Component
 * Owner: Scenario 1 (primary), with updates from Scenarios 3, 11, 13
 *
 * Main landing page that composes:
 * - HeroSection
 * - FeaturesSection (added by Scenario 2)
 * - StatsSection (optional, to be added)
 * - Footer (added by Scenario 9)
 */
import { HeroSection, FeaturesSection, Footer } from '../components/homepage'

export default function Home() {
  return (
    <main className="min-h-screen bg-base-100">
      <HeroSection />
      <FeaturesSection />
      <Footer />
    </main>
  )
}
