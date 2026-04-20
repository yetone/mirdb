/**
 * Homepage landing page component.
 * Owner: Scenario 1 - Hero Section Value Proposition Display
 *
 * This is the main landing page for the URL Shortening Service.
 * Displays a compelling hero section with value proposition and CTAs,
 * followed by a features section showcasing key capabilities.
 */

import { HeroSection, FeaturesSection } from '../components/home'

export default function Home() {
  return (
    <main className="min-h-screen">
      <HeroSection />
      <FeaturesSection />
    </main>
  )
}
