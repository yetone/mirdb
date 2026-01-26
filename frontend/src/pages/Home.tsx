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
 *
 * SEO Enhancements (Scenario 20):
 * - Sets document title for search engine display
 * - Manages meta description for search result snippets
 * - Ensures proper heading hierarchy
 */
import { useEffect } from 'react'
import Navbar from '../components/Navbar'
import {
  HeroSection,
  FeaturesSection,
  HowItWorksSection,
  AnalyticsPreviewSection,
  Footer,
} from '../components/home'

const SEO_TITLE = 'URL Shortener - Shorten URLs, Track Results'
const SEO_DESCRIPTION =
  'Free URL shortener with click analytics, GeoIP tracking, and custom short links. Transform long URLs into powerful, trackable short links.'

export default function Home() {
  useEffect(() => {
    // Set document title for SEO
    document.title = SEO_TITLE

    // Set or update meta description for SEO
    let metaDescription = document.querySelector(
      'meta[name="description"]'
    ) as HTMLMetaElement | null
    if (!metaDescription) {
      metaDescription = document.createElement('meta')
      metaDescription.name = 'description'
      document.head.appendChild(metaDescription)
    }
    metaDescription.content = SEO_DESCRIPTION

    // Cleanup function to reset if component unmounts
    return () => {
      // Optional: Reset title when navigating away
      // In a real SPA, this could be handled by a route-level meta management
    }
  }, [])

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
