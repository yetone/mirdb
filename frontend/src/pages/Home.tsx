/**
 * Home Page Component
 *
 * Main homepage container that assembles all homepage sections:
 * - Navigation bar with logo and links
 * - Hero section with value proposition and CTAs
 * - Inline URL shortener for anonymous users
 * - Features section placeholder
 * - (Future) Footer
 *
 * Requirements: REQ-1, REQ-2, REQ-3, REQ-5, REQ-7
 */

import { useRef } from 'react'
import { HeroSection } from '../components/homepage/HeroSection'
import { InlineShortener } from '../components/homepage/InlineShortener'
import { Navbar } from '../components/Navbar'

export function Home() {
  const shortenerRef = useRef<HTMLDivElement>(null)

  const handlePrimaryClick = () => {
    // Scroll to shortener section and focus input
    shortenerRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  const handleSecondaryClick = () => {
    // Navigate to registration
    window.location.href = '/register'
  }

  const handleFeaturesClick = () => {
    const featuresSection = document.getElementById('features-section')
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <>
      <Navbar onFeaturesClick={handleFeaturesClick} />
      <main data-testid="home-page" className="pt-16">
        <HeroSection
          onPrimaryClick={handlePrimaryClick}
          onSecondaryClick={handleSecondaryClick}
        />
        <div ref={shortenerRef}>
          <InlineShortener />
        </div>
        {/* Features section placeholder for anchor link */}
        <section
          id="features-section"
          data-testid="features-section"
          className="min-h-[50vh] py-16 bg-base-200"
        >
          <div className="container mx-auto px-4 text-center">
            <h2 className="text-3xl font-bold mb-8">Features</h2>
            <p className="text-base-content/70">Feature cards coming soon...</p>
          </div>
        </section>
      </main>
    </>
  )
}

export default Home
