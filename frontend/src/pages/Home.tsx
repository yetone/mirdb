/**
 * Home Page Component
 *
 * Main homepage container that assembles all homepage sections:
 * - Hero section with value proposition and CTAs
 * - Inline URL shortener for anonymous users
 * - Features section placeholder
 * - (Future) Footer
 *
 * Requirements: REQ-1, REQ-2, REQ-3, REQ-7
 */

import { useRef } from 'react'
import { HeroSection } from '../components/homepage/HeroSection'
import { InlineShortener } from '../components/homepage/InlineShortener'
import { FeaturesSection } from '../components/homepage/FeaturesSection'

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

  return (
    <main data-testid="home-page">
      <HeroSection
        onPrimaryClick={handlePrimaryClick}
        onSecondaryClick={handleSecondaryClick}
      />
      <div ref={shortenerRef}>
        <InlineShortener />
      </div>
      <FeaturesSection />
    </main>
  )
}

export default Home
