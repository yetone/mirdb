/**
 * Home Page Component
 *
 * Main homepage container that assembles all homepage sections:
 * - Hero section with value proposition and CTAs
 * - (Future) Inline URL shortener
 * - (Future) Features section
 * - (Future) Footer
 *
 * Requirements: REQ-1, REQ-3
 */

import { HeroSection } from '../components/homepage/HeroSection'
import { InlineShortener } from '../components/homepage/InlineShortener'

export function Home() {
  const handlePrimaryClick = () => {
    // Scroll to shortener section
    const shortener = document.getElementById('inline-shortener')
    shortener?.scrollIntoView({ behavior: 'smooth' })
  }

  const handleSecondaryClick = () => {
    // Navigate to registration (future implementation)
    console.log('Secondary CTA clicked')
  }

  return (
    <main data-testid="home-page">
      <HeroSection
        onPrimaryClick={handlePrimaryClick}
        onSecondaryClick={handleSecondaryClick}
      />
      <section id="inline-shortener" className="py-12 bg-base-100">
        <InlineShortener />
      </section>
    </main>
  )
}

export default Home
