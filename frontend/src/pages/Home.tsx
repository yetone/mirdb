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

export function Home() {
  const handlePrimaryClick = () => {
    // Scroll to shortener section or focus input (future implementation)
    console.log('Primary CTA clicked')
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
    </main>
  )
}

export default Home
