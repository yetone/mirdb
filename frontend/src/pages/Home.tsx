/**
 * Home Page
 *
 * Landing page that displays the homepage sections.
 * Integrates with theme system and routes to other pages.
 */

import { HeroSection } from '../components/homepage'

export default function Home() {
  return (
    <main className="min-h-screen bg-base-100">
      <HeroSection />
      {/* Additional sections will be added by other scenario builders */}
    </main>
  )
}
