/**
 * Main Application Component
 * Owner: Scenario 1 - Homepage Layout & Structure
 *
 * This component renders all sections in the correct order per the PRD layout:
 * 1. Navbar (top)
 * 2. Hero (above fold)
 * 3. Features (card grid)
 * 4. QuickStart (code example)
 * 5. StatusBadges (CI badges)
 * 6. Footer (bottom)
 */

import { Navbar, Hero, Features, QuickStart, StatusBadges, Footer } from './components'

function App() {
  return (
    <div className="min-h-screen flex flex-col bg-white dark:bg-gray-900">
      {/* Navbar - fixed at top */}
      <Navbar />

      {/* Main content */}
      <main className="flex-grow">
        {/* Hero section - above the fold */}
        <Hero />

        {/* Features section - key product features */}
        <Features />

        {/* Quick Start section - code examples */}
        <QuickStart />

        {/* Status Badges section - CI/CD status */}
        <StatusBadges />
      </main>

      {/* Footer - at the bottom */}
      <Footer />
    </div>
  )
}

export default App
