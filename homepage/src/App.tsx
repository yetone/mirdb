/**
 * Main application component with layout structure.
 */

import { Header, Footer, Hero, Features, QuickStart, Badge } from './components'
import { Demo } from './components/sections/Demo'
import { CIRCLECI_BADGE_URL, CIRCLECI_BUILD_URL } from './config/content'
import './styles/globals.css'

function App() {
  return (
    <div className="app">
      <Header />
      <main>
        <Hero />
        <Demo />
        <Features />
        {/* Build Status Badge section */}
        <section id="build-status" style={{ padding: '2rem 0', textAlign: 'center' }}>
          <div className="container">
            <h3 style={{ marginBottom: '1rem' }}>Build Status</h3>
            <Badge
              src={CIRCLECI_BADGE_URL}
              alt="Build Status"
              href={CIRCLECI_BUILD_URL}
            />
          </div>
        </section>
        <QuickStart />
      </main>
      <Footer />
    </div>
  )
}

export default App
