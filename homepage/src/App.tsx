/**
 * Main application component with layout structure.
 */

import { Header, Hero, Features } from './components'
import { Demo } from './components/sections/Demo'
import './styles/globals.css'

function App() {
  return (
    <div className="app">
      <Header />
      <main>
        <Hero />
        <Demo />
        <Features />
        {/* Quick Start section placeholder for primary CTA target */}
        <section id="quick-start" style={{ minHeight: '100vh', padding: '4rem 0' }}>
          <div className="container">
            <h2>Quick Start</h2>
            <p>Installation and usage instructions coming soon.</p>
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
