import { Header } from '@/components/layout/Header/Header'
import { Hero } from '@/components/sections/Hero/Hero'
import { Features } from './components/sections/Features/Features'

function App() {
  return (
    <div className="app">
      <Header />
      <main>
        <Hero />
        <Features />
        <section id="quick-start" className="section">
          <div className="container">
            <h2>Quick Start</h2>
            <p className="text-muted">Installation instructions coming soon...</p>
          </div>
        </section>
        <section id="usage" className="section">
          <div className="container">
            <h2>Usage</h2>
            <p className="text-muted">Usage examples coming soon...</p>
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
