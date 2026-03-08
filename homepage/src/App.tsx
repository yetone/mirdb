import { Header } from './components/Header'
import { SkipLink } from './components/common'
import { Hero } from './components/Hero'
import { Features } from './components/Features'

function App() {
  return (
    <div className="min-h-screen bg-slate-900 text-white">
      <SkipLink />
      <Header />
      <main id="main-content">
        <Hero />
        <Features />
        <section id="getting-started" className="py-20 px-4">
          <div className="max-w-4xl mx-auto">
            <h2 className="text-3xl font-bold mb-8">Getting Started</h2>
            <p className="text-slate-300">Getting started content will be added here.</p>
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
