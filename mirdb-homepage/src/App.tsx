import { Hero } from './components/Hero'
import { Features } from './components/Features'
import { UsageDemo } from './components/UsageDemo'
import { Roadmap } from './components/Roadmap'
import { StatusBadges } from './components/StatusBadges'
import { Navigation } from './components/Navigation'
import { Footer } from './components/Footer'

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900">
      <Navigation />
      <main>
        <Hero />
        <StatusBadges className="flex justify-center py-4" />
        <UsageDemo />
        <Features />
        <Roadmap />
      </main>
      <Footer />
    </div>
  )
}

export default App
