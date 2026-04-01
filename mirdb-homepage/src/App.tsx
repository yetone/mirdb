import { Hero } from './components/Hero'
import { Features } from './components/Features'
import { UsageDemo } from './components/UsageDemo'
import { Roadmap } from './components/Roadmap'
import { StatusBadges } from './components/StatusBadges'

function App() {
  return (
    <main className="min-h-screen bg-white dark:bg-gray-900">
      <Hero />
      <StatusBadges className="flex justify-center py-4" />
      <UsageDemo />
      <Features />
      <Roadmap />
    </main>
  )
}

export default App
