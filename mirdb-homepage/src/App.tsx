import { Hero } from './components/Hero'
import { Features } from './components/Features'
import { UsageDemo } from './components/UsageDemo'
import { Roadmap } from './components/Roadmap'

function App() {
  return (
    <main className="min-h-screen bg-white dark:bg-gray-900">
      <Hero />
      <UsageDemo />
      <Features />
      <Roadmap />
    </main>
  )
}

export default App
