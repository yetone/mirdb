import { Hero } from './components/Hero'
import { Features } from './components/Features'
import { Roadmap } from './components/Roadmap'

function App() {
  return (
    <main className="min-h-screen bg-white dark:bg-gray-900">
      <Hero />
      <Features />
      <Roadmap />
    </main>
  )
}

export default App
