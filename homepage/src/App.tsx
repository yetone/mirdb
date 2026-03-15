import { Hero } from '@/components/sections/Hero'
import Features from './components/sections/Features'
import { QuickStart } from './components/sections/QuickStart'
import { Architecture } from './components/sections/Architecture'

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900">
      <main>
        <Hero />
        <Features />
        <QuickStart />
        <Architecture />
      </main>
    </div>
  )
}

export default App
