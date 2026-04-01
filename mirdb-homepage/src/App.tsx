import { Hero } from './components/Hero'
import { Features } from './components/Features'
import { UsageDemo } from './components/UsageDemo'
import { Roadmap } from './components/Roadmap'
import { StatusBadges } from './components/StatusBadges'
import { Navigation } from './components/Navigation'
import { Footer } from './components/Footer'
import { ThemeToggle } from './components/ThemeToggle'

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 transition-colors duration-200">
      <Navigation />
      <main>
        <Hero />
        <StatusBadges className="flex justify-center py-4" />
        <UsageDemo />
        <Features />
        <Roadmap />
      </main>
      <Footer />
      {/* Theme Toggle - Fixed position in bottom right corner */}
      <div className="fixed bottom-4 right-4 z-50">
        <ThemeToggle className="shadow-lg bg-white dark:bg-gray-800 rounded-full" />
      </div>
    </div>
  )
}

export default App
