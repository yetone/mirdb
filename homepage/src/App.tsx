import { Hero } from '@/components/sections/Hero'
import Features from './components/sections/Features'
import { QuickStart } from './components/sections/QuickStart'
import { Architecture } from './components/sections/Architecture'
import { Header } from './components/layout/Header'
import { Footer } from './components/layout/Footer'
import { ThemeProvider } from './contexts/ThemeContext'
import { ThemeToggle } from './components/ui/ThemeToggle'

function App() {
  return (
    <ThemeProvider>
      <div className="min-h-screen bg-white dark:bg-gray-900">
        <Header />
        {/* Theme toggle positioned fixed in top-right corner */}
        <div className="fixed top-4 right-4 z-50 md:top-3 md:right-20">
          <ThemeToggle size="md" />
        </div>
        <main id="main-content">
          <Hero />
          <Features />
          <QuickStart />
          <Architecture />
        </main>
        <Footer />
      </div>
    </ThemeProvider>
  )
}

export default App
