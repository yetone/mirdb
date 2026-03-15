import { Hero } from '@/components/sections/Hero'
import Features from './components/sections/Features'
import { QuickStart } from './components/sections/QuickStart'
import { Architecture } from './components/sections/Architecture'
import { Header } from './components/layout/Header'
import { Footer } from './components/layout/Footer'
import { MobileNav } from './components/layout/MobileNav'
import { ThemeProvider } from './contexts/ThemeContext'
import { ThemeToggle } from './components/ui/ThemeToggle'

function App() {
  return (
    <ThemeProvider>
      <div className="min-h-screen bg-white dark:bg-gray-900">
        {/* Header with MobileNav for responsive navigation */}
        <div className="relative">
          <Header />
          {/* MobileNav positioned to overlay the Header's placeholder mobile button - higher z-index */}
          <div className="fixed top-0 right-0 h-16 flex items-center pr-4 z-[60]">
            <MobileNav />
          </div>
        </div>
        {/* Theme toggle positioned fixed - hidden on mobile to avoid MobileNav overlap */}
        <div className="hidden md:block fixed top-3 right-20 z-50">
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
