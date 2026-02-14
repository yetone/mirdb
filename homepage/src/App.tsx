import { ThemeProvider } from './context/ThemeContext'
import { ThemeToggle } from './components/common/ThemeToggle'
import { Hero } from './components/sections/Hero'

function App() {
  return (
    <ThemeProvider>
      <div className="min-h-screen">
        <header className="p-4 flex justify-end">
          <ThemeToggle />
        </header>
        <main>
          <Hero />
        </main>
      </div>
    </ThemeProvider>
  )
}

export default App
