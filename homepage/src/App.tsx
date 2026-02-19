import { Header } from '@/components/layout/Header/Header'
import { Hero } from '@/components/sections/Hero/Hero'
import { Features } from './components/sections/Features/Features'
import { QuickStart } from './components/sections/QuickStart/QuickStart'
import { Usage } from './components/sections/Usage/Usage'

function App() {
  return (
    <div className="app">
      <Header />
      <main>
        <Hero />
        <Features />
        <QuickStart />
        <Usage />
      </main>
    </div>
  )
}

export default App
