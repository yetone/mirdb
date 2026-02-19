import { Header } from '@/components/layout/Header/Header'
import { Footer } from '@/components/layout/Footer/Footer'
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
      <Footer />
    </div>
  )
}

export default App
