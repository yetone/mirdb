import { Header } from './components/Header'
import { SkipLink } from './components/common'
import { Hero } from './components/Hero'
import { Features } from './components/Features'
import { UsageExample } from './components/UsageExample'
import { GettingStarted } from './components/GettingStarted'
import { Roadmap } from './components/Roadmap'

function App() {
  return (
    <div className="min-h-screen bg-slate-900 text-white">
      <SkipLink />
      <Header />
      <main id="main-content">
        <Hero />
        <Features />
        <UsageExample />
        <GettingStarted />
        <Roadmap />
      </main>
    </div>
  )
}

export default App
