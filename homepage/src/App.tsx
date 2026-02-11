/**
 * Main application component with layout structure.
 */

import { Header, Hero, Features, QuickStart } from './components'
import { Demo } from './components/sections/Demo'
import './styles/globals.css'

function App() {
  return (
    <div className="app">
      <Header />
      <main>
        <Hero />
        <Demo />
        <Features />
        <QuickStart />
      </main>
    </div>
  )
}

export default App
