import { Hero } from './components/Hero'
import './App.css'

function App() {
  return (
    <div className="app">
      <Hero
        headline="Welcome to MirDB"
        subheadline="A high-performance persistent key-value store with Memcached protocol. Lightning fast, painless setup, and production ready."
        ctaText="Get Started"
        ctaHref="#getting-started"
      />
    </div>
  )
}

export default App
