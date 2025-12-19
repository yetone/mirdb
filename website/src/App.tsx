import { Hero } from './components/Hero'
import './App.css'

function App() {
  return (
    <div className="app">
      <Hero
        productName="MirDB"
        tagline="Memcached, but persistent."
        description="A persistent key-value store with memcached protocol compatibility. Built with Rust for safety and performance."
        ctaButtons={[
          { label: 'Get Started', href: '#quickstart', primary: true },
          { label: 'View on GitHub', href: 'https://github.com/example/mirdb', primary: false },
        ]}
      />
    </div>
  )
}

export default App
