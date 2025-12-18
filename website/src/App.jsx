import FeaturesSection from './components/FeaturesSection'
import ComparisonSection from './components/ComparisonSection'
import './App.css'

function App() {
  return (
    <div className="app">
      <header className="hero">
        <div className="hero-content">
          <h1 className="hero-title">MirDB</h1>
          <p className="hero-tagline">Persistent key-value store with memcached protocol</p>
          <p className="hero-description">
            A drop-in replacement for memcached that persists your data. Built with Rust for performance and reliability.
          </p>
          <div className="hero-cta">
            <a href="https://github.com/yetone/mirdb" className="btn btn-primary">
              Get Started
            </a>
            <a href="https://github.com/yetone/mirdb" className="btn btn-secondary">
              View on GitHub
            </a>
          </div>
        </div>
      </header>

      <FeaturesSection />

      <ComparisonSection />

      <footer className="footer">
        <div className="footer-content">
          <p>&copy; {new Date().getFullYear()} MirDB. MIT License.</p>
          <nav className="footer-nav">
            <a href="https://github.com/yetone/mirdb">GitHub</a>
            <a href="https://github.com/yetone/mirdb/blob/master/README.md">Documentation</a>
          </nav>
        </div>
      </footer>
    </div>
  )
}

export default App
