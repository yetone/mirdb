import { Navigation } from './components/Navigation'
import { Hero } from './components/Hero'
import { Footer } from './components/Footer'
import { NetworkStatus } from './components/NetworkStatus'
import { AuthProvider } from './context'
import './App.css'

function App() {
  return (
    <AuthProvider>
      <NetworkStatus>
        <div className="app">
          <Navigation />
          <main>
            <Hero
              headline="Welcome to MirDB"
              subheadline="A high-performance persistent key-value store with Memcached protocol. Lightning fast, painless setup, and production ready."
              ctaText="Get Started"
              ctaHref="#getting-started"
            />
          </main>
          <Footer />
        </div>
      </NetworkStatus>
    </AuthProvider>
  )
}

export default App
