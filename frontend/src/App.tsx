import { Navigation } from './components/Navigation'
import { Hero } from './components/Hero'
import { Footer } from './components/Footer'
import { AuthProvider } from './context'
import './App.css'

function App() {
  return (
    <AuthProvider>
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
    </AuthProvider>
  )
}

export default App
