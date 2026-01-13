import { Link } from 'react-router-dom'
import { FeaturesSection } from '../components/FeaturesSection'

export function Home() {
  return (
    <div className="min-h-screen bg-base-200">
      {/* Navigation */}
      <nav className="navbar bg-base-100/50 backdrop-blur-md sticky top-0 z-50">
        <div className="flex-1">
          <Link to="/" className="btn btn-ghost text-xl">URL Shortener</Link>
        </div>
        <div className="flex-none gap-2">
          <Link to="/login" className="btn btn-ghost" data-testid="login-link">
            Login
          </Link>
          <Link to="/register" className="btn btn-primary" data-testid="register-link">
            Sign Up
          </Link>
        </div>
      </nav>

      {/* Hero Section */}
      <section className="hero min-h-[70vh] bg-base-300">
        <div className="hero-content text-center">
          <div className="max-w-2xl">
            <h1 className="text-5xl font-bold text-base-content">
              Shorten. Track. Share.
            </h1>
            <p className="py-6 text-lg text-base-content/80">
              Transform long URLs into memorable short links. Track clicks, analyze
              performance, and manage all your links in one place.
            </p>
            <div className="flex gap-4 justify-center">
              <Link to="/register" className="btn btn-primary" data-testid="get-started-btn">
                Get Started Free
              </Link>
              <Link to="/login" className="btn btn-outline" data-testid="login-btn">
                Login
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <FeaturesSection />

      {/* Footer */}
      <footer className="footer footer-center p-10 bg-base-300 text-base-content">
        <div>
          <p>© 2024 URL Shortener. All rights reserved.</p>
        </div>
      </footer>
    </div>
  )
}

export default Home
