import { useState, FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Copy, Check } from 'lucide-react'
import { FeaturesSection } from '../components/FeaturesSection'
import { SocialProofSection } from '../components/SocialProofSection'
import { FuturisticButton } from '../components/FuturisticButton'
import { ThemeToggle } from '../components/ThemeToggle'

export function Home() {
  return (
    <div className="min-h-screen bg-base-200">
      {/* Navigation */}
      <nav className="navbar bg-base-100/50 backdrop-blur-md sticky top-0 z-50">
        <div className="flex-1">
          <Link to="/" className="btn btn-ghost text-xl">URL Shortener</Link>
        </div>
        <div className="flex-none gap-2">
          <ThemeToggle />
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
            <p className="py-6 text-lg text-base-content">
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

      {/* Demo Section - Try it now URL shortening */}
      <DemoSection />

      {/* Features Section */}
      <FeaturesSection />

      {/* Social Proof Section */}
      <SocialProofSection />

      {/* Footer */}
      <footer data-testid="footer-section" className="footer footer-center p-10 bg-base-300 text-base-content">
        <nav data-testid="footer-nav" className="grid grid-flow-col gap-4">
          <Link to="/login" data-testid="footer-login-link" className="link link-hover">
            Login
          </Link>
          <Link to="/register" data-testid="footer-register-link" className="link link-hover">
            Sign Up
          </Link>
        </nav>
        <aside>
          <p data-testid="footer-copyright">© 2024 URL Shortener. All rights reserved.</p>
        </aside>
      </footer>
    </div>
  )
}

/**
 * URL validation helper function
 * Validates that a string is a properly formatted URL
 */
function isValidUrl(urlString: string): boolean {
  try {
    const url = new URL(urlString)
    return url.protocol === 'http:' || url.protocol === 'https:'
  } catch {
    return false
  }
}

/**
 * Generate a demo short code for preview purposes
 * In production, this would come from the API
 */
function generateDemoShortCode(): string {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'
  let result = ''
  for (let i = 0; i < 6; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return result
}

/**
 * Demo Section Component
 * Implements REQ-4: "Try it now" URL shortening demo for unauthenticated users
 * Implements US-4: User can enter URL, see shortened result, and be prompted to sign up
 */
function DemoSection() {
  const [inputUrl, setInputUrl] = useState('')
  const [shortenedUrl, setShortenedUrl] = useState('')
  const [error, setError] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [copied, setCopied] = useState(false)

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault()
    setError('')
    setShortenedUrl('')
    setCopied(false)

    // Validate URL
    if (!inputUrl.trim()) {
      setError('Please enter a URL')
      return
    }

    if (!isValidUrl(inputUrl)) {
      setError('Please enter a valid URL (e.g., https://example.com)')
      return
    }

    // Simulate API call with loading state
    setIsLoading(true)
    try {
      // Simulate network delay for demo purposes
      await new Promise((resolve) => setTimeout(resolve, 500))
      const shortCode = generateDemoShortCode()
      const baseUrl = window.location.origin
      setShortenedUrl(`${baseUrl}/r/${shortCode}`)
    } catch {
      setError('Failed to shorten URL. Please try again.')
    } finally {
      setIsLoading(false)
    }
  }

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(shortenedUrl)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Fallback for browsers without clipboard API
      const textArea = document.createElement('textarea')
      textArea.value = shortenedUrl
      document.body.appendChild(textArea)
      textArea.select()
      document.execCommand('copy')
      document.body.removeChild(textArea)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  return (
    <section
      data-testid="demo-section"
      className="py-16 px-4 bg-base-100"
    >
      <div className="container mx-auto max-w-2xl">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2 className="text-3xl font-bold text-center mb-4">
            Try It Now
          </h2>
          <p className="text-center text-base-content mb-8">
            Shorten your first URL in seconds - no sign-up required
          </p>

          {/* Demo Form */}
          <form onSubmit={handleSubmit} data-testid="demo-form">
            <div className="flex flex-col sm:flex-row gap-3">
              <input
                type="text"
                data-testid="demo-url-input"
                value={inputUrl}
                onChange={(e) => setInputUrl(e.target.value)}
                placeholder="Enter your long URL here..."
                className="input input-bordered flex-1 w-full"
                aria-label="URL to shorten"
                aria-describedby={error ? 'demo-error' : undefined}
              />
              <FuturisticButton
                type="submit"
                data-testid="demo-shorten-button"
                disabled={isLoading}
                variant="primary"
                size="md"
              >
                {isLoading ? 'Shortening...' : 'Shorten'}
              </FuturisticButton>
            </div>
          </form>

          {/* Error Message */}
          {error && (
            <motion.div
              id="demo-error"
              data-testid="demo-error"
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              className="mt-4 p-3 bg-error/10 border border-error/30 rounded-lg text-error text-sm"
              role="alert"
            >
              {error}
            </motion.div>
          )}

          {/* Result Section */}
          {shortenedUrl && (
            <motion.div
              data-testid="demo-result"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              className="mt-6"
            >
              {/* Shortened URL Display */}
              <div className="p-4 bg-success/10 border border-success/30 rounded-lg">
                <p className="text-sm text-base-content/70 mb-2">
                  Your shortened URL:
                </p>
                <div className="flex items-center gap-2">
                  <code
                    data-testid="demo-shortened-url"
                    className="flex-1 text-primary font-mono text-lg break-all"
                  >
                    {shortenedUrl}
                  </code>
                  <button
                    type="button"
                    onClick={handleCopy}
                    data-testid="demo-copy-button"
                    className="btn btn-ghost btn-sm"
                    aria-label="Copy shortened URL"
                  >
                    {copied ? (
                      <Check className="w-5 h-5 text-success" />
                    ) : (
                      <Copy className="w-5 h-5" />
                    )}
                  </button>
                </div>
              </div>

              {/* Sign-up CTA */}
              <div
                data-testid="demo-signup-cta"
                className="mt-6 p-6 bg-gradient-to-r from-primary/10 to-secondary/10 rounded-lg text-center"
              >
                <h3 className="text-xl font-semibold mb-2">
                  Want to track your links?
                </h3>
                <p className="text-base-content/70 mb-4">
                  Sign up for free to save your URLs, track clicks, and access
                  detailed analytics.
                </p>
                <Link to="/register">
                  <FuturisticButton variant="primary" size="md">
                    Sign Up Free
                  </FuturisticButton>
                </Link>
              </div>
            </motion.div>
          )}
        </motion.div>
      </div>
    </section>
  )
}

export default Home
