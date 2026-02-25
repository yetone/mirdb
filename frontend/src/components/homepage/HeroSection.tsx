/**
 * HeroSection Component
 * Owner: Scenario 1 (Hero Section Display)
 *
 * Displays the main hero section of the homepage with:
 * - Product name and tagline
 * - URL shortening input form
 * - Primary CTA (Shorten URL) and Secondary CTAs (Sign Up Free, Log In)
 */

import { useState } from 'react'

export default function HeroSection() {
  const [url, setUrl] = useState('')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
  }

  return (
    <section
      className="hero min-h-[80vh] bg-gradient-to-br from-primary/10 via-base-100 to-secondary/10"
      data-testid="hero-section"
    >
      <div className="hero-content text-center flex-col gap-8">
        <div className="max-w-2xl">
          <h1 className="text-5xl font-bold text-base-content">
            URL Shortening Service
          </h1>
          <p className="py-6 text-xl text-base-content/80">
            Shorten Links. Track Insights. Share Smarter.
          </p>
        </div>

        {/* URL Shortening Form */}
        <form onSubmit={handleSubmit} className="w-full max-w-xl">
          <div className="join w-full">
            <input
              type="url"
              placeholder="Enter your long URL here..."
              className="input input-bordered join-item flex-1"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              aria-label="URL input"
              data-testid="url-input"
            />
            <button
              type="submit"
              className="btn btn-primary join-item"
              data-testid="shorten-url-button"
            >
              Shorten URL
            </button>
          </div>
        </form>

        {/* CTA Buttons */}
        <div className="flex gap-4 flex-wrap justify-center">
          <a href="/register" className="btn btn-secondary" data-testid="signup-button">
            Sign Up Free
          </a>
          <a href="/login" className="btn btn-outline" data-testid="login-button">
            Log In
          </a>
        </div>
      </div>
    </section>
  )
}
