/**
 * Homepage component.
 * Owner: Scenario 1 - Homepage Public Access and Value Proposition Display
 *
 * Main landing page component that:
 * - Renders hero section with URL shortening
 * - Displays features section
 * - Shows personalized content for authenticated users
 * - Integrates with AuthContext and ThemeContext
 */

import React from 'react'
import { FeaturesSection } from '@/components/homepage'
import { useAuth } from '@/contexts/AuthContext'

const Home: React.FC = () => {
  const { isAuthenticated, user } = useAuth()

  return (
    <div className="min-h-screen" data-testid="homepage">
      {/* Hero Section Placeholder - to be implemented by Scenario 2 */}
      <section className="py-20 px-4 text-center" data-testid="hero-section">
        <div className="max-w-4xl mx-auto">
          <h1 className="text-4xl md:text-6xl font-bold mb-6">
            {isAuthenticated
              ? `Welcome back, ${user?.username || 'User'}!`
              : 'Shorten URLs, Track Insights'}
          </h1>
          <p className="text-xl text-base-content/70 mb-8">
            Transform long URLs into short, memorable links. Track clicks,
            analyze performance, and grow your reach.
          </p>
          {/* URL Input will be added by Scenario 2 */}
          <div className="flex justify-center gap-4">
            <a href="/register" className="btn btn-primary btn-lg">
              Get Started
            </a>
            <a href="#features" className="btn btn-ghost btn-lg">
              Learn More
            </a>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <div id="features">
        <FeaturesSection />
      </div>

      {/* Footer Placeholder - to be implemented by Scenario 10 */}
      <footer className="py-8 text-center text-base-content/50" data-testid="footer">
        <p>&copy; {new Date().getFullYear()} URL Shortener. All rights reserved.</p>
      </footer>
    </div>
  )
}

export default Home
