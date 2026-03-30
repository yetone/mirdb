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
import { FeaturesSection, HeroSection } from '@/components/homepage'
import { useAuth } from '@/contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

const Home: React.FC = () => {
  const { isAuthenticated, user } = useAuth()
  const navigate = useNavigate()

  const handleUrlSubmit = async (url: string) => {
    // Store URL for later use after auth
    localStorage.setItem('pendingUrl', url)

    if (isAuthenticated) {
      // Redirect to dashboard with URL to shorten
      navigate('/dashboard', { state: { urlToShorten: url } })
    } else {
      // Redirect to register with URL preserved
      navigate('/register', { state: { urlToShorten: url } })
    }
  }

  return (
    <div className="min-h-screen" data-testid="homepage">
      {/* Hero Section with URL Input */}
      <HeroSection
        onUrlSubmit={handleUrlSubmit}
        isAuthenticated={isAuthenticated}
      />

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
