/**
 * Homepage Page Component.
 * Owners: Scenario 3 (Navigation), Scenario 6 (Auth Experience)
 *
 * Main landing page that composes:
 * - HeroSection
 * - FeaturesSection
 * - HowItWorksSection (optional)
 * - Footer
 *
 * Integrates with:
 * - AuthContext for authentication state
 * - ThemeContext for theming
 * - React Router for navigation
 *
 * Requirements:
 * - REQ-6: Include navigation links to login and registration
 * - REQ-10: Provide different experience for authenticated users
 */

import React from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '@/contexts/AuthContext'
import { Navbar } from '@/components/Navbar'
import { HeroSection } from '@/components/homepage/HeroSection'
import { FeaturesSection } from '@/components/homepage/FeaturesSection'
import { HowItWorksSection } from '@/components/homepage/HowItWorksSection'
import { Footer } from '@/components/homepage/Footer'

export function Home() {
  const navigate = useNavigate()
  const { isAuthenticated } = useAuth()

  const handleGetStarted = () => {
    if (isAuthenticated) {
      navigate('/dashboard')
    } else {
      navigate('/register')
    }
  }

  const handleLogin = () => {
    navigate('/login')
  }

  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <main className="flex-1">
        <HeroSection
          isAuthenticated={isAuthenticated}
          onGetStarted={handleGetStarted}
          onLogin={handleLogin}
        />
        <FeaturesSection />
        <HowItWorksSection />
      </main>
      <Footer />
    </div>
  )
}
