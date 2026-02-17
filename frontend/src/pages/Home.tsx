/**
 * Homepage/Landing page component.
 * Owner: Scenario 1 - Hero Section Display (primary)
 *
 * Assembles homepage from section components:
 * - HeroSection: Value proposition and CTAs
 * - FeaturesSection: Service feature cards
 * - UrlDemoSection: Interactive URL shortening demo
 * - Footer: Navigation and legal links
 */

import React from 'react'
import { useNavigate } from 'react-router-dom'
import { BackgroundEffect } from '../components/common'
import { Navbar, Footer } from '../components/layout'
import { HeroSection, FeaturesSection, UrlDemoSection } from '../components/home'

export function Home() {
  const navigate = useNavigate()

  const handleRegisterClick = () => {
    navigate('/register')
  }

  const handleLoginClick = () => {
    navigate('/login')
  }

  return (
    <div className="min-h-screen flex flex-col">
      <BackgroundEffect />
      <Navbar />
      <main className="flex-1">
        <HeroSection
          onRegisterClick={handleRegisterClick}
          onLoginClick={handleLoginClick}
        />
        <FeaturesSection />
        <UrlDemoSection onRegisterPrompt={handleRegisterClick} />
      </main>
      <Footer />
    </div>
  )
}

export default Home
