/**
 * Homepage Component
 * Owner: Shared across scenarios 1-7, 19
 *
 * Main homepage container that composes all sections.
 */

import { useEffect } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { BackgroundEffect } from '../components/BackgroundEffect'
import { Navbar } from '../components/Navbar'
import { HeroSection, FeaturesSection, HowItWorksSection, Footer } from '../components/homepage'

export default function Home() {
  const navigate = useNavigate()
  const location = useLocation()

  // Handle scroll to anchor on page load and hash changes
  useEffect(() => {
    if (location.hash) {
      const elementId = location.hash.slice(1)
      const element = document.getElementById(elementId)
      if (element) {
        // Small delay to ensure the page is fully rendered before scrolling
        setTimeout(() => {
          element.scrollIntoView({ behavior: 'smooth' })
        }, 100)
      }
    }
  }, [location.hash])

  const handleGetStarted = () => {
    navigate('/register')
  }

  const handleSignIn = () => {
    navigate('/login')
  }

  return (
    <BackgroundEffect>
      <Navbar onGetStarted={handleGetStarted} onSignIn={handleSignIn} />
      <main>
        <HeroSection onGetStarted={handleGetStarted} onSignIn={handleSignIn} />
        <FeaturesSection />
        <HowItWorksSection />
      </main>
      <Footer />
    </BackgroundEffect>
  )
}
