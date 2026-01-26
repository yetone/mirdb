/**
 * Homepage Component
 * Owner: Shared across scenarios 1-7, 19
 *
 * Main homepage container that composes all sections.
 */

import { useNavigate } from 'react-router-dom'
import { BackgroundEffect } from '../components/BackgroundEffect'
import { Navbar } from '../components/Navbar'
import { HeroSection, HowItWorksSection } from '../components/homepage'

export default function Home() {
  const navigate = useNavigate()

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
        <HowItWorksSection />
      </main>
    </BackgroundEffect>
  )
}
