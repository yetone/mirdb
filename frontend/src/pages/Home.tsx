import { Link } from 'react-router-dom'
import { HeroSection, FeaturesSection, HowItWorksSection, StatsSection } from '../components/homepage'
import { BackgroundEffect } from '../components/BackgroundEffect'
import { FuturisticButton } from '../components/FuturisticButton'

export function Home() {
  const handleLearnMore = () => {
    // Will scroll to features section when implemented
    const featuresSection = document.getElementById('features')
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <main className="min-h-screen relative">
      <BackgroundEffect />
      {/* Navigation header with Login CTA - Scenario 2 CTA integration */}
      <header className="absolute top-0 left-0 right-0 z-10 p-4 sm:p-6">
        <nav className="max-w-7xl mx-auto flex justify-end items-center">
          <Link to="/login">
            <FuturisticButton variant="ghost" className="px-6 py-2">
              Login
            </FuturisticButton>
          </Link>
        </nav>
      </header>
      <HeroSection onLearnMoreClick={handleLearnMore} />
      <FeaturesSection />
      <HowItWorksSection />
      <StatsSection />
    </main>
  )
}

export default Home
