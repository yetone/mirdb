import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'
import NavigationHeader from '../components/NavigationHeader'

const Home: React.FC = () => {
  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId)
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <div className="min-h-screen bg-base-100" data-testid="home-page">
      {/* Navigation Header */}
      <NavigationHeader onScrollToSection={scrollToSection} />

      {/* Hero Section */}
      <HeroSection />

      {/* Features Section */}
      <FeaturesSection />

      {/* How It Works Section */}
      <HowItWorksSection />

      {/* Footer */}
      <footer className="footer footer-center p-10 bg-base-200 text-base-content">
        <div>
          <p className="font-bold">URL Shortener</p>
          <p>Shorten. Share. Track.</p>
        </div>
        <div>
          <div className="grid grid-flow-col gap-4">
            <a href="/about" className="link link-hover">
              About
            </a>
            <a href="/privacy" className="link link-hover">
              Privacy Policy
            </a>
            <a href="/terms" className="link link-hover">
              Terms of Service
            </a>
          </div>
        </div>
        <div>
          <p>Copyright © 2024 - All rights reserved</p>
        </div>
      </footer>
    </div>
  )
}

export default Home
