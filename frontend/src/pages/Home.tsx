import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'
import NavigationHeader from '../components/NavigationHeader'
import Footer from '../components/Footer'

const Home: React.FC = () => {
  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId)
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <div className="min-h-screen bg-base-100" data-testid="home-page">
      {/* Skip to main content link for keyboard users */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-[100] focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-content focus:rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-focus"
        data-testid="skip-to-main"
      >
        Skip to main content
      </a>

      {/* Navigation Header */}
      <NavigationHeader onScrollToSection={scrollToSection} />

      {/* Main Content */}
      <main id="main-content">
        {/* Hero Section */}
        <HeroSection />

        {/* Features Section */}
        <FeaturesSection />

        {/* How It Works Section */}
        <HowItWorksSection />
      </main>

      {/* Footer */}
      <Footer />
    </div>
  )
}

export default Home
