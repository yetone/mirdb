import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'
import AnalyticsPreviewSection from '../components/AnalyticsPreviewSection'
import Footer from '../components/Footer'

const Home = () => {
  return (
    <main data-testid="home-page">
      <HeroSection />
      <FeaturesSection />
      <HowItWorksSection />
      <AnalyticsPreviewSection />
      <Footer />
    </main>
  )
}

export default Home
