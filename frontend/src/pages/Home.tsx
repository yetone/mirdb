import ErrorBoundary from '../components/ErrorBoundary'
import BackgroundEffect from '../components/BackgroundEffect'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import StatsSection from '../components/StatsSection'
import DemoSection from '../components/DemoSection'
import FooterSection from '../components/FooterSection'

const Home = () => {
  return (
    <ErrorBoundary>
      <BackgroundEffect />
      <main className="min-h-screen relative">
        <HeroSection />
        <FeaturesSection />
        <StatsSection />
        <DemoSection />
      </main>
      <FooterSection />
    </ErrorBoundary>
  )
}

export default Home
