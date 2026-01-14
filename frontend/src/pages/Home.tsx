import ErrorBoundary from '../components/ErrorBoundary'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import StatsSection from '../components/StatsSection'
import DemoSection from '../components/DemoSection'
import FooterSection from '../components/FooterSection'

const Home = () => {
  return (
    <ErrorBoundary>
      <main className="min-h-screen">
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
