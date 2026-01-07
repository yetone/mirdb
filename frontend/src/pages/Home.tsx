import Navbar from '../components/Navbar'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import SocialProofSection from '../components/SocialProofSection'
import HowItWorks from '../components/HowItWorks'
import Footer from '../components/Footer'
import ErrorBoundary from '../components/ErrorBoundary'

export default function Home() {
  return (
    <>
      <Navbar />
      <main data-testid="homepage">
        <ErrorBoundary>
          <HeroSection />
          <FeaturesSection />
          <SocialProofSection />
          <HowItWorks />
          <Footer />
        </ErrorBoundary>
      </main>
    </>
  )
}
