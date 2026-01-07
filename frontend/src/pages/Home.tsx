import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorks from '../components/HowItWorks'
import Footer from '../components/Footer'

export default function Home() {
  return (
    <main data-testid="homepage">
      <HeroSection />
      <FeaturesSection />
      <HowItWorks />
      <Footer />
    </main>
  )
}
