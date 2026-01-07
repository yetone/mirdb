import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorks from '../components/HowItWorks'

export default function Home() {
  return (
    <main data-testid="homepage">
      <HeroSection />
      <FeaturesSection />
      <HowItWorks />
    </main>
  )
}
