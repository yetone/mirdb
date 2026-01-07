import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'

export default function Home() {
  return (
    <main data-testid="homepage">
      <HeroSection />
      <FeaturesSection />
    </main>
  )
}
