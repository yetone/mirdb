import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import DemoSection from '../components/DemoSection'
import FooterSection from '../components/FooterSection'

const Home = () => {
  return (
    <main className="min-h-screen">
      <HeroSection />
      <FeaturesSection />
      <DemoSection />
      <FooterSection />
    </main>
  )
}

export default Home
