import Navbar from '../components/Navbar'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import SocialProofSection from '../components/SocialProofSection'
import HowItWorks from '../components/HowItWorks'
import Footer from '../components/Footer'

export default function Home() {
  return (
    <>
      <Navbar />
      <main data-testid="homepage">
        <HeroSection />
        <FeaturesSection />
        <SocialProofSection />
        <HowItWorks />
        <Footer />
      </main>
    </>
  )
}
