import Navbar from '../components/Navbar'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import SocialProofSection from '../components/SocialProofSection'
import HowItWorksSection from '../components/HowItWorksSection'
import FooterCTA from '../components/FooterCTA'
import Footer from '../components/Footer'

interface HomeProps {
  'data-testid'?: string
}

export default function Home({ 'data-testid': testId }: HomeProps) {
  return (
    <div data-testid={testId || 'home-page'}>
      <Navbar />
      <main
        className="min-h-screen bg-base-100 pt-16"
        data-testid="home-main-content"
      >
        <HeroSection />
        <FeaturesSection />
        <SocialProofSection />
        <HowItWorksSection />
        <FooterCTA />
        <Footer />
      </main>
    </div>
  )
}
