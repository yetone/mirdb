import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import Footer from '../components/Footer'

const Home = () => {
  return (
    <main data-testid="home-page">
      <HeroSection />
      <FeaturesSection />
      <Footer />
    </main>
  )
}

export default Home
