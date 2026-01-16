import { Routes, Route } from 'react-router-dom'
import Navbar from './components/Navbar'
import HeroSection from './components/HeroSection'
import FeaturesSection from './components/FeaturesSection'
import HowItWorksSection from './components/HowItWorksSection'
import FooterCTA from './components/FooterCTA'

function Home() {
  return (
    <>
      <Navbar />
      <main className="min-h-screen bg-base-100 pt-16">
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        <FooterCTA />
      </main>
    </>
  )
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
    </Routes>
  )
}

export default App
