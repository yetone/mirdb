import { Routes, Route } from 'react-router-dom'
import Navbar from './components/Navbar'
import HeroSection from './components/HeroSection'
import FeaturesSection from './components/FeaturesSection'
import HowItWorksSection from './components/HowItWorksSection'
import FooterCTA from './components/FooterCTA'
import Footer from './components/Footer'
import LoginPage from './pages/LoginPage'

function Home() {
  return (
    <>
      <Navbar />
      <main className="min-h-screen bg-base-100 pt-16">
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        <FooterCTA />
        <Footer />
      </main>
    </>
  )
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/login" element={<LoginPage />} />
    </Routes>
  )
}

export default App
