import { Routes, Route } from 'react-router-dom'
import HeroSection from './components/HeroSection'
import FeaturesSection from './components/FeaturesSection'
import HowItWorksSection from './components/HowItWorksSection'

function Home() {
  return (
    <main className="min-h-screen bg-base-100">
      <HeroSection />
      <FeaturesSection />
      <HowItWorksSection />
    </main>
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
