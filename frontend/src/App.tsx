import { Routes, Route } from 'react-router-dom'
import HeroSection from './components/HeroSection'
import FeaturesSection from './components/FeaturesSection'

function Home() {
  return (
    <main className="min-h-screen bg-base-100">
      <HeroSection />
      <FeaturesSection />
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
